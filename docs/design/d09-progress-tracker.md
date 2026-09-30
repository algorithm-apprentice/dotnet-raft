# D09 Progress Tracker Design

- **Status:** Accepted
- **Date:** 2026-09-30
- **DAG node:** D09
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D09 collects the leader's D08 follower progress objects and the active voter
and learner configuration. It derives:

- the quorum-committed log index;
- whether a voting quorum has been recently active;
- the result and informational counts of an election;
- stable voter, learner, and progress iteration order; and
- the protocol `ConfState` representation of the current membership.

D10 mutates cloned tracker configuration and progress maps transactionally.
D12-D20 use the tracker for elections, replication, quorum checks, membership,
read-index acknowledgements, and leadership transfer.

## Goals

- Preserve the pinned `tracker.ProgressTracker` behavior.
- Represent incoming and outgoing voter majorities through D03 `JointConfig`.
- Track active learners and staged post-joint learners separately.
- Derive commits only from configured voters.
- Exclude learners from activity and election counts.
- Record only the first vote received from each node in one election round.
- Visit progresses and expose node lists in deterministic numeric order.
- Produce owned, normalized `ConfState` values.
- Provide independently mutable configuration clones for D10.

## Non-goals

- Applying or validating membership changes; D10 owns those rules.
- Enforcing that every configuration member has a progress object; D10 checks
  cross-object invariants before replacing live state.
- Sending replication or election messages.
- Cloning D08 progress objects; D10 adds the narrowly required transactional
  progress-copy operation.
- Synchronizing concurrent callers. One serialized Raft event loop owns the
  tracker.

## Configuration model

```csharp
namespace DotnetRaft.Tracker;

internal sealed class TrackerConfig
{
    internal TrackerConfig();

    internal TrackerConfig(
        JointConfig voters,
        IEnumerable<ulong>? learners = null,
        IEnumerable<ulong>? learnersNext = null,
        bool autoLeave = false);

    internal JointConfig Voters { get; }
    internal HashSet<ulong> Learners { get; }
    internal HashSet<ulong> LearnersNext { get; }
    internal bool AutoLeave { get; set; }

    internal TrackerConfig Clone();
    internal ConfState ToConfState();

    public override string ToString();
}
```

The default configuration contains empty incoming and outgoing voter
majorities, empty learner sets, and `AutoLeave == false`.

The constructor defensively clones every supplied collection. `Clone` copies
both voter majorities, both learner sets, and `AutoLeave`, so mutating a
candidate D10 configuration cannot affect the live tracker.

### Canonical empty representation

Go uses `nil` maps to distinguish an absent outgoing configuration or learner
set from an allocated empty map. C# uses non-null empty collections
canonically:

```text
not joint       <=> Voters.Outgoing.Count == 0
no learners     <=> Learners.Count == 0
no staged ones  <=> LearnersNext.Count == 0
```

D10 enforces:

```text
Voters.Outgoing.Count == 0
    => LearnersNext.Count == 0
    && AutoLeave == false
```

No protocol or algorithmic behavior depends on the allocation identity of an
empty collection.

### Learner roles

`Learners` contains active learners. These IDs must not intersect either voter
majority and their progress objects have `IsLearner == true`.

`LearnersNext` contains voters being demoted while they still belong to the
outgoing majority. They remain voters, and their progress objects remain
`IsLearner == false`, until D10 leaves the joint configuration.

## Configuration conversion

`ToConfState` returns a new protobuf object on every call:

```text
Voters          = sorted incoming voters
VotersOutgoing  = sorted outgoing voters
Learners        = sorted active learners
LearnersNext    = sorted staged learners
AutoLeave       = current flag, with proto2 presence set
```

Mutating the returned protobuf cannot affect the tracker.

`ToString` preserves the reference diagnostic shape:

```text
voters=(1 2 3)&&(1 2) learners=(4) learners_next=(2) autoleave
```

Empty learner sections and a false auto-leave flag are omitted.

## Progress map

```csharp
internal sealed class ProgressMap
    : Dictionary<ulong, Progress>,
      IAckedIndexer
{
    public override string ToString();
}
```

The map is mutable because D10 and the Raft core add, remove, and replace
progress objects. Its `IAckedIndexer` implementation returns the progress
`Match` index for a present ID and reports a missing ID otherwise.

`ToString` sorts IDs numerically and appends one line per progress:

```text
1: StateProbe match=0 next=1
2: StateReplicate match=5 next=6
```

The result retains the reference trailing newline when the map is non-empty.

## Tracker type

```csharp
internal sealed class ProgressTracker
{
    internal ProgressTracker(
        int maxInflightMessages,
        ulong maxInflightBytes);

    internal TrackerConfig Config { get; set; }
    internal ProgressMap Progress { get; set; }
    internal IReadOnlyDictionary<ulong, bool> Votes { get; }
    internal int MaxInflightMessages { get; }
    internal ulong MaxInflightBytes { get; }

    internal ConfState ToConfState();
    internal bool IsSingleton { get; }
    internal ulong CommittedIndex { get; }
    internal void Visit(Action<ulong, Progress> visitor);
    internal bool QuorumActive();
    internal ulong[] VoterNodes();
    internal ulong[] LearnerNodes();
    internal void ResetVotes();
    internal void RecordVote(ulong id, bool granted);
    internal (int Granted, int Rejected, VoteResult Result) TallyVotes();
}
```

Construction rejects a negative inflight-message limit, stores both D07
limits, and initializes empty configuration, progress, and vote collections.
Zero remains valid here; D12 validates runnable Raft configuration.

`Config` and `Progress` have assembly-internal setters so D10 can atomically
install a validated candidate pair. Callers must never replace only one half
of that pair.

## Commit derivation

`CommittedIndex` delegates to:

```text
Config.Voters.CommittedIndex(Progress)
```

The incoming and outgoing majorities each calculate their quorum index through
D03; joint consensus selects the smaller result. Learners and unconfigured
progress objects cannot affect the result because the voter configuration
chooses which IDs to query.

An empty incoming configuration yields `ulong.MaxValue`, preserving D03 and
the reference's bootstrap algebra. A live Raft node never commits from an
empty configuration because D10 and D12 prevent that state.

## Stable visitation and node lists

`Visit`:

1. rejects a null callback;
2. snapshots and numerically sorts the current progress IDs; and
3. invokes the callback once per ID in that order.

The callback may mutate individual progress objects but must not structurally
modify the progress map during visitation.

`VoterNodes` returns the sorted union of incoming and outgoing voters.
`LearnerNodes` returns sorted active learners only; staged learners remain
voters until joint consensus is left. Empty results are returned as empty
lists, not `null`.

`IsSingleton` is true only when:

```text
Voters.Incoming.Count == 1
    && Voters.Outgoing.Count == 0
```

## Quorum activity

`QuorumActive` constructs an activity vote for every tracked non-learner:

```text
vote[id] = Progress[id].RecentActive
```

It then evaluates the current joint voter configuration through D03
`VoteResult`. The quorum is active only when the result is `Won`.

Learners are excluded. Staged learners remain included because their progress
is not marked as a learner until D10 leaves the joint configuration.
Activity flags are not reset by this method; D19 owns the election-timeout
reset sequence.

## Election vote tracking

`RecordVote(id, granted)` records a vote only when the ID has not already
voted in the current round. Duplicate or reordered responses cannot change the
first recorded decision.

`ResetVotes` removes every recorded vote before a new campaign.

`TallyVotes` returns:

- `Granted`: recorded granted votes whose progress exists and is not a
  learner;
- `Rejected`: recorded rejected votes whose progress exists and is not a
  learner; and
- `Result`: the D03 joint-quorum result over all recorded votes.

The informational counts ignore learner votes and votes without a current
non-learner progress. The quorum result independently ignores IDs not present
in the configured voter majorities.

## Ownership and invariants

```text
Config, Progress, and Votes are owned by one ProgressTracker
Progress keys are node IDs
Configured voter and learner IDs should have Progress entries
Learners and voter IDs should not intersect
LearnersNext IDs should belong to the outgoing voter majority
Progress.IsLearner is true exactly for active Learners
```

D09 does not reject temporarily incomplete candidate objects because D10 must
construct and validate them transactionally. Live-state replacements occur
only after D10 has checked all invariants.

Every `ConfState`, node-list result, configuration clone, and progress ID
snapshot returned by D09 owns its mutable storage.

## Test plan

Because the pinned tracker has no dedicated `tracker_test.go`, D09 tests derive
the observable contracts from `tracker.go`, quorum tests, configuration-change
data-driven tests, and Raft call sites:

1. default construction and inflight limits;
2. negative inflight limit rejection;
3. sorted, presence-normalized, defensively owned `ConfState`;
4. constructor ownership by mutating both supplied voter majorities and both
   supplied learner collections after construction;
5. independent configuration cloning, including `AutoLeave`;
6. reference configuration string formatting;
7. stable progress-map string formatting and visitation order;
8. single and joint committed-index calculation with learners ignored;
9. simple and joint quorum-activity outcomes without changing any
   `RecentActive` flag;
10. staged learners counted as voters while active learners are excluded;
11. sorted voter union and active learner lists;
12. singleton detection for simple and joint configurations;
13. first-vote-wins recording;
14. tally counts that exclude learners and unknown IDs;
15. a tracked non-learner outside the voter configuration affects
    informational counts but not the quorum result;
16. vote reset and pending/won/lost joint results;
17. mutation of returned clones, protobufs, and node lists cannot affect live
    tracker state.

## Development sequence

D09 uses strict test-driven development:

1. review and accept this design;
2. add all tracker tests before production types;
3. run the focused target and record the expected compile failure;
4. implement the smallest configuration, progress-map, and tracker behavior
   that passes;
5. refactor while preserving the test matrix;
6. run formatting and the full suite before code review.

## Completion criteria

D09 is complete when:

- this design is reviewed and accepted;
- configuration, progress-map, commit, activity, and vote tracking are
  implemented;
- all derived and additional tests pass;
- formatting, build, and full tests pass;
- code review has no unresolved substantiated findings;
- the reviewed changes are committed before D10 starts.
