# D14 Basic Log Replication Design

- **Status:** Accepted
- **Date:** 2026-09-30
- **DAG node:** D14
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D14 turns the D13 election model into the first complete replicated-log
model. A newly elected leader appends its current-term no-op, accepts
proposals, sends append and heartbeat messages, receives durable follower
acknowledgements, and advances commit through simple or joint quorum
replication.

The implementation remains deterministic and transport-free. Tests drive
messages and explicit persistence completion through the existing immediate
and after-append queues.

## Goals

- Append a current-term no-op whenever a candidate becomes leader.
- Count the leader's local log only after durable self acknowledgement.
- Append caller proposals without mutating caller-owned protobufs.
- Forward follower proposals to a known leader.
- Drop proposals explicitly when no role can process or forward them.
- Send basic append messages to every tracked remote voter or learner.
- Validate previous log term/index and replace conflicting follower suffixes.
- Delay append responses until the follower log is durable.
- Send and receive heartbeats with safe commit indexes.
- Track follower matches and retry rejected appends one index earlier.
- Commit only a quorum-replicated entry from the leader's current term.
- Broadcast an updated commit index after commit advances.
- Demonstrate election and proposal commitment in a deterministic three-node
  interaction test.

## Non-goals

- Optimistically advancing remote `Next`; D15 owns replicate-mode pipelining.
- Probe pausing, inflight windows, or heartbeat-based flow recovery; D15 owns
  them.
- Optimized append rejection hints and term skipping; D15 owns them.
- `MaxSizePerMessage`; D15 introduces size-bounded append batches.
- `MaxUncommittedEntriesSize`; D15 introduces proposal backpressure.
- Snapshot transmission or restoration; D16 owns it.
- Validating or applying configuration-change proposals; D17 owns it.
- Read-index processing or heartbeat acknowledgement contexts; D18 owns it.
- `CheckQuorum`, `PreVote`, and leader leases; D19 owns them.
- Proposal rejection during leadership transfer; D20 owns it.
- Production `Ready`/`Advance` capture and storage stabilization; D21 owns it.

## Core API additions

```csharp
internal sealed class RaftCore
{
    internal void TickLeader();
}
```

`Step` remains the single message entry point and gains D14 role dispatch for:

```text
MsgBeat
MsgProp
MsgApp
MsgAppResp
MsgHeartbeat
MsgHeartbeatResp
```

The append, heartbeat, broadcast, proposal, and commit helpers remain private.

## Proposal error model

Expected proposal loss is not a core invariant failure. D14 adds:

```csharp
internal sealed class ProposalDroppedException
    : InvalidOperationException
```

`Step(MsgProp)` throws this exception when:

- a follower has no known leader;
- follower proposal forwarding is disabled;
- a candidate or pre-candidate has no leader;
- a leader no longer has local progress.

D20 adds the leadership-transfer drop case. D15 adds quota-based drops.

An empty `MsgProp` is malformed and throws `RaftInvariantException`.

D21 may expose or map the typed drop through the public deterministic API
without parsing exception text.

## Leader activation

`BecomeLeader` retains the D12 structural reset:

```text
Reset(Term)
LeaderId = Id
Role = Leader
local progress -> Replicate
local RecentActive = true
PendingConfigurationIndex = old Log.LastIndex
```

It then appends exactly one empty `EntryNormal`:

```text
Term  = current Term
Index = old Log.LastIndex + 1
Data  = empty
```

The local progress match does not advance immediately. Appending queues a
self-addressed `MsgAppResp` in the after-append queue. Only persistence
completion and stepping that response may advance the local match and commit.

`BecomeLeader` itself does not broadcast. The successful D13 vote-response
path calls `BecomeLeader` and then broadcasts append messages, matching the
pinned separation.

The no-op always succeeds in D14 because quota enforcement is deferred to
D15. If the old last index has no room for a new entry with a representable
successor, leader transition fails before reset or role mutation.

### Persistence batches

For a singleton election:

1. persist term/vote and step the captured self `MsgVoteResp`;
2. become leader and append the no-op;
3. expose the no-op and its captured self `MsgAppResp` in a later batch;
4. persist the no-op and step only that captured response; and
5. commit the no-op through the singleton quorum.

New after-append work is never folded into the batch whose self message
created it.

## Local append

Appending leader entries:

1. materializes and validates the complete input before log mutation;
2. requires at least one entry;
3. clones every entry;
4. assigns consecutive indexes after `Log.LastIndex`;
5. overwrites each cloned term with the leader's current term;
6. preserves entry type, data, and unknown protobuf fields;
7. appends through D06 `RaftLog.Append`; and
8. queues one self `MsgAppResp` for the new final index.

The caller's message and entries remain unchanged.

Index overflow or a final index without a representable successor fails before
queue mutation. `RaftLog` remains the final authority for contiguous and term
invariants.

## Proposal handling

### Leader

A leader:

1. rejects an empty proposal as malformed;
2. checks that local progress still exists;
3. appends all supplied entries as one local operation; and
4. broadcasts append messages to tracked remote nodes.

D17 later examines configuration-change entries before append. In D14 their
type and data are preserved without applying membership.

### Follower

A follower with a known leader clones the proposal, sets `To = LeaderId`, and
passes it through `Send`. `MsgProp` retains term zero and the caller's `From`.

The original message is never rewritten.

A follower without a leader, or with
`DisableProposalForwarding == true`, throws `ProposalDroppedException`.

### Candidate and pre-candidate

Candidates and pre-candidates throw `ProposalDroppedException`.

## Basic append sending

For remote progress:

```text
previousIndex = progress.Next - 1
previousTerm  = Log.GetTerm(previousIndex)
entries       = all entries in [progress.Next, Log.LastIndex]
commit        = Log.Committed
```

The leader sends:

```text
Type    = MsgApp
To      = remote ID
Index   = previousIndex
LogTerm = previousTerm
Entries = entries
Commit  = commit
```

D14 always uses an unlimited entry-size bound and does not call
`Progress.SentEntries`. Remote `Next` advances only after acknowledgement.
Remote progress therefore remains in probe state throughout D14.

An empty append may be sent to communicate an updated commit index.

If the previous term is compacted below the retained boundary, D14 emits
nothing. D16 replaces this branch with snapshot transmission.

`StorageError.Unavailable` is not snapshot deferral. It indicates invalid
progress beyond `Log.LastIndex + 1` or missing supposedly retained storage and
propagates explicitly. Other storage and invariant failures also propagate.

Broadcast traversal uses D09's stable sorted progress visitation and includes
voters and learners, excluding only the local ID.

## Follower append handling

Current- or higher-term `MsgApp` first performs D13 leader recognition. The
resulting follower then handles the payload.

### Already committed prefix

When `message.Index < Log.Committed`, the follower does not inspect or append
the supplied suffix. It queues:

```text
Type  = MsgAppResp
To    = leader
Index = Log.Committed
```

### Matching append

The follower builds:

```text
LogSlice(
    term: message.Term,
    previous: (message.LogTerm, message.Index),
    entries: message.Entries)
```

`RaftLog.MaybeAppend`:

- rejects a previous-entry mismatch;
- preserves already matching entries;
- truncates an uncommitted conflicting suffix;
- appends new entries; and
- advances commit to
  `min(message.Commit, last index covered by the append)`.

On success, the follower queues a durable `MsgAppResp` with the returned final
index.

### Mismatch

On mismatch, the follower leaves log and commit unchanged and queues:

```text
Type   = MsgAppResp
To     = leader
Index  = message.Index
Reject = true
```

D14 deliberately omits `RejectHint` and response `LogTerm`. D15 adds the
term-aware optimized hint.

Every append response, including rejection and empty-append success, remains
in the after-append queue.

## Basic append response handling

Only a current-term leader processes `MsgAppResp`, and only when progress
exists for `message.From`.

### Acceptance

An acknowledgement beyond `Log.LastIndex` is malformed and fails before
progress mutation.

Otherwise:

```text
progress.RecentActive = true
updated = progress.MaybeUpdate(message.Index)
```

When match advances:

1. attempt current-term quorum commit;
2. broadcast append if commit advances; otherwise
3. when `message.From != Id`, send another append to that follower when it is
   still behind.

The self exclusion is required when multiple local appends await persistence.
Stepping an earlier captured self acknowledgement may leave local match below
`Log.LastIndex`; those later entries already have their own self
acknowledgements and must never cause a network append to the leader itself.

D14 does not move remote progress from probe to replicate and does not free
inflight windows. D15 adds those transitions.

### Rejection

The leader marks the peer recently active and performs a one-index fallback.
A rejection of previous index zero is impossible for a conforming follower
because every log matches the `(0, 0)` anchor. D14 ignores it without retrying
the identical probe.

For a rejection of previous index `i > 0`, the basic match hint is:

```text
i - 1
```

`Progress.MaybeDecrementTo(i, basicHint)` rejects stale responses and never
moves `Next` below `Match + 1`. When it changes `Next`, the leader sends one
new append probe.

D15 replaces the basic hint with the follower's optimized
`RejectHint`/`LogTerm` and adds replicate-to-probe recovery.

Unknown-node responses and stale acknowledgements do nothing.

## Commit calculation

A leader computes:

```text
quorumIndex = Tracker.CommittedIndex
```

and asks D06 to commit:

```text
Log.MaybeCommit(
    EntryId(
        Term: current Term,
        Index: quorumIndex))
```

This advances commit only when:

- the quorum index is above the existing commit;
- the leader contains that index; and
- the entry at that index belongs to the current term.

Committing a current-term entry also commits every preceding entry, including
entries from older terms. A quorum count for only an older-term entry does not
advance commit.

Joint configurations use D03/D09's minimum of incoming and outgoing quorum
indexes.

When commit advances, the leader broadcasts append messages so followers learn
the new commit index. D18 later releases pending read-index requests at this
point.

## Heartbeats

### Sending

`MsgBeat` is local and handled only by a leader. The leader sends one
`MsgHeartbeat` to each tracked remote node.

For each destination:

```text
Commit = min(progress.Match, Log.Committed)
```

This prevents the leader from telling a follower to commit beyond the prefix
known to match.

D14 heartbeat context is empty. D18 supplies safe-read contexts.

### Receiving

A follower:

1. resets election elapsed and records the leader through D13 handling;
2. advances commit to `message.Commit`; and
3. sends an immediate `MsgHeartbeatResp` carrying the same context.

Heartbeats do not append entries and their responses do not require durable
state.

### Leader response

A current-term leader ignores responses from unknown nodes. For known
progress it:

- marks the sender recently active; and
- sends an append when the sender's match is behind `Log.LastIndex`.

D15 adds flow-control unpausing. D18 adds safe-read acknowledgement.

### Clock

`TickLeader` consumes D12 `TickLeaderClocks`.

- D14 ignores the election/quorum signal; D19 consumes it.
- When heartbeat is due and the node is still leader, D14 steps local
  `MsgBeat`.

D20 later inserts transfer-expiration handling between the quorum and
heartbeat phases.

## Role dispatch

D14 extends D13 without weakening its term filter:

- follower `MsgApp` and `MsgHeartbeat` process payloads;
- candidate/pre-candidate leader messages first become follower, then process
  payloads;
- leader equal-term append/heartbeat messages remain ignored;
- higher-term leader messages make a leader follower before payload handling;
- stale-term messages remain ignored before payload dispatch;
- `MsgSnap` still performs only leader recognition until D16.

Only leaders process append and heartbeat responses.

## Ownership

- `Step` never mutates inbound messages.
- Proposal forwarding clones before changing `To`.
- Local append clones entries before assigning term/index.
- `RaftLog` and outbound queues own independent protobuf clones.
- Follower append handling reads inbound entries and relies on `RaftLog` to
  clone appended state.
- Append and heartbeat send paths build new messages; `Send` clones them.

## Invariants

```text
leader appends exactly one no-op on activation
local Match advances only after durable self MsgAppResp
follower Match advances only after MsgAppResp
append acknowledgement never exceeds leader LastIndex
committed entries are never overwritten
follower commit never exceeds the accepted append boundary
quorum count commits only a current-term entry
current-term commit includes every preceding entry
heartbeat commit never exceeds the sender's known Match
proposal input and replicated entries are caller-independent
all append responses wait for durable local log state
```

## Test plan

### Leader activation and persistence

1. direct candidate-to-leader transition appends one empty current-term no-op;
2. the no-op queues a self `MsgAppResp` but does not immediately advance local
   match or commit;
3. stepping the captured durable self response advances local progress;
4. singleton no-op commits only in the later persistence batch;
5. election victory broadcasts the no-op to every remote progress record;
6. leader-transition index exhaustion fails atomically;
7. D12/D13 tests are updated from structural-leader expectations to D14
   activation semantics.

### Proposals and append sending

8. leader proposals receive consecutive current-term indexes and preserve
   type/data on caller-independent clones;
9. multi-entry proposals produce one final durable self acknowledgement;
10. append messages contain exact previous index/term, entries, and commit;
11. broadcasts visit voters and learners in sorted order;
12. empty proposals fail without log or queue mutation;
13. follower forwarding preserves caller ownership, sender, entries, and
    term-zero semantics;
14. no-leader, disabled-forwarding, candidate, pre-candidate, and
    removed-leader proposals throw `ProposalDroppedException`;
15. compacted followers produce no append until D16 snapshot support;
16. unavailable retained terms and progress beyond `LastIndex + 1` fail
    explicitly instead of masquerading as snapshot deferral;
17. D14 ignores configured message-size and uncommitted-size limits.

### Follower append and heartbeat

18. matching empty and nonempty appends acknowledge the correct final index;
19. conflicting uncommitted suffixes are replaced;
20. conflicts at or below commit remain invariant failures;
21. previous-index mismatch rejects without changing log or commit;
22. D14 rejection contains no optimized hint;
23. an append anchored below commit acknowledges the committed index;
24. leader commit is bounded by the final accepted append index;
25. candidate and pre-candidate process append/heartbeat payloads after
    stepping down;
26. append responses are durable while heartbeat responses are immediate;
27. heartbeat advances commit, preserves context, and does not alter entries;
28. inbound append, heartbeat, proposal, entries, and context remain unchanged.

### Leader progress and commit

29. accepted responses monotonically advance match and next;
30. stale acknowledgements and responses from unknown IDs are ignored;
31. impossible future acknowledgements fail before progress mutation;
32. rejected probes back up one index and retry;
33. a zero-index rejection does not resend the identical probe;
34. stale rejections do not move progress;
35. simple majority outcomes commit at the expected index;
36. joint commit requires both constituent majorities;
37. older-term quorum indexes do not commit;
38. acknowledging a current-term entry commits all preceding entries;
39. commit advancement broadcasts empty or nonempty append messages carrying
    the new commit;
40. heartbeat responses mark progress active and trigger catch-up append;
41. with multiple pending local appends, stepping the earlier self
    acknowledgement never emits or attempts an append to self.

### Heartbeat clock and interaction

42. heartbeat sends use `min(Match, Committed)` and contain no entries;
43. `MsgBeat` is ignored outside leader state;
44. exact heartbeat timeout emits one stable-order heartbeat broadcast;
45. the D19 quorum cadence remains behavior-free in D14;
46. a deterministic three-node network elects one leader, durably replicates a
    proposal, commits it, and converges follower logs and commit indexes;
47. basic repeated rejection retries reconcile a divergent uncommitted suffix.

## Design review resolution

The GPT-5.6 Sol design review found one correctness blocker and two defensive
hardening gaps:

1. follow-up append after an accepted response did not exclude self;
2. zero-index rejection could resend an identical probe forever; and
3. snapshot deferral did not distinguish compaction from unavailable retained
   state.

All findings are accepted. Follow-up replication is remote-only, index-zero
rejections are ignored without retry, and only `StorageError.Compacted`
defers to D16.

## Development sequence

D14 uses strict test-driven development:

1. review and accept this design;
2. update historical structural-leader tests and add D14 tests first;
3. run the focused target and record the expected red state;
4. implement only basic replication and proposal behavior;
5. preserve explicit D15-D21 boundaries;
6. run formatting and the full suite;
7. resolve substantiated code-review findings before commit.

## Completion criteria

D14 is complete when:

- this design is reviewed and accepted;
- the no-op and every acknowledgement obey persistence ordering;
- proposals replicate without caller mutation;
- follower log matching and basic leader retry work;
- simple and joint current-term commit safety pass;
- a deterministic three-node proposal commits;
- no flow-control, snapshot, membership, read, or transfer behavior is
  implemented early;
- formatting, build, and the full suite pass;
- code review has no unresolved substantiated findings;
- the reviewed changes are committed before D15 starts.
