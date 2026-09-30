# D12 Core State-Machine Shell Design

- **Status:** Accepted
- **Date:** 2026-09-30
- **DAG node:** D12
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D12 composes the completed log, quorum, progress, configuration-change, and
read-only components into the persistent shell of one Raft peer. It defines
configuration validation, restart initialization, volatile role state,
logical clocks, role reset semantics, and outbound message durability queues.

Later nodes add protocol behavior to this shell:

- D13 handles campaigns, votes, terms received from messages, and election
  outcomes.
- D14 appends the leader no-op entry and implements append and heartbeat
  processing.
- D17 applies live membership changes.
- D18 integrates safe reads.
- D19 adds quorum checking, pre-vote policy, and lease-read behavior.
- D20 adds leadership transfer.
- D21 exposes the shell through `RawNode` and `Ready`.
- D25 turns the durability queues into asynchronous storage messages.

## Goals

- Provide the public configuration object consumed later by `RawNode`.
- Validate configuration before reading storage or allocating core state.
- Normalize default quota values without mutating the caller's configuration.
- Restore persisted log, hard state, and membership atomically.
- Start every peer as a follower with progress derived from the restored log.
- Model follower, pre-candidate, candidate, and leader role state.
- Preserve term, vote, timer, progress, and read-only reset semantics.
- Make randomized election timeouts testable without wall-clock time.
- Separate messages that may be sent immediately from messages that require
  durable local state first.
- Defensively own configuration inputs and queued protobuf messages.

## Non-goals

- Dispatching or handling inbound Raft messages; D13 starts `Step`.
- Starting campaigns or tallying election responses; D13 owns elections.
- Appending the leader no-op entry; D14 completes leader activation.
- Sending append, heartbeat, or snapshot RPCs; D14-D16 own replication.
- Applying configuration changes after startup; D17 owns live membership.
- Producing `Ready` batches or tracking accepted batches; D21 owns that API.
- Creating asynchronous storage request messages; D25 owns that behavior.
- Providing synchronization. One serialized event loop owns `RaftCore`.

## Public configuration

```csharp
namespace DotnetRaft;

public sealed class RaftConfig
{
    public ulong Id { get; init; }
    public int ElectionTick { get; init; } = 10;
    public int HeartbeatTick { get; init; } = 1;
    public IStorage? Storage { get; init; }
    public ulong Applied { get; init; }
    public bool AsyncStorageWrites { get; init; }
    public ulong MaxSizePerMessage { get; init; } = ulong.MaxValue;
    public ulong MaxCommittedSizePerReady { get; init; }
    public ulong MaxUncommittedEntriesSize { get; init; }
    public int MaxInflightMessages { get; init; } = 256;
    public ulong MaxInflightBytes { get; init; }
    public bool CheckQuorum { get; init; }
    public bool PreVote { get; init; }
    public ReadOnlyOption ReadOnlyOption { get; init; }
    public IRaftLogger? Logger { get; init; }
    public bool DisableProposalForwarding { get; init; }
    public bool DisableConfChangeValidation { get; init; }
    public bool StepDownOnRemoval { get; init; }
}
```

The option set is created now because the deterministic core must own a stable
configuration snapshot. The nodes that use individual options remain
responsible for their behavior.

`MaxSizePerMessage = ulong.MaxValue` gives the normal unlimited default.
Callers may explicitly set it to zero to request at most one entry per append
message, matching the reference.

## Validation and normalization

Construction produces an internal immutable validated snapshot and never
modifies the supplied `RaftConfig`.

Validation rejects:

```text
Id == 0
Id == LocalAppendThread or LocalApplyThread
HeartbeatTick <= 0
ElectionTick <= HeartbeatTick
ElectionTick > (int.MaxValue / 2) + 1
Storage == null
MaxInflightMessages <= 0
finite MaxInflightBytes < MaxSizePerMessage
ReadOnlyOption == LeaseBased without CheckQuorum
```

Normalization applies:

```text
MaxCommittedSizePerReady == 0
    => MaxSizePerMessage

MaxUncommittedEntriesSize == 0
    => ulong.MaxValue

MaxInflightBytes == 0
    => ulong.MaxValue

Logger == null
    => NullRaftLogger.Instance
```

Invalid public arguments throw the corresponding `ArgumentException`
subtype before any storage method is called. Invalid persisted state throws
`RaftInvariantException` or the existing configuration-change exception.

## Public role state

```csharp
namespace DotnetRaft;

public enum RaftRole
{
    Follower,
    Candidate,
    Leader,
    PreCandidate,
}

public sealed record SoftState(
    ulong LeaderId,
    RaftRole Role);
```

`SoftState` is volatile and will later be surfaced by `Ready`; it never needs
durable storage.

## Core composition

```csharp
namespace DotnetRaft.Core;

internal sealed class RaftCore
{
    internal RaftCore(RaftConfig config);

    internal ulong Id { get; }
    internal ulong Term { get; }
    internal ulong Vote { get; }
    internal ulong LeaderId { get; }
    internal RaftRole Role { get; }
    internal bool IsLearner { get; }

    internal RaftLog Log { get; }
    internal ProgressTracker Tracker { get; }
    internal ReadOnlyTracker ReadOnly { get; }

    internal SoftState SoftState { get; }
    internal HardState HardState { get; }
}
```

The core also stores normalized limits, feature flags, elapsed clock values,
the randomized election timeout, pending configuration index, uncommitted
payload size, leadership-transfer target, and both outbound queues. Internal
properties expose these values to their owning future nodes and tests.

## Initialization order

Construction is fail-fast and follows this order:

1. validate and normalize the public configuration;
2. create `RaftLog` with the normalized committed-entry apply quota;
3. read `StorageState` from storage;
4. reject a null storage state or null `ConfState`;
5. create `ProgressTracker` with normalized inflight limits;
6. restore the persisted `ConfState` through D10 using the log's last index;
7. install the returned configuration and progress map;
8. derive `IsLearner` from the local progress, or `false` when absent;
9. load a non-empty persisted `HardState`;
10. apply `RaftConfig.Applied` when it is nonzero; and
11. become a follower at the restored term with no known leader.

The existing `RaftLog` and D10 validation enforce:

```text
initial log boundaries are valid
restored membership is structurally valid
every configured member has progress
snapshot/applied <= commit <= last index
```

## Hard-state restoration

A hard state is empty when term, vote, and commit are all zero, regardless of
protobuf field presence.

For a non-empty hard state:

```text
Log.Committed <= HardState.Commit <= Log.LastIndex
```

The lower bound begins at the retained snapshot boundary. An out-of-range
commit fails before term or vote is installed. A valid hard state sets:

```text
Log.Committed = Commit
Term          = Term
Vote          = Vote
```

`RaftConfig.Applied` is then passed through `RaftLog.AppliedTo`, which rejects
an applied index below the retained boundary or above the restored commit.

## Core reset

`Reset(term)` is the shared role-transition primitive.

It rejects a term lower than the current term. When the term increases, it:

```text
Term = term
Vote = 0
```

When the term is unchanged, the existing vote is preserved.

Every reset:

```text
LeaderId = 0
ElectionElapsed = 0
HeartbeatElapsed = 0
RandomizedElectionTimeout = ElectionTick + random offset
LeaderTransferee = 0
Tracker votes are cleared
PendingConfigurationIndex = 0
UncommittedSize = 0
ReadOnly = a new tracker with the same option
```

Every tracked progress record is reset while preserving only learner status:

```text
State = Probe
Match = 0
Next = Log.LastIndex + 1
RecentActive = false
PendingSnapshot = 0
LastSentCommit = 0
AppendFlowPaused = false
Inflights are empty
```

The local record instead uses:

```text
Match = Log.LastIndex
Next = Log.LastIndex + 1
```

`RaftLog` already rejects `LastIndex == ulong.MaxValue`, so every reset has a
representable next index.

## Role transitions

### Become follower

```text
Reset(term)
LeaderId = supplied leader
Role = Follower
```

The supplied term may equal or exceed the current term.

### Become candidate

Leader-to-candidate is invalid and throws `RaftInvariantException`.

```text
Reset(Term + 1)
Vote = Id
Role = Candidate
```

Term overflow fails explicitly.

### Become pre-candidate

Leader-to-pre-candidate is invalid and throws.

Pre-candidate is intentionally not a full reset:

```text
Tracker votes are cleared
LeaderId = 0
Role = PreCandidate
```

Term, vote, elapsed clocks, randomized timeout, progress, pending reads, and
uncommitted accounting are preserved, matching the reference pre-vote phase.

### Become leader

Follower-to-leader is invalid and throws. D13 reaches this transition only
from candidate or pre-candidate election flow.

```text
Reset(Term)
LeaderId = Id
Role = Leader
local progress becomes Replicate
local progress RecentActive = true
PendingConfigurationIndex = Log.LastIndex
```

The local node must have a progress record. Missing local progress indicates an
invalid direct transition and throws.

D12 stops before appending the leader no-op entry. D14 extends successful
leader activation with that append and its durability-dependent self
acknowledgement.

## Logical clocks and randomization

Production timeout offsets use a cryptographically backed random integer in:

```text
[0, ElectionTick)
```

The resulting timeout is:

```text
[ElectionTick, 2 * ElectionTick - 1]
```

`RaftCore` has an internal constructor overload accepting a deterministic
offset provider for tests. Provider output outside the required range is an
invariant failure.

The shell exposes stable internal clock operations for later protocol nodes:

```csharp
bool TickElectionClock();
LeaderClockTick TickLeaderClocks();
bool PastElectionTimeout { get; }
```

`TickElectionClock`:

1. increments `ElectionElapsed`;
2. returns `false` for an unpromotable node;
3. returns `false` before the randomized timeout; and
4. on a promotable timeout, resets `ElectionElapsed` to zero and returns
   `true`.

It does not start a campaign; D13 consumes the signal.

`TickLeaderClocks` requires leader state, increments both elapsed counters,
uses the fixed election timeout for quorum-check cadence, uses the heartbeat
timeout for heartbeat cadence, resets each elapsed counter when due, and
returns both due flags. D14, D19, and D20 consume those signals.

When both flags are due, consumers must preserve the pinned processing order:

1. process the election-timeout quorum check;
2. expire leadership transfer only if the node is still leader;
3. re-read `Role`; and
4. send a heartbeat only if the node is still leader.

A quorum check may step the node down, in which case the coincident heartbeat
must be suppressed. D19 owns the integration test because D12 does not yet
implement quorum-check behavior.

Elapsed counter overflow fails explicitly.

`Promotable` is true only when local progress exists, is not a learner, and
the log has no unstable snapshot awaiting or undergoing persistence.

## Outbound message normalization

`Send(message)` clones the supplied protobuf before storing or modifying it.
The caller's message remains independent.

If `From == 0`, the clone receives the local ID.

Vote-family messages are:

```text
MsgVote
MsgVoteResp
MsgPreVote
MsgPreVoteResp
```

They must already carry a nonzero term. Their term is not rewritten.

Every other message must enter with term zero. The core attaches its current
term except to:

```text
MsgProp
MsgReadIndex
```

Forwarded proposals and read-index requests intentionally retain no term.

Invalid term shape throws `RaftInvariantException` before queue mutation.

## Durability queues

Messages whose safety depends on persistent local state enter the
after-append queue:

```text
MsgAppResp
MsgVoteResp
MsgPreVoteResp
```

This includes rejection responses and may include messages addressed back to
the local node.

All other messages enter the immediate queue. An immediate message addressed
to the local node is an invariant failure.

The core owns queued messages until a `Take...` operation transfers them in a
new array and clears the corresponding queue:

```csharp
Message[] TakeMessages();
Message[] TakeMessagesAfterAppend();
```

D13-D20 may process a durability-complete self-message through `Step` or move
a remote one to the immediate queue. D21 later maps these queues onto
`Ready`, and D25 maps them onto local storage request messages.

Role resets do not discard already produced outbound messages.

## Ownership

- `RaftConfig` is read once into an immutable normalized snapshot.
- `StorageState`, `HardState`, and `ConfState` are not retained by reference.
- D10 returns owned configuration and progress instances.
- `Send` clones every queued message.
- Taking a queue transfers its message objects and removes all core
  references to them.
- `SoftState` and `HardState` accessors return new values.

## Invariants

```text
Term never decreases
Vote is cleared exactly when Term increases
Follower/candidate resets clear leader identity
local Match == Log.LastIndex after every full reset
every progress Next == Log.LastIndex + 1 after every full reset
candidate Vote == Id
leader LeaderId == Id
leader local progress is Replicate and recently active
randomized timeout is in [ElectionTick, 2 * ElectionTick)
immediate messages never target Id
durability-dependent responses never enter the immediate queue
queued protobuf messages are core-owned
```

## Test plan

### Configuration

1. valid defaults normalize quotas and logger without mutating the input;
2. zero and reserved local IDs fail before storage access;
3. heartbeat, election, and randomized-timeout representability are
   validated before storage access;
4. null storage fails explicitly;
5. nonpositive inflight message limits fail;
6. finite inflight bytes below message size fail;
7. lease reads require quorum checking;
8. all future feature flags and finite limits are copied into core state.

### Initialization and restart

9. empty storage starts a term-zero follower with no leader;
10. an explicitly present all-zero hard state is empty by scalar value, even
    above a nonzero snapshot boundary;
11. persisted hard state, entries, snapshot membership, and applied index are
    restored;
12. voter and learner progress use the restored last-index reset values;
13. a local learner is recognized, while an absent local ID is not;
14. hard-state commit below the snapshot boundary fails atomically;
15. hard-state commit above the last index fails atomically;
16. configured applied index outside the retained/committed range fails;
17. malformed persisted membership fails without partial core construction;
18. null storage state or `ConfState` fails explicitly;
19. mutating storage-supplied hard/config state after construction cannot
    change the core, and mutating one returned hard state cannot change later
    accessors.

### Role reset and clocks

20. same-term follower transition preserves vote;
21. higher-term follower transition clears vote;
22. lower-term follower transition fails atomically;
23. candidate transition increments term, votes for self, and rejects leader
    transition;
24. candidate transition at `ulong.MaxValue` fails atomically;
25. pre-candidate clears tracker votes and leader identity while preserving
    term, persistent vote, clocks, randomized timeout, progress, pending
    reads, and accounting;
26. full resets clear votes, pending reads, accounting, timers, snapshots,
    pauses, inflights, and replication state while preserving learners;
27. leader transition rejects followers, requires local progress, and sets the
    local replicate state without appending a no-op;
28. deterministic timeout offsets cover both inclusive range endpoints;
29. promotability covers absent local progress, learner progress, a pending
    snapshot, the same snapshot after persistence begins, and the state after
    snapshot acknowledgement;
30. election clock signals only a promotable randomized timeout;
31. leader clocks signal and independently reset fixed election and heartbeat
    intervals;
32. invalid random offsets and elapsed-counter overflow fail explicitly;
33. D19 reserves an integration test where coincident quorum and heartbeat
    deadlines step down for inactive quorum without sending a heartbeat.

### Outbound queues

34. send fills a missing sender and defensively clones the input;
35. vote-family messages require and preserve an explicit term;
36. ordinary messages reject an explicit term and receive the current term;
37. proposals and read-index requests retain zero term;
38. append, vote, and pre-vote responses enter the after-append queue,
    including rejections and self-addressed responses;
39. other self-addressed output fails before queue mutation;
40. taking either queue preserves order, clears retained references, and does
    not drain the other queue;
41. same-term, higher-term, and pre-candidate transitions preserve both queues
    byte-for-byte and in order.

## Development sequence

D12 uses strict test-driven development:

1. review and accept this design;
2. add configuration, initialization, transition, clock, and queue tests;
3. run the focused target and record the expected compile failure;
4. implement the minimum shell required by the accepted tests;
5. refactor without adding election or replication behavior;
6. run formatting and the full suite;
7. resolve substantiated code-review findings before commit.

## Completion criteria

D12 is complete when:

- this design is reviewed and accepted;
- public configuration validation and normalization are implemented;
- restart restoration produces the expected follower state;
- role resets, deterministic clocks, and durability queues pass focused tests;
- no D13+ protocol behavior is implemented prematurely;
- formatting, build, and the full suite pass;
- code review has no unresolved substantiated findings;
- the reviewed changes are committed before D13 starts.
