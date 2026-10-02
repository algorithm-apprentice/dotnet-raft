# D21 RawNode and Ready Design

- **Status:** Accepted
- **Date:** 2026-10-03
- **DAG node:** D21
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D21 adds the first public integration boundary around the completed
deterministic Raft core.

`RawNode` is a thread-unsafe, host-driven facade. The host supplies logical
ticks and inbound messages, consumes immutable `Ready` batches, persists and
applies their work in order, sends their outbound messages, and acknowledges
completion through `Advance`.

This preserves the pinned `etcd/raft` architecture:

```text
host transport/storage/state machine
              |
          RawNode
              |
          RaftCore
```

`RawNode` does not own threads, transport, durable storage, or the replicated
application state machine.

## Goals

- Expose deterministic ticking, campaigning, proposals, message stepping,
  read-index requests, leadership transfer, and reporting.
- Expose pending state-machine work through detached `Ready` values.
- Preserve persistence-before-response ordering.
- Track accepted unstable and applying work without redelivery.
- Acknowledge persisted and applied work through synchronous `Advance`.
- Expose exact `HasReady` and `MustSync` decisions.
- Reject network-delivered local messages and responses from unknown peers.
- Preserve message, entry, snapshot, read-state, and configuration ownership.
- Cover restart, snapshot, pagination, auto-leave, and bounded-log behavior
  through the public integration loop.

## Non-goals

- Bootstrap helpers; D22 owns bootstrap and restart utilities.
- Status and progress inspection; D22 owns status snapshots.
- The concurrent `Node` wrapper; D24 owns serialization and lifecycle.
- Asynchronous storage messages; D25 owns `MsgStorageAppend`,
  `MsgStorageApply`, completion responses, and ABA protection.
- NuGet API finalization and compatibility guarantees; D26 owns release
  hardening.
- `TickQuiesced`, which is deprecated in the pinned implementation.

## Synchronous-only boundary

D21 supports:

```text
RaftConfig.AsyncStorageWrites == false
```

Constructing `RawNode` with asynchronous storage writes enabled throws
`NotSupportedException`. Silently treating the option as synchronous would
misrepresent durability ordering. D25 removes this guard when the local
storage-message protocol is implemented.

The underlying core retains its existing async configuration field so D25 can
add the reference protocol without redesigning state.

## Public types

### RawNode

```csharp
public sealed class RawNode
{
    public RawNode(RaftConfig config);

    public void Tick();
    public void Campaign();
    public void Propose(ReadOnlySpan<byte> data);
    public void ProposeConfChange(ConfChange change);
    public void ProposeConfChange(ConfChangeV2 change);

    public ConfState ApplyConfChange(ConfChange change);
    public ConfState ApplyConfChange(ConfChangeV2 change);

    public void Step(Message message);

    public bool HasReady();
    public Ready Ready();
    public void Advance(Ready ready);

    public void ReportUnreachable(ulong id);
    public void ReportSnapshot(
        ulong id,
        SnapshotStatus status);
    public void TransferLeader(ulong transferee);
    public void ForgetLeader();
    public void ReadIndex(ReadOnlySpan<byte> context);

    public static bool MustSync(
        HardState state,
        HardState previousState,
        int entryCount);
}
```

All methods execute synchronously on the caller's thread. `RawNode` is not
thread-safe.

### SnapshotStatus

```csharp
public enum SnapshotStatus
{
    Success,
    Failure,
}
```

### Ready

```csharp
public sealed class Ready
{
    public SoftState? SoftState { get; }
    public HardState? HardState { get; }
    public IReadOnlyList<ReadState> ReadStates { get; }
    public IReadOnlyList<Entry> Entries { get; }
    public Snapshot? Snapshot { get; }
    public IReadOnlyList<Entry> CommittedEntries { get; }
    public IReadOnlyList<Message> Messages { get; }
    public bool MustSync { get; }
}
```

`null` `SoftState`, `HardState`, or `Snapshot` means that category has no new
work in the batch. Collections are never `null`.

## Construction baseline

The constructor creates `RaftCore`, then captures:

```text
previous soft state = core.SoftState
previous hard state = core.HardState
```

Persisted state loaded during core construction is therefore the comparison
baseline, not new `Ready` work.

On restart, already committed but unapplied entries can produce an initial
`Ready`, while unchanged persisted hard and soft state do not.

`RaftConfig.Applied` must be the highest index represented by the recovered
application state, not the pre-crash core's logical admitted cursor.
`IStorage.GetInitialState().ConfState` must describe the membership physically
established at that same recovered point. If a storage implementation derives
its initial `ConfState` only from snapshot metadata, the host must not set
`Applied` past a later accepted configuration entry unless it first creates a
matching application snapshot or otherwise supplies the later `ConfState`.

Before constructing `RawNode`, recovery must also establish:

```text
recovered HardState.Commit >= RaftConfig.Applied
```

A commit-only `MustSync == false` write may be lost while separately durable
application state survives at a higher index. The host must resolve that split
before construction by either:

- restoring the application to a point no higher than the recovered commit;
  or
- after verifying that the physically recovered state and retained
  log/snapshot cover the index, durably repairing `HardState.Commit` to at
  least the physical-applied index.

The repair is safe because physical application is permitted only for
committed entries whose log data was previously durably persisted. It changes
neither term nor vote. The repaired hard state must be durable before the
facade can emit work or messages.

Consequently, a crash after early `Advance` but before physical application
reconstructs `RawNode` with the lower physical cursor. The committed suffix
that was admitted but not physically completed is delivered again.

## Input methods

### Tick

`Tick` dispatches by current role:

```text
leader     -> RaftCore.TickLeader()
other role -> RaftCore.TickElection()
```

### Campaign

`Campaign` steps a local `MsgHup`. D19 chooses pre-election or ordinary
election according to configuration.

### Propose

`Propose` owns a copy of the caller's bytes and steps one local `MsgProp`
normal entry.

Proposal drops remain explicit exceptions. `RawNode` does not convert them
into success-shaped results. The existing `ProposalDroppedException` becomes
public so facade callers can handle this retryable drop condition without
matching an internal implementation type.

### Configuration changes

The V1 overload serializes an `EntryConfChange`. The V2 overload serializes an
`EntryConfChangeV2`. Caller protobufs are cloned or serialized without
mutation.

`ApplyConfChange` delegates to the D17 application path and returns a detached
`ConfState`.

### ReadIndex

`ReadIndex` owns a copy of the caller's context and steps a one-entry
`MsgReadIndex`.

### Transfer and reports

- `TransferLeader(id)` steps `MsgTransferLeader` with `From = id`.
- `ForgetLeader()` steps `MsgForgetLeader`.
- `ReportUnreachable(id)` steps `MsgUnreachable` with `From = id`.
- `ReportSnapshot(id, Success)` steps accepted `MsgSnapStatus`.
- `ReportSnapshot(id, Failure)` sets `Reject = true`.

These facade-generated local inputs bypass the public network `Step` filter.

For every outbound `MsgSnap`, the host must eventually call
`ReportSnapshot` after transport reports success or failure. Failing to report
can leave the target progress paused in snapshot state.

## Public Step filtering

`Step` owns an input clone and rejects invalid network input before term
handling:

1. every message whose `From` is a reserved local storage target is rejected
   because D21 has no active storage worker;
2. any `MessageClassifier.IsLocal(type)` message is rejected as a local-only
   message;
3. any response message whose sender is absent from `Tracker.Progress`,
   including sender zero, is rejected as an unknown peer response; and
4. accepted messages are delegated to `RaftCore.Step`.

D25 will replace the blanket reserved-target rejection with exact allowed
pairings:

```text
MsgStorageAppendResp from LocalAppendThread
MsgStorageApplyResp  from LocalApplyThread
```

It will not add a blanket local-target exemption.

The pinned `MessageClassifier.IsLocal` set does not include
`MsgForgetLeader`, so D21 preserves that public `Step` parity. The transport
boundary is responsible for authenticating peers and restricting which Raft
message types it accepts. Applications should use the dedicated
`ForgetLeader()` facade for local intent.

Rejecting an unknown higher-term response before core stepping is intentional.
It prevents forged responses, including zero-origin `MsgReadIndexResp`, from
changing term or creating output.

## Ready detection

When no batch is awaiting `Advance`, `HasReady` returns true if any of the
following is present:

```text
core.SoftState != previous soft state
core.HardState != previous hard state
next unstable snapshot exists
immediate messages exist
after-append messages exist
next unstable entries exist
next committed entries exist
read states exist
```

Committed-entry detection uses:

```text
allowUnstable = true
```

for synchronous operation. A committed entry may therefore be persisted and
applied in the same `Ready`, provided the host follows the required ordering.

While a previously returned batch awaits `Advance`, `HasReady` returns false.
New inputs may still be stepped and accumulate work; that work becomes visible
after the outstanding batch advances.

## Ready construction

`Ready()` first rejects a second outstanding batch. It then builds:

```text
Entries          = Log.GetNextUnstableEntries()
CommittedEntries = Log.GetNextCommittedEntries(allowUnstable: true)
Messages         = immediate messages
                 + non-local after-append messages
Snapshot         = next unstable snapshot, if any
ReadStates       = current read-state output
```

State deltas are included only when changed:

```text
SoftState = current soft state when changed
HardState = current hard state when changed
```

Immediate messages precede non-local after-append messages, matching the
pinned batch shape.

Self-directed after-append messages are not exposed. They are retained for
`Advance`, ensuring the state transitions they acknowledge cannot occur before
the host reports completion of the required persistence operation.

## MustSync

`MustSync` is true when:

```text
entryCount != 0
|| current.Term != previous.Term
|| current.Vote != previous.Vote
```

A commit-only hard-state change does not require a forced synchronous flush
according to the pinned durability rule.

`MustSync` distinguishes atomic publication from forced durability:

- every batch is published without a torn snapshot/entry/hard-state
  generation;
- entries always make `MustSync` true;
- snapshots are always durably persisted even when `MustSync` is false;
- term or vote changes require a forced durable hard-state write; and
- a commit-only hard-state generation with `MustSync == false` may use a
  non-forced write and may be lost as a whole on crash.

`MustSync == false` does not relax the complete-batch publication boundary or
persistence-before-message ordering. It only informs the host's
HardState/entry WAL flush decision. If separately durable application progress
outlives a lost commit-only generation, restart performs the recovery repair
defined above.

The public static helper rejects negative entry counts and compares hard-state
values rather than protobuf presence bits.

## Accepting a Ready

`Ready()` atomically accepts the returned batch before returning it:

1. update the previous soft-state baseline when emitted;
2. update the previous hard-state baseline when emitted;
3. clear only the read states included in the batch;
4. drain immediate and after-append core queues;
5. retain self-directed after-append messages for `Advance`;
6. call `Log.AcceptUnstable()` to mark the emitted entries or snapshot in
   progress;
7. call `Log.AcceptApplying()` for the exact committed-entry suffix; and
8. retain detached acknowledgement metadata for `Advance`.

This prevents the same entries, snapshot, committed entries, messages, or read
states from being emitted twice.

The metadata retained internally is derived from core state, not from mutable
objects returned to the caller.

## Host processing order

For every accepted batch, the host must:

1. publish `Snapshot`, `Entries`, and `HardState` as one crash-consistent
   logical transaction, forcing durability when required below;
2. after that complete generation is published, send `Messages`;
3. restore `Snapshot`, if present, into the application state machine before
   physically applying any later entry;
4. apply `CommittedEntries` in order, calling `ApplyConfChange` for accepted
   configuration entries at their application position; and
5. call `Advance` with that exact `Ready` instance.

For a simple mutable storage implementation, the publication order is:

```text
ApplySnapshot
Append Entries
SetHardState
```

A storage implementation should stage or WAL the complete generation and
atomically publish it. When `MustSync` is true, the generation must be forced
durable before publication completes. A snapshot must also be forced durable
regardless of `MustSync`. A commit-only `MustSync == false` generation can use
a non-forced write.

After every crash, storage must expose either the prior generation or a
self-consistent new generation. It must never expose:

- `HardState.Commit` beyond its durable log;
- a hard-state commit below the published snapshot boundary; or
- post-snapshot entries without the snapshot generation they extend.

`Ready` can contain both a snapshot and later entries. Applying the snapshot
after those entries would erase them in `MemoryStorage`.

No `Ready.Messages` may be released until the complete generation is
published and every required forced-durability operation is complete.

If persistence fails, the host must not call `Advance`.

The conservative integration loop completes state-machine restoration and
entry application before `Advance`. The pinned synchronous contract also
allows pipelining: after persistence publication, the host may irrevocably
admit the snapshot and committed entries to an externally ordered application
pipeline, then call `Advance` while physical application continues. The host
must guarantee that admitted work cannot be lost or fail silently and that
later batches remain physically ordered behind it.

Pipelined mode must maintain a separate host-owned physical-applied cursor.
`RaftCore.Log.Applied` becomes the logical admitted cursor after early
`Advance`; it is not proof that the application state machine has finished the
work.

Every `ReadState` is admitted to the same ordered pipeline. A linearizable
read may execute only after:

```text
physical snapshot restoration is complete
&& physicalApplied >= ReadState.Index
```

The host must never use the core's logical admitted cursor as this read
barrier.

Configuration entries require a synchronous ordered decision before
admission-only acknowledgement. Before an early `Advance` that covers an
`EntryConfChange` or `EntryConfChangeV2`, the host must:

1. decode the entry in log order;
2. make the application-level accept or reject decision solely from the
   deterministic replicated application state at that log position, never
   from node health, local timing, transport observations, or other external
   information;
3. for an accepted change, synchronously call the matching `ApplyConfChange`
   and retain its detached returned `ConfState`; or
4. for a rejected change treated as a no-op, do not call `ApplyConfChange`.

The pinned `Node` interface is authoritative for this rule. Its rejected-entry
contract supersedes the older `doc.go` text that describes calling with a
zeroed V1 node ID.

This preserves D17's invariant that an accepted membership is installed before
`RaftCore.AppliedTo` reaches or passes the configuration entry, while matching
the pinned rejected-change rule. If the host cannot complete the ordered
decision and any required apply call, it must delay `Advance`.

Pipelined mode must also base all local state snapshots, compaction, and
restart metadata on physical completion:

```text
application snapshot index <= physicalApplied
compaction index <= durably published application snapshot index
snapshot ConfState = configuration physically established at that index
```

The `ConfState` returned by the latest physically applied accepted
`ApplyConfChange` must be recorded in application snapshots. A host must never
label a snapshot with the logical admitted cursor while physical state is
behind it.

If `ApplyConfChange` throws after dispatch into the core, `RawNode` enters a
terminal faulted state. The host must not retry the change or call `Advance`
on that instance. It must discard the instance and reconstruct from the last
complete persistence generation, using the physical application cursor before
the failed entry and its matching `ConfState`; normal committed-entry replay
then presents the entry to the new instance. This conservative rule covers
the existing non-transactional case in which tracker installation succeeded
but leader follow-up work failed. Argument validation completed before core
dispatch does not fault the instance.

## Advance

`Advance` rejects:

- `null`;
- a `Ready` not produced by this `RawNode`;
- a stale or already advanced batch; and
- a call when no batch is outstanding.

For a valid batch it performs the pinned synchronous completion order:

1. step retained self-directed after-append messages;
2. stabilize the exact final emitted entry through `Log.StableTo`;
3. acknowledge the exact emitted snapshot through
   `Log.AcknowledgeSnapshot`;
4. acknowledge the exact final committed entry through `RaftCore.AppliedTo`;
5. reduce the uncommitted payload budget by the applied entries' payload
   bytes; and
6. clear the outstanding batch.

Self responses run before unstable entries are removed. This preserves the
pinned fast path and allows a singleton leader's persisted self
acknowledgement to commit its no-op before that entry leaves the unstable log.

Messages or entries created after `Ready()` was accepted are not cleared or
acknowledged by advancing the older batch.

`Advance` is therefore an acknowledgement of:

- completed persistence publication and every required forced flush; and
- completed physical application **or** irrevocable ordered admission to the
  host's application pipeline.

It is not required to wait for slow physical application when the host
provides that ordering and reliability guarantee. This permits ticks, inbound
messages, and later `Ready` batches to continue without violating the pinned
synchronous contract.

The admission option does not weaken:

- the synchronous accepted/rejected configuration decision, including
  `ApplyConfChange` only for accepted changes, before advancing across a
  configuration entry;
- the physical-applied read barrier for `ReadState`; and
- physical-applied snapshot creation, configuration capture, compaction, and
  restart boundaries.

## Ownership

### Inputs

`RawNode` never mutates caller-provided:

- `Message`;
- proposal or read-index byte buffers;
- `ConfChange`;
- `ConfChangeV2`; or
- `Ready`.

### Outputs

Every `Ready` owns detached:

- hard state;
- entries;
- committed entries;
- snapshot;
- messages; and
- collection containers.

`SoftState` and `ReadState` are immutable values. `ByteString` request
contexts are immutable.

Mutating returned protobufs is unsupported but cannot mutate the core's queued
messages, unstable log, snapshot, previous-state baselines, or retained
acknowledgement metadata.

## Snapshot lifecycle

When a follower accepts a snapshot:

1. `Ready.Snapshot` exposes a detached snapshot;
2. its persistence-dependent `MsgAppResp` appears in `Ready.Messages`;
3. accepting `Ready` marks the internal snapshot in progress;
4. no committed entries are exposed while that unstable snapshot exists;
5. the host publishes the snapshot before any same-batch entries and restores
   it to the application state machine;
6. `Advance` moves `Applied`, clears the unstable snapshot, and permits later
   committed entries; and
7. every outbound snapshot message is followed by a success or failure
   `ReportSnapshot`.

The response may be sent only after the host persists the batch.

## Committed-entry pagination

`Ready` exposes only the exact committed slice returned by
`MaxCommittedSizePerReady`.

Accepting the batch moves `Log.Applying` only through the final exposed index.
Advancing moves `Log.Applied` through that same index and releases only that
batch's encoded-size and uncommitted-payload accounting.

This prevents restart or page-boundary gaps where an entry is skipped because
the applied cursor advances beyond the entries actually delivered.

## Failure behavior

- Invalid configuration still fails during `RaftCore` construction.
- Async storage configuration fails explicitly in `RawNode`.
- Local network messages and unknown-peer responses fail before core term
  handling.
- Proposal drops remain explicit.
- `Ready` lifecycle misuse throws `InvalidOperationException`.
- Storage errors from the host are never swallowed by `RawNode`.
- An exception after dispatching `ApplyConfChange` faults that `RawNode`;
  subsequent operations reject use of the partially mutated instance;
- Invariant failures remain explicit and do not produce success-shaped
  batches.

## Test plan

### Facade inputs

- tick dispatches to leader and election clocks;
- campaign drives pre-vote and ordinary election paths;
- proposal and read-index byte buffers are owned;
- V1 and V2 configuration proposal bytes and entry types are exact;
- applying V1 and V2 changes returns detached configuration state;
- transfer, forget-leader, unreachable, and snapshot reports create the
  pinned local messages;
- public `Step` rejects every local message type and unknown-peer response;
- public `Step` rejects response senders zero and both reserved storage IDs;
- reserved-ID rejection is tested with non-local, non-response messages so it
  cannot pass through the unknown-response branch alone;
- the pinned network-visible `MsgForgetLeader` classification is documented
  and covered separately from facade use;
- rejected higher-term unknown responses do not change local term;
- forged zero-origin `MsgReadIndexResp` cannot create a `ReadState`;
- accepted input messages remain unchanged.

### Ready and HasReady

- constructor baselines suppress unchanged persisted hard and soft state;
- each individual source makes `HasReady` true;
- `Ready()` consumes messages, read states, and state deltas;
- a read-only readiness probe does not consume work;
- work added after acceptance survives the older `Advance`;
- `HasReady` remains false while a batch is outstanding;
- advancing exposes accumulated later work;
- a second `Ready` and wrong/stale `Advance` are rejected;
- early `Advance` after persistence and irrevocable admission exposes later
  work while the modeled
  application pipeline preserves physical order;
- a pipelined `ReadState` remains blocked until the host's physical-applied
  cursor reaches its index and any preceding snapshot restoration completes;
- early advancement across an accepted configuration entry is rejected by the
  host harness until `ApplyConfChange` has run in order;
- a rejected configuration entry advances as a no-op without calling
  `ApplyConfChange`;
- accept/reject decisions use only modeled replicated application state;
- an `ApplyConfChange` core failure faults the facade, rejects retry and
  `Advance`, and recovery replays from the preceding physical cursor.

### Durability ordering

- singleton election requires one `Advance` for the self vote before becoming
  leader;
- leader no-op requires persistence and `Advance` before its self append
  acknowledgement commits;
- follower append responses are exposed with entries but not before them;
- self-directed after-append messages are never exposed;
- self messages run before entry stabilization;
- term/vote changes and entries set `MustSync`;
- commit-only and apply-only batches do not set `MustSync`;
- a modeled commit-only or snapshot-only `MustSync == false` batch still
  withholds outbound messages until the complete persistence generation is
  published;
- forced term/vote or entry generations survive every modeled crash after
  publication;
- snapshot-only generations force snapshot durability even when `MustSync` is
  false;
- commit-only `MustSync == false` recovery exposes either the complete old
  generation or the complete new generation within Raft storage;
- when durable application progress outlives the old recovered commit, restart
  durably repairs commit to at least the physical-applied cursor before
  constructing `RawNode`; and
- no modeled crash exposes a torn snapshot/entry/hard-state generation.

### Storage and application

- unstable entries stabilize only through `Advance`;
- a snapshot remains in progress until `Advance`;
- snapshot-plus-entry batches publish snapshot, entries, and hard state in a
  restart-safe generation;
- crash cuts before and after generation publication restart from a
  self-consistent state;
- a crash after early `Advance` but before physical application reconstructs
  with `RaftConfig.Applied` at the physical cursor and redelivers the admitted
  but incomplete committed suffix;
- restart rejects or avoids an `Applied`/initial-`ConfState` pair that does not
  describe the same physically recovered application point;
- restart never passes `Applied > HardState.Commit`; the crash model covers a
  lost non-forced commit with surviving application progress and verifies
  durable commit repair before construction;
- early logical advancement cannot move application snapshot or compaction
  boundaries beyond the physical-applied cursor;
- application snapshots retain the `ConfState` established at their physical
  index;
- committed entries move `Applying` on acceptance and `Applied` on advance;
- exact encoded and payload sizes are released;
- committed-entry pagination has no gaps across batches or restart;
- bounded uncommitted growth is released only after application.

### Integration

- a public singleton loop campaigns, persists, becomes leader, commits,
  applies, and becomes idle;
- restart emits committed-but-unapplied entries without replaying hard state;
- restart from snapshot emits only post-snapshot committed entries;
- snapshot restoration precedes later application entries;
- failed outbound snapshot transmission is reported and returns progress to
  probe state;
- simple, explicit-joint, and auto-joint configuration changes flow through
  `Ready`, `ApplyConfChange`, and `Advance`;
- read-index results are delivered once;
- leadership-transfer output flows through `Ready.Messages`;
- messages added after `Ready` acceptance are not dropped by `Advance`.

## Acceptance criteria

D21 is complete when:

1. valid synchronous `RawNode` usage preserves the pinned `Ready`/`Advance`
   lifecycle and output ordering;
2. persistence-dependent self transitions cannot occur before `Advance`;
3. unstable and applying cursors acknowledge only the exact accepted batch;
4. `HasReady` and `MustSync` match the documented matrices;
5. ownership tests prove public input and output detachment;
6. restart, snapshot, pagination, configuration, read-index, and bounded-log
   integration tests pass;
7. async storage use fails explicitly pending D25;
8. all existing tests remain green; and
9. an independent review finds no unresolved correctness issue.
