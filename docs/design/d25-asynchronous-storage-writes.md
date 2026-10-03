# D25 Asynchronous Storage Writes Design

- **Status:** Accepted
- **Date:** 2026-10-03
- **DAG node:** D25
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D25 adds the pinned asynchronous local-storage protocol. When
`RaftConfig.AsyncStorageWrites` is enabled, `Ready.Messages` carries ordered
requests for a host-owned append worker and apply worker. Their response
messages replace `Advance` and permit storage/application work to be pipelined
across multiple Ready batches.

Transport remains a host responsibility. These are local, reliable messages,
not network traffic.

## Goals

- Emit `MsgStorageAppend` and `MsgStorageApply` requests with detached work and
  completion responses.
- Allow repeated Ready acceptance without waiting for prior local storage
  completion.
- Keep append and apply pipelines independently ordered.
- Apply only entries already known to be in stable local storage.
- Preserve committed-entry byte budgeting across outstanding apply requests.
- Prevent unstable/stable divergence during overlapping term changes and log
  replacement through the pinned ABA defense.
- Integrate the protocol with `RawNode`, `RaftNode`, tracing, descriptions, and
  the D23 interaction harness.

## Non-goals

- Creating storage threads, queues, WALs, databases, or application workers in
  the library.
- Retrying failed local storage operations.
- Allowing local storage messages to be dropped, reordered within one target,
  duplicated, or synthesized by the host.
- Persisting network messages.
- Changing synchronous `Ready`/`Advance` behavior.

## Public local targets

```csharp
public static class RaftLocalMessageTargets
{
    public const ulong AppendThread = ulong.MaxValue;
    public const ulong ApplyThread = ulong.MaxValue - 1;

    public static bool IsLocal(ulong id);
}
```

The existing internal target classifier delegates to these constants.

Hosts route local requests by `Message.To`:

```text
AppendThread -> MsgStorageAppend
ApplyThread  -> MsgStorageApply
```

Every other target is a network node ID.

## Configuration

`RaftConfig.AsyncStorageWrites = true` becomes supported by `RawNode` and
`RaftNode`.

The mode is fixed for the lifetime of the instance. Restart must use the same
host protocol discipline as a newly constructed async node.

## Ready contract

The ordinary Ready fields remain populated for diagnostics:

- `HardState`;
- `Entries`;
- `Snapshot`; and
- `CommittedEntries`.

In async mode the host must not process those four fields directly. Their work
is duplicated into local messages in `Ready.Messages`.

The host may process immediately:

- network messages;
- local storage requests;
- `ReadStates`; and
- volatile `SoftState`.

`MustSync` retains its D21 meaning for the diagnostic append fields. For the
actual local request, forced durability is required exactly when a
`MsgStorageAppend` has one or more response messages. A response-free
commit-only append request may be atomically published without a forced flush.

`Ready.Messages` has this exact aggregate order:

```text
existing immediate core messages
MsgStorageAppend, when needed
MsgStorageApply, when needed
```

After-append messages appear only inside the append request's `Responses`.

## Ready acceptance and pipelining

Async `Ready()`:

1. snapshots current outputs;
2. creates local storage request messages;
3. updates soft/hard-state baselines;
4. clears core output queues and read states;
5. marks emitted unstable entries/snapshot in progress;
6. marks emitted committed entries applying with
   `allowUnstable = false`; and
7. returns without creating an outstanding Ready/Advance obligation.

The caller may request another Ready immediately. New entries, hard-state
updates, and newly stable committed work can form later local requests while
older requests are still in progress.

`HasReady`:

- does not wait for `Advance`;
- returns committed entries only through the stable prefix
  `min(Committed, Unstable.Offset - 1)`; and
- remains subject to the cumulative outstanding apply-byte limit.

`Advance` and `RaftNode.AdvanceAsync` throw `NotSupportedException` in async
mode before any mutation. Storage response messages are the only
acknowledgement path.

## Append request

A local append request is emitted when any of these exist:

```text
new unstable entries
hard-state update
new unstable snapshot
after-append response messages
```

Its exact shape is:

```text
Type      = MsgStorageAppend
From      = local Raft ID
To        = RaftLocalMessageTargets.AppendThread
Entries   = Ready.Entries
Snapshot  = Ready.Snapshot, when nonempty
Term/Vote/Commit = Ready.HardState fields, only when HardState is emitted
Responses = every after-append message in queue order,
            followed by one MsgStorageAppendResp when required
```

The host reconstructs a nonempty `HardState` only when at least one of
`HasTerm`, `HasVote`, or `HasCommit` is true.

Hard-state presence is all-or-none: when emitted, `Term`, `Vote`, and `Commit`
are all present, including present zero values. When omitted, all three fields
are absent.

The append worker must:

1. process requests to `AppendThread` reliably in Ready order;
2. atomically publish each request's snapshot, entries, and hard state;
3. for a snapshot request, restore the physical application snapshot and its
   `ConfState` under the shared application-state barrier described below;
4. force durability before responses when `Responses.Count > 0`;
5. deliver all responses in their listed order; and
6. never deliver responses after a failed/partial publication or failed
   application snapshot restore.

Requests to the apply worker are independent and may complete before or after
append requests, but the Raft core emits application work only for entries
already acknowledged stable.

### Application-state snapshot barrier

The append and apply workers share one exclusive application-state barrier:

- applying an entry holds the barrier while mutating application state and
  configuration;
- restoring a snapshot holds the same barrier while replacing application
  state, physical-applied index, and configuration;
- an apply request that starts after a newer snapshot restore skips physical
  effects for every entry whose index is at or below the current
  physical-applied index, including configuration application; and
- skipped entries still produce their original apply response so Raft can
  release outstanding encoded-size and payload accounting.

If an older apply request already holds the barrier, it completes before the
snapshot restore. If it starts later, the snapshot supersedes it. Response
delivery may therefore occur after snapshot completion even though the older
physical effects happened before or were skipped.

## Append response and ABA defense

`MsgStorageAppendResp` is generated when the Ready includes a snapshot or the
unstable log contains any new or in-progress entries.

```text
Type = MsgStorageAppendResp
From = AppendThread
To   = local Raft ID
Term = current Raft term when request is generated
```

`Term` is always present, including present zero.

If unstable entries exist, it also carries:

```text
Index   = current last log index
LogTerm = term at that index
```

`Index` and `LogTerm` are either both present or both absent.

If the request carries a snapshot, the response carries the same detached
snapshot.

On response:

- when `response.Term == current Term`, `StableTo(Index, LogTerm)` runs;
- a tuple absent from unstable or whose term no longer matches is ignored;
- when `response.Term < current Term`, entry stability is ignored even if the
  tuple currently matches;
- snapshot completion is accepted regardless of response term because
  snapshot state is committed and term-independent; and
- a hard-state-only append after a term change still attests the current last
  tuple whenever unstable entries remain.

The response-term test plus tuple-term test prevents ABA:

1. term-one index `i` begins appending;
2. term-two index `i` replaces it and begins appending;
3. term-one index `i` appears again in unstable;
4. the first response cannot truncate unstable because its response term is
   stale; and
5. only an ordered current-term append response with a matching tuple can
   stabilize the entry.

Assuming finitely many term changes and reliable ordered append processing, a
current-term hard-state/request response eventually releases stable entries.

Snapshot completion clears only the matching retained unstable snapshot and
acknowledges:

```text
max(snapshot index, current Applied)
```

with zero entry bytes. A response for an older/nonmatching snapshot can still
advance to that physically restored snapshot index, but cannot clear a newer
unstable snapshot.

Before clearing a matching retained snapshot, the core re-installs its exact
`ConfState` into the tracker at the snapshot index. This closes the race in
which an older outstanding configuration apply runs after initial
`MsgSnap` acceptance but before physical snapshot restoration. The completion
order is:

```text
if response snapshot matches retained unstable snapshot:
    restore tracker/configuration from response Snapshot.ConfState
    reset votes and local learner state
    clear that unstable snapshot
advance Applied with max semantics
```

An older/nonmatching snapshot response never reasserts configuration and
therefore cannot overwrite the configuration of a newer retained snapshot.
Configuration re-installation emits `ConfigurationApplied` tracing before the
applied cursor advances. Any failure after response dispatch terminally faults
the node.

## Apply request

When stable committed entries are available:

```text
Type      = MsgStorageApply
From      = local Raft ID
To        = RaftLocalMessageTargets.ApplyThread
Entries   = Ready.CommittedEntries
Responses = one MsgStorageApplyResp carrying the same entries
Term      = present zero
```

The apply worker must:

1. process requests to `ApplyThread` reliably in Ready order;
2. apply every entry in log order;
3. make the deterministic accept/reject decision for each configuration entry;
4. synchronously call `ApplyConfChange` for every accepted change before
   sending the response;
5. update physical application state and snapshot configuration; and
6. deliver the listed response only after the entire request completes.

On `MsgStorageApplyResp`, the core:

- advances `Applied` through
  `max(final response entry index, current Applied)`;
- releases the exact encoded-size budget represented by all response entries;
- reduces uncommitted payload accounting by those entries; and
- runs auto-leave checks through the normal `AppliedTo` path.

This permits an older apply response to arrive after a newer snapshot response
without regressing `Applied`, while still releasing that request's exact
accounting.

`MsgStorageApplyResp.Term` is also present zero. The response contains a
nonempty contiguous entry list, and `Index`/`LogTerm`, hard-state fields,
snapshot, and nested responses are absent.

In async mode `BasicStatus.Applied` is a physical-completion cursor because an
apply response is sent only after physical application or deterministic
snapshot-supersession under the barrier. Duplicate or same-target out-of-order
responses are invalid host behavior.

## Ordering between append and apply

Messages to each local target are FIFO and reliable. Messages to different
targets may run independently except for the shared application-state
snapshot barrier.

The core enforces the dependency:

```text
committed entry is emitted to ApplyThread
only after its index is below Unstable.Offset
```

The host does not need to coordinate the two worker queues beyond preserving
each queue's own order and delivering responses.

## RawNode message filtering

In async mode `RawNode.Step` accepts exactly:

```text
MsgStorageAppendResp from AppendThread to local ID
MsgStorageApplyResp  from ApplyThread  to local ID
```

It rejects:

- storage requests stepped back into Raft;
- mismatched local sender/type pairs;
- local responses addressed to another node;
- every reserved sender in synchronous mode; and
- classifier-local messages from ordinary network senders.

Unknown-peer response filtering does not apply to validated local responses.
All validation occurs before core term handling.

Validation also requires:

- append response `HasTerm == true`;
- append response `HasIndex == HasLogTerm`, with nonzero index when present;
- apply response `HasTerm == true && Term == 0`;
- apply response entries are nonempty, contiguous, and non-null; and
- generated response-only fields not listed above are absent.

Malformed local responses fail before dispatch and do not fault the facade.

Local request messages and every nested response are mutually detached from
the diagnostic Ready fields and from one another. They are nevertheless
mutable protobuf outputs. The host must deliver the exact library-generated
response objects or unchanged clones; changing term, tuple, snapshot, entries,
sender, target, type, or order is invalid protocol usage. Async mode retains no
hidden acknowledgement shadow from which mutated responses can be repaired.

`RaftNode.StepAsync` mirrors the same rules. Valid storage responses use the
ordinary command lane, not the proposal lane.

After local-response validation, dispatch is a terminal mutation boundary.
Any exception from core dispatch faults `RawNode` and `RaftNode`, invalidating
all accepted local work for that instance. Validation failures before dispatch
remain ordinary caller errors and do not fault.

## Core storage-response handling

`RaftCore.Step` adds role-independent handlers:

```text
MsgStorageAppendResp:
    stabilize matching entries when response term is current
    complete matching snapshot regardless of response term

MsgStorageApplyResp:
    acknowledge final entry and exact response-entry accounting
```

For a lower-term append response, existing lower-term leader/vote behavior is
not run. Only the snapshot completion is accepted.

Every received local response remains visible through D22
`MessageReceived` tracing.

Synthetic `MsgStorageAppend` and `MsgStorageApply` requests emit
`MessageSent` trace events in the same order they are appended to
`Ready.Messages`, before `ReadyAccepted`.

Both synthetic `MessageSent` callbacks run before any Ready acceptance
mutation: soft/hard baselines, output queues, unstable/applying in-progress
cursors, and read states are still unchanged. If either callback throws:

- no Ready is returned;
- no output/cursor is accepted or drained;
- `RawNode` enters terminal trace fault; and
- the host discards the instance without processing any constructed local
  message.

After successful synthetic traces, acceptance mutates all baselines/queues and
in-progress cursors atomically from the facade's perspective. `ReadyAccepted`
then runs. If that final callback throws, acceptance has occurred but no Ready
is returned; the facade faults and must be reconstructed from the actual
durable/application generation, exactly as in D22.

## Concurrent Node behavior

`RaftNode` records the mode at construction.

In async mode:

- `WaitForReadyAsync` still has one pending waiter at a time, but a returned
  Ready is not stored as outstanding;
- another waiter may be registered immediately;
- `AdvanceAsync` returns `NotSupportedException`;
- stop has no outstanding Ready to invalidate; and
- storage responses are serialized with every other command.

The host can therefore continuously drain Ready while append/apply workers
pipeline local work.

## Interaction harness

D25 extends `InteractionNode` with FIFO append/apply work queues.

`ProcessReady` in async mode:

- prints the Ready;
- routes `MsgStorageAppend` to append work;
- routes `MsgStorageApply` to apply work;
- queues other messages for network/local response delivery; and
- does not persist fields directly or call `Advance`.

`process-append-thread` handles one request:

1. removes its responses for the `Processing:` description;
2. atomically persists snapshot, entries, and reconstructed hard state;
3. emits `Responses:` in order; and
4. queues detached responses.

For a snapshot request it also replaces `InteractionNode`'s physical
application snapshot before queueing responses.

`process-apply-thread` handles one request:

1. removes responses for description;
2. applies all entries using the D23 application state machine and
   configuration rules;
3. emits `Responses:` in order; and
4. queues detached responses.

`Stabilize` adds append-thread and apply-thread phases after Ready and message
delivery, matching pinned order:

1. process at most one Ready per selected node;
2. deliver queued messages to selected recipients;
3. for each selected node in order, if append work exists, print one
   `processing append thread` header and drain that node's entire append queue
   FIFO beneath it;
4. for each selected node in order, if apply work exists, print one
   `processing apply thread` header and drain that node's entire apply queue
   FIFO beneath it; and
5. repeat to a fixed point.

Unselected node work remains queued. Each Ready, delivered/dropped/rejected
message, append request, and apply request consumes one stabilization work
item.

Direct empty-worker commands return exactly:

```text
no append work to perform
no apply work to perform
```

with no trailing newline before command normalization.

The harness applies the shared snapshot barrier deterministically: restoring a
snapshot updates physical application state; later older apply requests skip
entries at or below that index but still queue their responses.

`async-storage-writes=true` becomes valid in `add-nodes`.

## Ownership and failures

- Local request messages, entries, snapshots, and nested responses are fully
  detached from core state and mutually detached as described above.
- A storage failure produces no responses. The host must recover from its
  actual durable/application generation.
- An `ApplyConfChange` failure faults the node; the apply response must not be
  sent.
- The library does not catch worker/storage exceptions or turn them into
  successful acknowledgements.

### Instance epochs and recovery

Every local request and nested response belongs to the exact `RawNode`/
`RaftNode` instance epoch that emitted it.

After crash, stop, or terminal fault, the host must:

1. stop both old workers;
2. discard every queued/in-progress request and undelivered response from that
   epoch;
3. recover the actual durable FIFO append prefix;
4. recover the actual physical application prefix and matching `ConfState`;
5. enforce D21's `HardState.Commit >= physicalApplied` repair/rollback rule;
   and
6. construct a fresh node, which re-emits any work not represented by those
   recovered prefixes.

An old response must never be stepped into a reconstructed node even when its
sender, term, index, and entry tuple happen to look valid.

## Test plan

### RawNode message construction

- vote persistence creates an append request whose response is the self vote;
- leader append responses precede `MsgStorageAppendResp`;
- hard-state fields preserve proto2 presence;
- snapshot, entries, and nested responses are detached;
- response-free commit-only hard state does not require forced durability;
- apply requests carry exact paginated committed entries; and
- async Ready can be accepted repeatedly without Advance.

### Response handling

- append response stabilizes only a current-term matching tuple;
- term mismatch and missing tuple are ignored;
- lower response term ignores entries but acknowledges snapshot;
- a matching snapshot response clears it, a nonmatching old response leaves a
  newer unstable snapshot intact, and both use max-applied semantics;
- matching snapshot completion reasserts its exact configuration after any
  older outstanding configuration apply;
- an older apply response after newer snapshot completion releases exact
  accounting without regressing `Applied`;
- hard-state-only current-term response eventually releases in-progress
  unstable entries;
- apply response advances exact accounting and unblocks pagination;
- invalid sender/type/target combinations are rejected before term handling;
  and
- synchronous mode still rejects reserved senders.

### Pipelining and ABA

- depth-two append pipelines expose only newly available entries;
- apply work never passes stable local entries;
- append and apply queues can complete in either cross-queue order;
- same-index term replacement preserves unstable visibility;
- the term-one/term-two/term-one ABA sequence does not truncate early; and
- final current-term response converges unstable to storage.

### Concurrent Node

- async Ready delivery never requires or permits Advance;
- successive waiters receive pipelined batches;
- valid local responses are serialized and invalid local messages fail;
- stop/fault/cancellation behavior remains unchanged; and
- sync-mode D24 tests remain unchanged.

### Interaction and corpus

- append/apply processing is FIFO and response-ordered;
- dropped local messages remain impossible;
- storage failure sends no response;
- configuration application precedes apply response;
- stabilization reaches a fixed point with both workers;
- focused C# golden scenarios cover async pipelining and ABA; and
- the two pinned `async_storage_writes*.txt` command flows are ported or
  equivalently covered with exact deterministic outputs.

## Acceptance criteria

D25 is complete when:

1. async Ready emits correct local request/response messages;
2. no caller uses `Advance` in async mode;
3. stable/applying cursors advance only from valid worker responses;
4. ABA and pipeline tests pass;
5. synchronous D21-D24 behavior remains unchanged;
6. focused interaction and full tests pass;
7. formatting verification passes;
8. an independent GPT-5.6 Sol review approves the design and implementation;
   and
9. the completed node is committed and pushed before D26 begins.
