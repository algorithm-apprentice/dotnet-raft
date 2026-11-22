# Public API and Host Integration Guide

`dotnet-raft` is a deterministic consensus state machine. The core does not
open sockets, start timers, publish durable writes, execute application
commands, or choose snapshot and compaction policies. It does read the
host-provided stable log synchronously through `IStorage`.

This is the current host contract for package version 1.0.0. For the design
history and reference-porting milestones, see the [design index](design/README.md).
For code you can run, see the [examples](../examples/README.md).

## Architecture boundary

The library owns:

- Raft roles, terms, votes, quorum decisions, and leader election;
- replicated-log matching, commitment, and follower progress;
- snapshots and membership state inside the consensus algorithm;
- safe/lease read-index coordination;
- leadership transfer and availability extensions;
- deterministic status, descriptions, and tracing; and
- synchronous and concurrent integration facades.

The host owns:

- a durable implementation of `IStorage`;
- network transport for nonlocal `Message` values;
- periodic logical ticks;
- the replicated application state machine;
- configuration-entry acceptance or rejection;
- physical application progress and read barriers;
- snapshot creation, durable publication, and compaction;
- retry and client request identity; and
- crash recovery.

`DotnetRaft.Sqlite` is an optional package implementing durable consensus
storage. It does not persist the application state machine.

## Durable SQLite storage

See the [installation instructions](../README.md#install) and the
[SQLite package guide](../src/DotnetRaft.Sqlite/README.md) for its atomic write
APIs, crash recovery with pending application snapshots, and supported backup
procedure. Restoring Raft state alone cannot recover a separate application
store.

## Storage reads

[`IStorage`](../src/DotnetRaft/Storage/IStorage.cs) is a synchronous read
interface, not the write side of the Ready protocol. Reads run on the node's
serialized call path and may touch disk. A backend used with separate storage
workers must synchronize reads with publication and must not expose partial
batches. Return detached mutable protobufs so callers cannot modify storage.

| Method | Contract |
|---|---|
| `GetInitialState()` | Return recovered `HardState` (nullable for a fresh store) and non-null `ConfState`; see [Restart](#restart). |
| `GetFirstIndex()` | First index after the retained boundary, even if no entry remains there. Fresh storage starts at 1; the term at `FirstIndex - 1` remains readable. |
| `GetLastIndex()` | Last retained log position, including the boundary when no entries remain after it. Require `1 <= FirstIndex <= LastIndex + 1` and a representable successor for `LastIndex`. |
| `GetTerm(index)` | Read the term in `[FirstIndex - 1, LastIndex]`, including the boundary. Throw `StorageException` with `Compacted` below it or `Unavailable` above the last index. |
| `GetEntries(lowInclusive, highExclusive, maxSize)` | Return a contiguous prefix of `[lowInclusive, highExclusive)`. Include the first entry of a nonempty readable range even if it exceeds `maxSize`, including zero. Stop before any subsequent entry would put the total protobuf encoded size (the sum of `Entry.CalculateSize()`) over the budget. |
| `GetSnapshot()` | Return the retained snapshot with its metadata and configuration, or an empty snapshot for an uninitialized store. If snapshot generation is temporarily in progress, throw `StorageException(StorageError.SnapshotTemporarilyUnavailable, ...)` so replication can retry later. |

`GetEntries` uses `Compacted` when the requested low index is below the
current first index, and rejects reversed ranges or ranges beyond
`LastIndex + 1`. It must not return an empty success for a nonempty retained
range. `MemoryStorage` and `SqliteStorage` return an empty list for a valid
empty range when real entries are retained, and `Unavailable` when only the
boundary remains. During replication `Compacted` can lead to snapshot
catch-up; `Unavailable` for an expected stable entry is not a normal empty page.

## Configuration

At minimum:

```csharp
var config = new RaftConfig
{
    Id = nodeId,
    ElectionTick = 10,
    HeartbeatTick = 1,
    Storage = storage,
};
```

Node IDs are nonzero, cannot be the IDs reserved by `RaftLocalMessageTargets`,
and must not be reused for a different member. Tick values are logical:
`ElectionTick > HeartbeatTick > 0`. The host calls `Tick` on its chosen
cadence. `RawNode.Tick` runs synchronously; `RaftNode.Tick` enqueues a tick
without waiting and can drop ticks when its bounded queue is full.

Enable `CheckQuorum` before selecting lease-based reads. `PreVote` avoids
disruptive term increases by isolated nodes. `MaxCommittedSizePerReady` is a
cumulative encoded-byte budget for outstanding application work, including
asynchronous apply requests. Zero defaults to `MaxSizePerMessage`; the first
entry of a nonempty page may exceed the available budget to allow progress.

## `RawNode`

`RawNode` is synchronous and thread-unsafe. Call every method from one host
event loop.

To start a brand-new cluster, use empty storage and the same ordered initial
peer list on each starting voter:

```csharp
RawNode raw = RawNode.Start(
    config,
    peers);
```

For `RaftNode`, use `RaftNode.Start(config, peers)`. `Start` does not finish
application bootstrap: have the Ready consumer publish, apply, and
acknowledge every bootstrap page using the configured storage protocol
before accepting client traffic or inbound peer messages, or initiating
campaigns. This does not delay local storage completions. Do not require a
live quorum before draining these local initialization entries.

### Restart

`Start` bootstraps membership; it is not a recovery API. After reconciling
persisted state, give `RawNode.Restart` or `RaftNode.Restart` the recovered
storage and `RaftConfig.Applied`:

```csharp
RawNode raw = RawNode.Restart(config);
```

Before calling either restart method, recover the application's bytes and
their actual physical applied index. If that index is behind the compacted
boundary, restore a snapshot covering the missing prefix; the node cannot
replay compacted entries. With SQLite,
[reconcile any pending application snapshot](../src/DotnetRaft.Sqlite/README.md#open-and-restart)
first, even if the application already restored its bytes before the crash.

Require a coherent recovery point:

```text
RaftConfig.Applied = physically recovered application index
FirstIndex - 1 <= RaftConfig.Applied <= effective commit <= LastIndex
IStorage.GetInitialState().ConfState = membership at RaftConfig.Applied
```

Use the recovered `HardState.Commit` when hard state is present; a fresh
empty store starts at zero. Do not use `Applied = 0` to request replay below
the compacted boundary: the core initializes its cursors to `FirstIndex - 1`
before considering a nonzero `Applied` value. If a non-forced commit update
was lost while later application state survived, durably repair only `Commit`
before constructing the node, using trusted application state backed by
retained durable log or snapshot data. Otherwise roll the application back
to a compatible retained prefix or fail startup. Never derive `Term` or
`Vote` from an application cursor or snapshot.

`Applied` suppresses replay through that index; it does **not** load
membership from a separate application database. Both built-in stores return
the retained snapshot's `ConfState` from `GetInitialState`. If application
state is ahead of that snapshot and includes a later accepted membership
change, publish a matching snapshot from the physically recovered application
state, use a storage implementation that supplies the matching initial
configuration, or recover an earlier compatible application state and replay.
Do not simply advance `Applied` past the change with the old configuration.

### Synchronous Ready loop

For `AsyncStorageWrites == false`:

1. wait until `HasReady`;
2. call `Ready`;
3. atomically publish snapshot, entries, and hard state;
4. force durability when `Ready.MustSync` is true;
5. send `Ready.Messages` only after required persistence;
6. restore the snapshot and apply committed entries in log order;
7. apply every accepted configuration entry before advancement;
8. admit every `ReadState` to the same ordered application pipeline; and
9. call `Advance` with the exact returned `Ready`.

Snapshot presence does not participate in `Ready.MustSync`. Complete snapshot
persistence according to the storage implementation's durability contract
before dependent messages or `Advance`; do not infer a separate generic flush
solely because `Ready.Snapshot` is present.

Only one Ready can be outstanding. Inputs can continue accumulating while it
is outstanding; newer work appears after advancement.

An incoming snapshot can be accepted while the host is still applying
configuration entries from an earlier Ready. Those changes still apply at
their own log positions and return the membership of that earlier application
prefix; for example, an earlier joint exit remains valid even when the new
snapshot has already left joint consensus. The node uses the snapshot's
membership for consensus and preserves it when confirming the snapshot.
Confirmation of an older snapshot cannot restore its configuration over a
newer one. The host still applies the earlier Ready in order; it does not need
to pause inbound messages around configuration apply.

`Advance` may occur after irrevocable ordered admission rather than slow
physical completion, but the host must separately maintain `physicalApplied`.
A linearizable read can execute only when:

```text
snapshot restoration complete
&& physicalApplied >= ReadState.Index
```

Snapshots and compaction use physical, not logical, progress.

## `RaftNode`

`RaftNode` owns a single background loop and is safe for concurrent callers.
That loop serializes node commands; it does not consume Ready on the host's
behalf. Start one independently running Ready consumer before awaiting any
proposal. With no known leader, `ProposeAsync` waits, and the election itself
may need the consumer to send its outgoing vote requests.

With the default Ready/Advance storage protocol, that consumer repeatedly
awaits `WaitForReadyAsync`, processes the batch in the
[order above](#synchronous-ready-loop), and calls `AdvanceAsync`. Campaign,
proposal, timers, and network input run independently of it. Supervise the
Ready consumer alongside `node.Completion` and stop the host on a terminal
failure. See the runnable example's
[`RaftClusterHost`](../examples/DotnetRaft.KvCluster/RaftClusterHost.cs) for
the consumer and shutdown loop.

Cancellation prevents dispatch only while a request is waiting. Once the
owner loop claims it, the operation runs exactly once and its result wins.
On submission, an already stopping, stopped, or faulted node reports that
state before honoring an already-canceled token.

Stop unblocks every waiter. `Completion` succeeds after normal stop and
faults after a terminal owner-loop failure. `TerminalStatus` is diagnostic
only and may describe partial in-memory state.

Only one Ready waiter/consumer is supported.

### Call completion and client results

| Milestone | What it means |
|---|---|
| `Propose` or `await ProposeAsync` returns | The node processed the submission. It may only have forwarded it; this is not a commit, application, or client acknowledgement. A dropped proposal can raise `ProposalDroppedException`. |
| `Ready.CommittedEntries` (default) or `MsgStorageApply.Entries` (async storage) delivers a command | Raft chose the entry for ordered application. The host still has to execute it. |
| The host physically applies the command | The result exists locally. Respond to the matching client only when the application's required durability and retry rules are satisfied. |
| `ReadIndex` or `await ReadIndexAsync` returns | This submits a read barrier request; it does not return an index or guarantee a response. Follow the [read completion rules](#read-index). |

Cancellation of an API call after dispatch is not cancellation of an already
proposed command or of a pending protocol read. The host owns request
deadlines, correlation, and retry identity.

## Configuration entries

For each committed `EntryConfChange` or `EntryConfChangeV2`, make a
deterministic application-level decision at that log position:

- **accepted:** call the matching `ApplyConfChange`/`ApplyConfChangeAsync`
  synchronously before acknowledgement;
- **rejected:** treat it as an application no-op and do not call the apply
  method.

The decision must depend only on replicated application state, never local
health, timing, or transport observations.

Bootstrap-generated configuration entries are initialization records and
cannot be rejected.

For ordinary accepted changes, record the returned `ConfState` with the
application index at which the change was applied. Snapshots must carry the
configuration at their exact index. **Bootstrap exception:** `Start` installs
the entire initial voter set before emitting the generated add-node entries.
If application effects are persisted entry by entry, `ApplyConfChange` can
return the full set even at the first bootstrap index. Persist the membership
of the actual ordered bootstrap prefix, or recover the bootstrap as a whole;
do not label a partial bootstrap snapshot with the full voter set. The
[KV bootstrap example](design/durable-grpc-kv-host.md#configuration-and-no-op-entries)
shows how to record the prefix.

An exception after configuration dispatch faults that node instance. Discard
it and reconstruct from the prior complete durable/application generation.

## Asynchronous storage writes

With `AsyncStorageWrites = true`, do not process the diagnostic
`Ready.HardState`, `Entries`, `Snapshot`, or `CommittedEntries` fields and do
not call `Advance` or `AdvanceAsync`. Consume `Ready.ReadStates` in this mode
as well; read barriers still depend on physical application.

Route messages by target. Each local worker queue must be FIFO and lossless.
For bounded channels set `FullMode = BoundedChannelFullMode.Wait`. Do not
use `DropWrite`, `DropOldest`, or `DropNewest`: `WriteAsync` can succeed in
those modes even though a request was discarded.

```csharp
foreach (Message message in ready.Messages)
{
    switch (message.To)
    {
        case RaftLocalMessageTargets.AppendThread:
            await appendQueue.Writer.WriteAsync(message);
            break;
        case RaftLocalMessageTargets.ApplyThread:
            await applyQueue.Writer.WriteAsync(message);
            break;
        default:
            await transport.SendAsync(message);
            break;
    }
}
```

With wait-mode channels, awaited writes apply backpressure when a queue is
full and surface a completed or faulted channel. Treat such failure as a
node/host failure; never discard a local storage request by ignoring a false
`TryWrite` result.

### Append worker

`MsgStorageAppend` carries entries, optional snapshot, and optional hard state
encoded in its term/vote/commit fields.

Atomically publish snapshot, entries, and hard state. If responses are
present, force durability before delivering them in order. The response list,
not snapshot presence by itself, defines the asynchronous forced-durability
decision.

A snapshot-bearing request also restores physical application state and
configuration under the same exclusive application-state barrier used by the
apply worker. Older apply work starting afterward skips entries covered by the
snapshot but still returns its response to release accounting.

After persistence and any required snapshot restoration or storage-specific
acknowledgement, deliver the responses in their listed order. An append request
can contain both a local confirmation and responses to remote peers such as
`MsgVoteResp` or `MsgAppResp`. Route by each response's `To`, not by the
request's target or the response's type:

```csharp
foreach (Message response in appendRequest.Responses)
{
    if (response.To == localNodeId)
    {
        await node.StepAsync(response, cancellationToken);
    }
    else
    {
        await transport.SendAsync(response, cancellationToken);
    }
}
```

For `RawNode`, deliver local responses with `Step` on its serialized owner.
The [counter demo](../examples/DotnetRaft.Counter/README.md#asynchronous-storage-writes)
shows both worker queues and routes nested responses in a three-node run.
The [SQLite integration test](../tests/DotnetRaft.Sqlite.Tests/SqliteStorageRawNodeTests.cs)
exercises a durable append with both destinations.
The [same membership rule as synchronous mode](#synchronous-ready-loop)
applies when older apply work overlaps an incoming snapshot. A completion for
a superseded snapshot does not reinstall its configuration.

### Apply worker

`MsgStorageApply` carries committed entries. Apply them in order, including
the configuration rules above, then deliver its response.

Do not mutate library-generated response messages. Local responses are the
acknowledgement protocol and are scoped to the exact node instance that emitted
them.

After crash, stop, or fault, discard old worker queues/responses before
constructing a new node.

## Network transport

Send every nonlocal Ready message to `Message.To`. Clone or serialize it
before sharing across threads.

Authenticate the connection-level peer, restrict the listener to authorized
cluster peers, and validate that `Message.To` is the exact local node. For
messages sent directly or forwarded once by the originating node, require the
connection identity to match `Message.From`. `Step` and `StepAsync` do not
authenticate peers or check routing for you.

An old leader that has stepped down may forward a `MsgReadIndex` again,
keeping the original requester in `From`. The core can also preserve the
original `From` when re-forwarding `MsgProp` through `RawNode`. A strict
one-hop policy can drop such a message and let the originating host retry.
Do not rewrite `MsgReadIndex.From` to the immediate peer: it is the reply
address. If the transport supports re-forwarding, explicitly authorize the
authenticated forwarder to carry an original requester's identity.

Pass the validated message to `Step`/`StepAsync`. The library rejects known
malformed message shapes before core mutation. For ordinary network traffic,
`RawNode.Step` rejects responses from unknown peers and local-only message
types; `RaftNode.StepAsync` ignores them. Neither behavior is a substitute
for the transport allowlist. A structural-validation `ArgumentException`
rejects that operation without faulting the node.

After sending a snapshot, call `ReportSnapshot` or `ReportSnapshotAsync` with
the transport result. If the peer is still in snapshot mode, both outcomes
return it to paused probe mode; a subsequent heartbeat response can trigger
another attempt. Transport success is not proof that the receiver applied
the snapshot. Failing to report a transfer can leave follower progress
paused in snapshot mode.

The runnable
[`DotnetRaft.KvCluster`](../examples/DotnetRaft.KvCluster/README.md) example
shows this boundary with an ASP.NET Core gRPC bytes-envelope transport. It is
an unauthenticated, loopback-only educational host, not a production
deployment template.

### Leadership transfer

Treat leadership transfer as an authorized administrative operation. Prefer
calling `TransferLeader` or `TransferLeadershipAsync` on the current leader
through the host's management path. The pinned wire encoding uses
`MsgTransferLeader.From` for the **requested transferee**, not necessarily
the sender. A follower can forward that message while retaining `From`. A
transport that supports such forwarding needs a separately authenticated and
authorized control path; do not weaken ordinary sender checks or treat the
claimed transferee as the caller's identity. If the host only permits
leader-local transfer, reject all inbound `MsgTransferLeader` on the peer
listener and invoke the transfer method only on a node that is the leader.

## Snapshots and compaction

An application snapshot contains:

- application bytes through its physical index;
- the term at that index; and
- the exact `ConfState` established at that index.

Required ordering:

```text
snapshot index <= physicalApplied
compaction index <= durably published application snapshot index
```

Never label a snapshot with a logical/admitted cursor while physical state is
behind.

## Read index

Register a fresh, immutable context for **every read attempt** before calling
`ReadIndex` or `ReadIndexAsync`. For example, encode a fresh `Guid.NewGuid()`
as bytes. Do not reuse a context after success, a timeout, or restart: a
delayed reply could otherwise match the wrong request. A successful call
does not return a barrier; the index comes later in `Ready.ReadStates`.
Match `ReadState.RequestContext` to the pending attempt and wait for
`physicalApplied >= ReadState.Index` after any required snapshot restore.
Read the value and check the application index under the application's own
synchronization or transactional read barrier.

A follower without a known leader can ignore the submission, and leadership
changes can interrupt confirmation. Bound the wait in the host and remove
stale waiters; the API cancellation token does not remove an already
dispatched protocol request. Do not substitute a stale local value for a
timed-out linearizable read.

Every leader waits until it has committed an entry in its own term before
answering, even if it is the sole voter. A sole-voter leader then needs no
heartbeat quorum round, but the application's physical read barrier still
applies.

Lease-based reads additionally depend on the host's bounded-clock-drift
assumption.

## Status, descriptions, and tracing

`GetBasicStatus`, `GetStatus`, and `VisitProgress` return detached snapshots.
`BasicStatus.Applied` is logical in synchronous early-admission mode and
physical-completed in asynchronous storage mode.

`RaftDescriptions` produces deterministic invariant-culture text.

Set `RaftConfig.TraceSink` for detached structured events. Trace callbacks are
synchronous, must not re-enter the same node, and fault the node when they
throw.

## Fault handling

Do not retry a partially dispatched operation on the same faulted instance.
Recover from:

- the actual durable append prefix;
- the actual physical application prefix;
- the matching `ConfState`; and
- a commit index repaired to at least physical application.

Then create a new instance and discard every old local response.
