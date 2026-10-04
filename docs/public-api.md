# Public API and Host Integration Guide

`dotnet-raft` is a deterministic consensus state machine. It does not open
sockets, start timers, persist a WAL, execute application commands, or decide
when application snapshots are safe.

This guide describes the host contract for package version 1.0.0.

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

Install:

```bash
dotnet add package DotnetRaft.Sqlite --version 1.0.0
```

Open one local database:

```csharp
using DotnetRaft.Storage.Sqlite;

using var storage =
    new SqliteStorage("data/raft.db");
```

The storage uses SQLite WAL mode, exclusive locking, and
`synchronous=FULL`. It atomically persists each synchronous Ready:

```csharp
storage.PersistReady(ready);
```

For asynchronous storage writes:

```csharp
storage.PersistStorageAppend(appendRequest);
```

Do not deliver `appendRequest.Responses` until persistence succeeds.

### Pending application snapshots

Persisting an incoming snapshot and restoring application state cannot be one
transaction when application state is stored elsewhere. SQLite storage
therefore records a durable pending marker.

Before constructing a restarted node:

```csharp
Snapshot? pending =
    storage.GetPendingApplicationSnapshot();
if (pending is not null)
{
    RestoreApplicationSnapshotAtomically(
        pending.Data,
        pending.Metadata.Index,
        pending.Metadata.ConfState);

    HardState hardState =
        storage.GetHardState()
        ?? throw new InvalidDataException(
            "A pending non-bootstrap snapshot requires durable HardState.");
    if (hardState.Term
        < pending.Metadata.Term)
    {
        throw new InvalidDataException(
            "Recovered HardState term is older than the pending snapshot.");
    }

    if (hardState.Commit
        < pending.Metadata.Index)
    {
        hardState.Commit =
            pending.Metadata.Index;
        storage.SetHardState(hardState);
    }

    storage.AcknowledgeApplicationSnapshot(
        pending.Metadata.Index);
}
```

`GetInitialState` rejects startup while the marker remains. Snapshot
acknowledgement also requires durable `HardState.Commit` to cover the snapshot.
Repeating snapshot restore after a crash must be safe and idempotent.
Term and vote cannot be synthesized from snapshot metadata; missing or
regressed durable term is a terminal recovery failure.

The database, `-wal`, and `-shm` files must remain together on one local
filesystem. Network filesystems, hard-link aliases, multiple owners, and
copying only the main database file while it is live are unsupported.

SQLite persists only Raft consensus state. Application bytes,
`physicalApplied`, proposal deduplication, and any application database remain
the host's responsibility.

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

Node IDs are nonzero and must not be reused for a different member.
`ElectionTick` must exceed `HeartbeatTick`.

Enable `CheckQuorum` before selecting lease-based reads. `PreVote` avoids
disruptive term increases by isolated nodes. `MaxCommittedSizePerReady` is a
cumulative outstanding-application byte budget, including asynchronous apply
requests.

## `RawNode`

`RawNode` is synchronous and thread-unsafe. Call every method from one host
event loop.

Create a new cluster:

```csharp
RawNode raw = RawNode.Start(
    config,
    peers);
```

Restart from recovered storage:

```csharp
RawNode raw = RawNode.Restart(config);
```

Before restart:

```text
RaftConfig.Applied = physically recovered application index
HardState.Commit >= RaftConfig.Applied
snapshot/application ConfState = membership at that physical index
```

If a non-forced commit update was lost while later application state survived,
roll application state back or durably repair commit before construction.

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

```csharp
await using RaftNode node = RaftNode.Restart(config);

await node.CampaignAsync(cancellationToken);
await node.ProposeAsync(commandBytes, cancellationToken);

Ready ready = await node.WaitForReadyAsync(cancellationToken);
// Host processing.
await node.AdvanceAsync(cancellationToken);
```

Cancellation prevents dispatch only while a request is waiting. Once the
owner loop claims it, the operation runs exactly once and its result wins.

Proposals remain pending while no leader is known. Stop unblocks every waiter.
`Completion` succeeds after normal stop and faults after a terminal owner-loop
failure. `TerminalStatus` is diagnostic only and may describe partial
in-memory state.

Only one Ready waiter/consumer is supported.

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

Record the returned `ConfState` in application snapshots.

An exception after configuration dispatch faults that node instance. Discard
it and reconstruct from the prior complete durable/application generation.

## Asynchronous storage writes

With `AsyncStorageWrites = true`, do not process the diagnostic
`Ready.HardState`, `Entries`, `Snapshot`, or `CommittedEntries` fields and do
not call `Advance`.

Route messages by target:

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

Append and apply queues are independently FIFO. Awaited writes apply
backpressure when a bounded queue is full and surface a completed or faulted
channel. Treat such failure as a node/host failure; never discard a local
storage request by ignoring a false `TryWrite` result.

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

### Apply worker

`MsgStorageApply` carries committed entries. Apply them in order, including
the configuration rules above, then deliver its response.

Do not mutate library-generated response messages. Responses are the
acknowledgement protocol and are scoped to the exact node instance that emitted
them.

After crash, stop, or fault, discard old worker queues/responses before
constructing a new node.

## Network transport

Send every nonlocal Ready message to `Message.To`. Clone or serialize it
before sharing across threads.

Authenticate the connection-level peer and require that identity to match
`Message.From`. Validate that `Message.To` is the exact local node and restrict
the listener to authorized cluster peers. `Step` and `StepAsync` are
consensus-message facades, not authentication or routing trust boundaries.

Pass the validated message to `Step`/`StepAsync`. The library rejects
host-independent malformed message structure before core mutation. Unknown
response senders are filtered, and classifier-local messages are not network
protocol messages. An `ArgumentException` rejects only that operation;
internal invariant failures remain terminal.

After sending a snapshot, call `ReportSnapshot` with success or failure. A
failure resumes probing; failing to report can leave follower progress paused.

The runnable
[`DotnetRaft.KvCluster`](../examples/DotnetRaft.KvCluster/README.md) example
shows this boundary with an ASP.NET Core gRPC bytes-envelope transport. It is
an unauthenticated, loopback-only educational host, not a production
deployment template.

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

Call `ReadIndex`/`ReadIndexAsync` with an immutable request context. Match the
returned `ReadState.RequestContext` to the request.

Execute the read only after physical state reaches the returned index.

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
