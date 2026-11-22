# Public API and Host Integration Guide

`dotnet-raft` is a deterministic consensus state machine. It does not open
sockets, start timers, persist a WAL, execute application commands, or decide
when application snapshots are safe.

This is the current host contract for package version 1.0.0. For the design
history and reference-porting milestones, see the [design index](design/README.md).

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

For `IStorage.GetEntries(lowInclusive, highExclusive, maxSize)`, return a
detached, contiguous prefix of the requested half-open range within the total
protobuf encoded-size budget, not the application payload size. A nonempty
readable range must return at least its first entry, even with a zero budget.

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

In synchronous mode, that consumer repeatedly awaits `WaitForReadyAsync`,
processes the batch in the [order above](#synchronous-ready-loop), and calls
`AdvanceAsync`. Campaign, proposal, timers, and network input run independently
of it. Supervise the Ready consumer alongside `node.Completion` and stop the
host on a terminal failure. See the runnable example's
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
