# gRPC Key-Value Cluster Example Design

- **Status:** Implemented
- **Date:** 2026-10-04
- **Scope:** educational runnable example
- **Related decision:** ADR 0004

## Objective

Provide a minimal three-process example that makes every host responsibility
visible while using the production `RaftNode` API unchanged.

## Project layout

```text
examples/DotnetRaft.KvCluster/
  Program.cs
  ClusterOptions.cs
  RaftClusterHost.cs
  IRaftMessageTransport.cs
  GrpcRaftMessageTransport.cs
  RaftTransportService.cs
  KeyValueStateMachine.cs
  PendingOperations.cs
  KvCommandCodec.cs
  Protos/transport.proto
  README.md
```

The project uses `Microsoft.NET.Sdk.Web`, targets `net10.0`, references the
library project, and pins gRPC packages to the repository's existing 2.84.0
tooling line.

## Configuration

Each process receives:

```text
Raft:NodeId
Raft:HttpPort
Raft:GrpcPort
Raft:Peers:<nodeId>
Raft:TickIntervalMilliseconds
Raft:RequestTimeoutSeconds
Raft:TransportTimeoutMilliseconds
Raft:AutomaticTicks
```

`Peers` maps every member ID to its gRPC URI and must contain the local ID.
The MVP requires exactly three peers. IDs, loopback URIs, distinct ports, and
positive timeout values are validated before Kestrel starts.

Kestrel binds loopback endpoints:

- `HttpPort` with HTTP/1 for the JSON API; and
- `GrpcPort` with plaintext HTTP/2 for Raft transport.

## Transport protocol

```proto
syntax = "proto3";

import "google/protobuf/empty.proto";

service RaftTransport {
  rpc Send (RaftEnvelope) returns (google.protobuf.Empty);
}

message RaftEnvelope {
  bytes payload = 1;
}
```

`payload` is `DotnetRaft.Protocol.Message.ToByteArray()`. The receiver parses
with `Message.Parser` and calls `RaftClusterHost.ReceiveAsync`.

Before stepping a message, the receiver requires:

- nonempty payload;
- present and known message type;
- present nonzero `From` and `To`;
- `To` equal to the local node ID;
- `From` present in the configured peer map and different from the local ID;
- a network-legal message type, rejecting local tick, check, unreachable,
  snapshot-status, and storage-thread messages.

Malformed or local-control envelopes return gRPC `InvalidArgument`.
Stopped/faulted nodes return `Unavailable`. Every JSON endpoint uses a shared
endpoint filter that requires
`HttpContext.Connection.LocalPort == HttpPort`. The gRPC service checks
`ServerCallContext.GetHttpContext().Connection.LocalPort == GrpcPort` before
parsing the envelope. Host/authority matching may be added as defense in depth
but is not treated as the listener boundary.

The outbound transport:

- ignores no message silently;
- sends in Ready order;
- uses `TransportTimeoutMilliseconds` as a per-RPC deadline linked to host
  shutdown;
- reports `MsgSnap` success/failure through `ReportSnapshotAsync`;
- reports other send failures through `ReportUnreachableAsync`; and
- logs `Unavailable` and `DeadlineExceeded`, reports them to Raft, and
  continues the Ready sequence;
- treats protocol errors such as `InvalidArgument` as host faults.

## Raft host

`RaftClusterHost` is the sole Ready consumer.

Startup:

1. create `MemoryStorage`;
2. construct `RaftConfig` with `PreVote` and `CheckQuorum`;
3. call `RaftNode.Start` with sorted peers;
4. start one supervised background operation containing the tick loop, Ready
   loop, and `RaftNode.Completion`.

Ready processing is sequential:

```text
publish Ready.Snapshot to storage
append Ready.Entries
store Ready.HardState
perform Ready.MustSync durability barrier
send Ready.Messages in order
restore Ready.Snapshot into the state machine
apply Ready.CommittedEntries in order
complete Ready.ReadStates after application
AdvanceAsync
```

`MemoryStorage` has no atomic transaction or flush API, so atomic publication
and `MustSync` are explicitly documented no-ops in this volatile sample. The
ordering is nevertheless identical to a durable host.

For each committed entry, the state-machine lock is held while required work
is performed and `physicalApplied` is advanced to `entry.Index`:

- empty normal entry: no-op, then advance;
- set command: update the map, then advance and complete its proposal waiter;
- indexes 1-3: require term-one `EntryConfChange` add-node entries matching the
  sorted configured IDs with empty contexts, call `ApplyConfChangeAsync`, then
  advance;
- every later V1/V2 configuration entry: deterministic rejected no-op, then
  advance.

A malformed or mismatched bootstrap prefix faults the host.

The sample has one Ready in flight and uses synchronous storage mode only.
Any Ready-loop, tick-loop, or `RaftNode.Completion` fault cancels the sibling
operations and propagates from `BackgroundService.ExecuteAsync`, causing the
ASP.NET host to stop. Shutdown cancels Ready/network/timer work, awaits the
loops, then stops and disposes the node and transport.

## State machine and client completion

The replicated command is:

```text
Set {
  requestId,
  key,
  value
}
```

It is serialized as deterministic UTF-8 JSON. Applying a set command:

1. updates the dictionary;
2. advances the lock-protected physical applied index; and
3. completes a matching local proposal waiter.

Proposal HTTP requests register their request ID before `ProposeAsync` and wait
until local application or timeout. A timeout removes only the waiter; it does
not cancel an already dispatched proposal.

## Read paths

`GET /kv/{key}` is linearizable:

1. register an immutable 16-byte request context;
2. call `ReadIndexAsync`;
3. wait for the matching `ReadState`;
4. wait under the state-machine barrier until physical application reaches
   `ReadState.Index`;
5. read the key and cursor under the same lock.

`GET /local/{key}` reads immediately and is explicitly documented as
potentially stale.

## HTTP API

```text
PUT  /kv/{key}       body: { "value": "..." }
GET  /kv/{key}       linearizable read
GET  /local/{key}    local potentially stale read
GET  /status         detached Raft status plus applied index
POST /campaign       optional manual campaign trigger
```

Responses label Raft's logical applied index and the state machine's physical
applied index separately.

## Failure behavior

- Malformed replicated commands fault the host because deterministic
  application has diverged.
- A terminal `RaftNode` failure or host-loop failure stops the ASP.NET host.
- Ordinary transport unavailability is reported back to Raft and retried by
  later protocol traffic.
- Proposal/read HTTP timeouts return an explicit timeout response.
- The sample does not claim crash recovery because storage and application
  state are in memory.

## Testing

1. command codec and options validation;
2. state-machine apply, snapshot, and detached reads;
3. pending proposal/read timeout and completion behavior;
4. transport envelope serialization;
5. deterministic three-node integration using an in-memory FIFO transport;
6. gRPC service parsing and delivery;
7. solution Debug/Release tests, formatting, package verification,
   reproducibility, and benchmark smoke.

The integration test disables automatic ticks, drains all bootstrap work,
calls `CampaignAsync` on a selected node, and delivers in-memory messages FIFO.
It proposes through a follower, verifies application on all nodes, and
performs a linearizable read. Wall-clock timeouts are deadlock guards only.
The periodic tick loop is tested separately.

## Non-goals

- persistent WAL or restart;
- snapshot generation or compaction;
- dynamic membership API;
- TLS/authentication;
- Kubernetes or service discovery;
- production backpressure, batching, or retry tuning.

## Validation outcome

- command/options/state/pending/validation component tests;
- deterministic three-node host integration with explicit campaign;
- manual tick-source supervision test;
- Ready-loop fault cleanup test;
- real three-process ASP.NET Core gRPC smoke:
  - node 2 elected leader;
  - a PUT through follower node 1 applied at index 5;
  - a linearizable GET through follower node 3 returned the replicated value
    at physical index 5;
- invalid PUT bodies return HTTP 400;
- full solution Debug and Release suites pass.
