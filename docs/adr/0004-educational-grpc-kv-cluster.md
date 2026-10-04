# ADR 0004: Educational gRPC Key-Value Cluster

- **Status:** Accepted
- **Date:** 2026-10-04
- **Decision owners:** dotnet-raft maintainers

## Context

The library and its public integration contract are complete, but the
repository does not contain a runnable multi-process example showing how a
host combines:

- `RaftNode`;
- logical ticks;
- Ready persistence and advancement;
- network transport;
- configuration application;
- a replicated state machine; and
- read-index barriers.

The example must teach the boundary between consensus and transport without
being mistaken for a production database.

## Decision

Add `examples/DotnetRaft.KvCluster`, a fixed-membership three-node sample with
these boundaries:

1. **Consensus**
   - one synchronous `RaftNode` per process;
   - `MemoryStorage`;
   - bootstrap peers supplied by configuration;
   - `PreVote` and `CheckQuorum` enabled.
2. **Transport**
   - ASP.NET Core hosts the process;
   - a plaintext HTTP/2 gRPC endpoint carries serialized canonical
     `DotnetRaft.Protocol.Message` bytes;
   - the transport proto defines only an envelope and does not duplicate
     `raft.proto`;
   - sends preserve Ready order;
   - every send has a bounded transport deadline;
   - transport failures report peers unreachable;
   - snapshot sends report success or failure.
3. **Application**
   - deterministic JSON-encoded set commands;
   - an in-memory key-value state machine;
   - every committed entry, including no-ops and configuration entries,
     advances one lock-protected physical-applied cursor;
   - proposal request IDs complete only after local committed application;
   - linearizable GET uses `ReadIndexAsync` and waits for the matching
     `ReadState` after committed entries in that Ready have been applied;
   - a separate local-read endpoint demonstrates stale reads.
4. **Hosting**
   - separate loopback HTTP/1 application and plaintext HTTP/2 gRPC ports;
   - one supervised Ready consumer and one supervised logical tick loop;
   - exactly the expected term-one bootstrap configuration prefix is accepted;
     every later configuration entry is a deterministic application no-op;
   - snapshot restore is supported, but the MVP does not create snapshots or
     compact logs.
5. **Scope**
   - fixed membership;
   - no WAL, durable restart, TLS, authentication, discovery, dynamic
     reconfiguration, production retry policy, or deployment automation.

## Rationale

The bytes envelope keeps one authoritative Raft wire schema and makes the
transport boundary explicit. Separate ports avoid plaintext HTTP/1 versus
HTTP/2 protocol negotiation ambiguity. Synchronous Ready processing is the
smallest integration surface and directly illustrates the host contract.

Waiting for local application gives write endpoints a useful completion
semantic without inventing a client protocol inside the consensus library.
Read index demonstrates the correct linearizable read barrier instead of
presenting local state as universally consistent.

## Consequences

### Positive

- Users can run and inspect a real three-process cluster.
- gRPC is shown as a host transport rather than part of Raft itself.
- Ready ordering, configuration application, and read barriers are executable
  documentation.
- The host can be tested with an in-memory transport independently of gRPC.

### Tradeoffs

- `MemoryStorage` and the state machine are lost on restart.
- `MemoryStorage` cannot demonstrate atomic durable publication or an actual
  `MustSync` flush; the sample marks both as volatile no-ops.
- The sample does not exercise snapshot creation or compaction.
- JSON commands favor readability over wire efficiency.
- A proposal can commit after the originating HTTP request times out.

## Rejected alternatives

- **Duplicate the Raft protobuf in the transport proto:** risks schema drift
  and generated-type conflicts.
- **Build a durable database/WAL first:** obscures the integration lesson and
  over-designs the educational milestone.
- **Use lease reads:** requires host clock-drift assumptions that distract
  from the read-index barrier.
- **Use one mixed plaintext HTTP/1+HTTP/2 port:** protocol selection is
  environment-sensitive without TLS.
- **Add dynamic membership:** configuration application is demonstrated by
  bootstrap entries; an admin membership API is a separate feature.

## Outcome

Implemented as `examples/DotnetRaft.KvCluster` with component tests,
deterministic three-node in-memory integration tests, and a real three-process
gRPC smoke test.

ADR 0007 later extends this same host with durable Raft/application SQLite
state, deduplication, snapshots, compaction, and crash/restart recovery.
