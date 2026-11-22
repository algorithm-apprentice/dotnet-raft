# Design records

Use the current contract for integration; use these records to understand
why the implementation was built this way.

| Question | Start here |
|---|---|
| How do I integrate either facade? | [Host contract](../public-api.md) |
| How do I persist and recover with SQLite? | [SQLite package guide](../../src/DotnetRaft.Sqlite/README.md) |
| Which example should I run? | [Runnable examples](../../examples/README.md) |
| Which component owns a rule? | [Architecture](../reference-architecture.md) |
| Which tests cover the pinned behavior? | [Parity matrix](../parity-matrix.md) |

## Completed reference port

The [implementation DAG](../implementation-dag.md) records ordering and
acceptance gates. These records describe the scope at each milestone;
limitations such as “until D25” are historical, not current behavior.

| Layer | Milestone records |
|---|---|
| Storage and log | [D04 Stable storage](d04-stable-storage.md), [D05 Unstable log](d05-unstable-log.md), [D06 Unified log](d06-raft-log.md) |
| Replication tracking | [D07 Inflight window](d07-inflight-window.md), [D08 Follower progress](d08-follower-progress.md), [D09 Progress tracker](d09-progress-tracker.md) |
| Membership and reads | [D10 Configuration changes](d10-configuration-changes.md), [D11 Read-only tracker](d11-read-only-tracker.md) |
| Core and elections | [D12 Core shell](d12-core-state-machine-shell.md), [D13 Leader election](d13-leader-election.md), [D14 Log replication](d14-basic-log-replication.md), [D15 Flow control](d15-replication-flow-control.md) |
| Protocol extensions | [D16 Snapshots](d16-snapshots.md), [D17 Membership integration](d17-membership-integration.md), [D18 Safe reads](d18-safe-linearizable-reads.md), [D19 Availability](d19-availability-extensions.md), [D20 Leadership transfer](d20-leadership-transfer.md) |
| Host integration | [D21 RawNode and Ready](d21-rawnode-ready.md), [D22 Bootstrap and status](d22-bootstrap-status-diagnostics.md), [D23 Interaction harness](d23-interaction-harness.md), [D24 Concurrent facade](d24-concurrent-node.md), [D25 Async storage](d25-asynchronous-storage-writes.md), [D26 Release baseline](d26-parity-release-hardening.md) |

## Post-parity designs

- [Core refactor plan](post-parity-raftcore-refactor.md) and
  [recorded baseline](post-parity-refactor-safety-baseline.md)
- [RaftNode concurrency verification](raftnode-concurrency-hardening.md)
- [Protocol input hardening](protocol-input-hardening.md)
- [SQLite storage internals](sqlite-durable-storage.md)
- [Durable KV host](durable-grpc-kv-host.md), which replaced the
  [original volatile gRPC example](grpc-kv-cluster-example.md)

## Decision records

1. [Porting strategy and boundary](../adr/0001-porting-strategy.md)
2. [Release and compatibility](../adr/0002-release-and-compatibility-contract.md)
3. [Core component ownership](../adr/0003-raftcore-structural-boundaries.md)
4. [Original gRPC KV example](../adr/0004-educational-grpc-kv-cluster.md)
5. [Concurrency verification](../adr/0005-deterministic-raftnode-concurrency-verification.md)
6. [SQLite storage](../adr/0006-sqlite-durable-raft-storage.md)
7. [Durable fixed-membership host](../adr/0007-durable-fixed-membership-kv-host.md)
8. [Protocol input validation](../adr/0008-protocol-input-hardening.md)
9. [Retry joint exit after transfer timeout](../adr/0009-auto-leave-retry-after-transfer-timeout.md)
