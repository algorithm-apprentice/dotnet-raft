# Design records

Start with current guidance to use or change the system. Read design notes
for the reasoning behind a boundary; use the milestone records only to trace
how the port was built.

## Current guidance

| Question | Read this | Inspect this |
|---|---|---|
| Who owns consensus, Ready, and the application? | [C# architecture](../reference-architecture.md) | [`RaftCore`](../../src/DotnetRaft/Core/RaftCore.cs), [`RawNode`](../../src/DotnetRaft/RawNode.cs), [`RaftNode`](../../src/DotnetRaft/RaftNode.cs) |
| When can I send, apply, or acknowledge Ready work? | [Ready/Advance](../public-api.md#synchronous-ready-loop) or [async storage workers](../public-api.md#asynchronous-storage-writes) | [Counter in both modes](../../examples/DotnetRaft.Counter/README.md) |
| What is safe to read, snapshot, or replay? | [Application progress](../public-api.md#application-progress), [snapshots](../public-api.md#snapshots-and-compaction), [restart](../public-api.md#restart) | [Physical cursor and crash replay tests](../../tests/DotnetRaft.Tests/RawNode/RawNodePersistenceTests.cs) |
| How do I run or recover a durable host? | [SQLite package guide](../../src/DotnetRaft.Sqlite/README.md), [KV runbook](../../examples/DotnetRaft.KvCluster/README.md) | [KV recovery code](../../examples/DotnetRaft.KvCluster/DurableHostRecovery.cs) |
| How do I learn or verify the system? | [Learning path and labs](../raft-learning-guide.md), [parity matrix](../parity-matrix.md) | [C# test map](../reference-architecture.md#test-architecture) |

## Post-parity designs

These notes explain trade-offs and failure cases that are not obvious from an
API signature. They do not replace the host contract or package guide.

| Area | Rationale and implementation notes |
|---|---|
| Consensus ownership | [RaftCore collaborator boundaries](../adr/0003-raftcore-structural-boundaries.md) |
| Node concurrency | [Owner-loop race tests](raftnode-concurrency-hardening.md) |
| Protocol input | [Validation and fault boundaries](protocol-input-hardening.md) |
| SQLite internals | [Schema, transactions, and the pending snapshot marker](sqlite-durable-storage.md) |
| Durable KV example | [Client request identity and recovery across two databases](durable-grpc-kv-host.md) |

## Decision records

ADRs record the scope and reasons for decisions. Follow a superseding record
where one is listed. Use the linked current guides for operating instructions.

| Decision | Status | Scope |
|---|---|---|
| [0001 Porting strategy](../adr/0001-porting-strategy.md) | Accepted | Reference pin, port scope, and library/host boundary; the original port is complete. |
| [0002 Release and compatibility](../adr/0002-release-and-compatibility-contract.md) | Accepted | API, wire, storage, host protocol, and package compatibility. |
| [0003 RaftCore boundaries](../adr/0003-raftcore-structural-boundaries.md) | Accepted | Single ownership of mutable consensus state. |
| [0004 Original gRPC example](../adr/0004-educational-grpc-kv-cluster.md) | Partially superseded | The educational transport scope remains; [0007](../adr/0007-durable-fixed-membership-kv-host.md) replaces the volatile storage and application design. |
| [0005 Concurrency verification](../adr/0005-deterministic-raftnode-concurrency-verification.md) | Accepted | Deterministic evidence for owner-loop ordering, cancellation, and failure. |
| [0006 SQLite storage](../adr/0006-sqlite-durable-raft-storage.md) | Accepted | Independent Raft storage package and its durability boundary. |
| [0007 Durable fixed-membership KV](../adr/0007-durable-fixed-membership-kv-host.md) | Accepted | Durable state and request identity for the three-node example. |
| [0008 Protocol input hardening](../adr/0008-protocol-input-hardening.md) | Accepted | Validation before mutation; fail closed on internal faults. |
| [0009 Automatic joint exit](../adr/0009-auto-leave-retry-after-transfer-timeout.md) | Accepted | Retry after a transfer timeout rather than waiting for another apply. |

## Completed reference port

The [porting timeline](../implementation-dag.md) explains the D00–D26 labels.
Each note below keeps the milestone's contribution and a path to current code
or tests. Original specifications, checklists, and review outcomes remain
available in the [pre-consolidation source](https://github.com/algorithm-apprentice/dotnet-raft/tree/45160468cb7a0a3465c5fbebf088b5998188890b/docs/design).
Open the same filename there, or use `git show 4516046:<path>` in a local checkout.

| Topic | Milestone notes |
|---|---|
| Stable and unstable log | [D04 Storage](d04-stable-storage.md), [D05 Unstable log](d05-unstable-log.md), [D06 Unified log](d06-raft-log.md) |
| Replication | [D07 Inflight window](d07-inflight-window.md), [D08 Progress states](d08-follower-progress.md), [D09 Quorum tracking](d09-progress-tracker.md), [D14 Append and commit](d14-basic-log-replication.md), [D15 Flow control](d15-replication-flow-control.md), [D16 Snapshots](d16-snapshots.md) |
| Roles and elections | [D12 Core state](d12-core-state-machine-shell.md), [D13 Elections](d13-leader-election.md), [D19 Availability](d19-availability-extensions.md), [D20 Transfer](d20-leadership-transfer.md) |
| Membership and reads | [D10 Configuration changes](d10-configuration-changes.md), [D11 Read tracker](d11-read-only-tracker.md), [D17 Membership integration](d17-membership-integration.md), [D18 Safe reads](d18-safe-linearizable-reads.md) |
| Host integration | [D21 Ready/Advance](d21-rawnode-ready.md), [D22 Bootstrap and diagnostics](d22-bootstrap-status-diagnostics.md), [D24 Concurrent facade](d24-concurrent-node.md), [D25 Async storage](d25-asynchronous-storage-writes.md) |
| Verification and release | [D23 Interaction harness](d23-interaction-harness.md), [D26 Parity and release](d26-parity-release-hardening.md) |

Other historical evidence: the [volatile KV example](grpc-kv-cluster-example.md)
was replaced by the durable host; the [completed core refactor](post-parity-raftcore-refactor.md)
and its [recorded baseline](post-parity-refactor-safety-baseline.md) explain
what the extraction changed and how it was checked.

## Maintaining these docs

Update the current owner of a rule when behavior changes: the host contract
for integration, the package guide for SQLite operations, and the example
guide or design for application-specific behavior. Link to that owner from
learning and design notes instead of copying instructions. Keep the rationale
and a source or regression link in design notes; leave API listings, schemas,
and exhaustive test cases in source and approvals. Record a superseding
decision rather than rewriting the context of an old ADR.
