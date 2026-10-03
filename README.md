# dotnet-raft

`dotnet-raft` is an educational, behavior-oriented C# port of
[`etcd-io/raft`](https://github.com/etcd-io/raft). The reference snapshot is
commit `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`.

The goal is to preserve the reference implementation's deterministic Raft
state-machine model while making the algorithm approachable to .NET
developers. Like `etcd/raft`, this library will implement consensus only.
Network transport, durable storage engines, and the replicated application
state machine remain application responsibilities.

## Current status

The architecture and implementation DAG are complete. Implementation is
following the validated graph in strict topological order.

- **Completed milestone:** M6 Public integration
- **Current milestone:** M7 Parity release
- **Completed nodes:** D00-D25
- **Next node:** D26 Parity and release hardening

- [Reference architecture](docs/reference-architecture.md)
- [Implementation DAG](docs/implementation-dag.md)
- [ADR 0001: Porting strategy and architectural boundary](docs/adr/0001-porting-strategy.md)
- [D04 stable storage design](docs/design/d04-stable-storage.md)
- [D05 unstable log design](docs/design/d05-unstable-log.md)
- [D06 unified Raft log design](docs/design/d06-raft-log.md)
- [D07 inflight window design](docs/design/d07-inflight-window.md)
- [D08 follower progress design](docs/design/d08-follower-progress.md)
- [D09 progress tracker design](docs/design/d09-progress-tracker.md)
- [D10 configuration changes design](docs/design/d10-configuration-changes.md)
- [D11 read-only tracker design](docs/design/d11-read-only-tracker.md)
- [D12 core state-machine shell design](docs/design/d12-core-state-machine-shell.md)
- [D13 leader election design](docs/design/d13-leader-election.md)
- [D14 basic log replication design](docs/design/d14-basic-log-replication.md)
- [D15 replication flow control design](docs/design/d15-replication-flow-control.md)
- [D16 snapshot design](docs/design/d16-snapshots.md)
- [D17 membership integration design](docs/design/d17-membership-integration.md)
- [D18 safe linearizable reads design](docs/design/d18-safe-linearizable-reads.md)
- [D19 availability extensions design](docs/design/d19-availability-extensions.md)
- [D20 leadership transfer design](docs/design/d20-leadership-transfer.md)
- [D21 RawNode and Ready design](docs/design/d21-rawnode-ready.md)
- [D22 bootstrap, status, and diagnostics design](docs/design/d22-bootstrap-status-diagnostics.md)
- [D23 deterministic interaction harness design](docs/design/d23-interaction-harness.md)
- [D24 concurrent Node wrapper design](docs/design/d24-concurrent-node.md)
- [D25 asynchronous storage writes design](docs/design/d25-asynchronous-storage-writes.md)

## Development principles

- Treat messages and logical ticks as deterministic state-machine inputs.
- Keep persistence and transport outside the consensus core.
- Port behavior and invariants, not Go syntax or goroutine structure.
- Complete and verify one DAG node before starting the next.
- Use the reference tests as executable specifications.
- Preserve Apache-2.0 attribution for material derived from `etcd/raft`.
