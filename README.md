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

- **Completed milestone:** M1 Foundations
- **Current milestone:** M2 Log substrate
- **Completed nodes:** D00-D04
- **Next node:** D05 Unstable log buffer

- [Reference architecture](docs/reference-architecture.md)
- [Implementation DAG](docs/implementation-dag.md)
- [ADR 0001: Porting strategy and architectural boundary](docs/adr/0001-porting-strategy.md)
- [D04 stable storage design](docs/design/d04-stable-storage.md)

## Development principles

- Treat messages and logical ticks as deterministic state-machine inputs.
- Keep persistence and transport outside the consensus core.
- Port behavior and invariants, not Go syntax or goroutine structure.
- Complete and verify one DAG node before starting the next.
- Use the reference tests as executable specifications.
- Preserve Apache-2.0 attribution for material derived from `etcd/raft`.
