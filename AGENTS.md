# Working on dotnet-raft

## Find the owner before changing a rule

- `src/DotnetRaft/Core` owns consensus. `RawNode` owns the host-facing
  Ready/acknowledgement protocol; `RaftNode` serializes calls into `RawNode`.
  Do not implement the same state transition in both facades or storage modes.
- `src/DotnetRaft.Sqlite` is a separate `IStorage` implementation. Keep SQLite
  and its native assets out of the core package. Share pure protocol and log
  rules, not backend-specific transactions, errors, or lifetime management.
  Do not couple independently usable packages through internal binary APIs.
- `examples/DotnetRaft.KvCluster` owns transport, timers, KV application,
  request identity, and recovery across its two databases. The gRPC envelope
  carries the generated Raft message; it is not a second protocol model.

## Use the right specification

- [Host integration contract](docs/public-api.md): Ready, persistence,
  application, snapshots, and recovery.
- [SQLite package guide](src/DotnetRaft.Sqlite/README.md): SQLite-specific
  persistence and operational requirements. The
  [storage design](docs/design/sqlite-durable-storage.md) explains the schema
  and transaction choices.
- [KV example](examples/DotnetRaft.KvCluster/README.md) and
  [example design](docs/design/durable-grpc-kv-host.md): running the host and
  its application protocol.
- [Architecture](docs/reference-architecture.md),
  [parity matrix](docs/parity-matrix.md), and
  [design index](docs/design/README.md): ownership, reference coverage, and
  decision history. The numbered milestone notes are not current contracts.
  Follow the [documentation maintenance rules](docs/design/README.md#maintaining-these-docs)
  when recording a behavior change or design decision.

## Verify the boundary that changed

Search for existing helpers and tests before adding another implementation.
Exercise both synchronous and asynchronous paths when the rule is shared.
Use the deterministic tests under `tests/DotnetRaft.Tests` for the core,
facades, and host; `tests/DotnetRaft.Sqlite.Tests` for durable storage and
reopen; and `eng/smoke-durable-kv-cluster.sh` for the real-process example.
Keep protobuf, public API, and SQLite schema approvals intact unless a
compatibility change is intentional.
