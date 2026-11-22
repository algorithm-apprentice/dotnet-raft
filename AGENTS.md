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

- [Host integration contract](docs/integration/README.md): Ready, persistence,
  application, snapshots, and recovery.
- [SQLite package guide](src/DotnetRaft.Sqlite/README.md): SQLite-specific
  persistence and operational requirements. The
  [storage design](docs/internals/sqlite-storage.md) explains the schema
  and transaction choices.
- [KV example](examples/DotnetRaft.KvCluster/README.md) and
  [example design](docs/internals/kv-host.md): running the host and
  its application protocol.
- [Implementation map](docs/internals/README.md) and
  [decisions](docs/adr/README.md): current ownership and design rationale.
  Follow the [documentation rules](docs/maintaining/README.md#maintaining-the-docs)
  when recording a change. Explain the invariant, its owner, and what verifies
  it; another implementation is not a substitute for a reason.

## Verify the boundary that changed

Search for existing helpers and tests before adding another implementation.
Exercise both synchronous and asynchronous paths when the rule is shared.
Use the deterministic tests under `tests/DotnetRaft.Tests` for the core,
facades, and host; `tests/DotnetRaft.Sqlite.Tests` for durable storage and
reopen; and `eng/smoke-durable-kv-cluster.sh` for the real-process example.
Keep protobuf, public API, and SQLite schema approvals intact unless a
compatibility change is intentional.
