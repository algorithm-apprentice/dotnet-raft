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

## Keep serialization at the boundaries

- Use ordinary C# domain values for consensus and application decisions.
  Generated messages, field presence, and encoded size belong to transport,
  storage, or host protocol adapters. Keep existing wire and on-disk formats
  readable when a managed API changes.
- Do not add a new domain dependency on a generated type. Existing dependencies
  are listed in the [serialization-boundary audit](docs/maintaining/serialization-boundary-audit.md);
  migrate one end-to-end path rather than adding a second implementation.

## Decompose by responsibility, then share identical rules

- Make orchestration read as named steps at one abstraction level. Recursively
  extract independent operations and predicates until each leaf function has
  one clear purpose, rather than mixing scheduling, validation, I/O, and
  application decisions in the same body.
- Group state by the invariant and lifetime its owner maintains. Prefer a named
  value or state slot when fields must be published or cleared together. Do not
  replace a flat field list with an unbounded `State`/`Options` bag or hide the
  ordering between distinct owners. When a review identifies a pattern, check
  other owners for the same problem before treating it as a one-file cleanup.
- After decomposition, search for equivalent helpers and merge redundant
  implementations when their contracts, invariants, and owner are the same.
  Similar-looking code with different durability, error, or lifetime semantics
  is not the same operation.
- Prefer .NET standard library APIs for general-purpose operations, such as
  JSON encoding and parsing. Keep domain-specific field selection and value
  mapping in the owner; do not reimplement the format or its escaping rules.
- Keep required ordering and atomicity visible in the enclosing workflow.
  Small helpers must not split a transaction or lock boundary, duplicate a
  consensus transition, or hide when Ready work is acknowledged. A function's
  single responsibility does not imply that its I/O is an atomic transaction.

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
For structured output such as JSON, deserialize it and assert the resulting
data. Check exact text or bytes only when the representation is itself part
of the contract.
Use the deterministic tests under `tests/DotnetRaft.Tests` for the core,
facades, and host; `tests/DotnetRaft.Sqlite.Tests` for durable storage and
reopen; and `eng/smoke-durable-kv-cluster.sh` for the real-process example.
Keep protobuf, public API, and SQLite schema approvals intact unless a
compatibility change is intentional.
