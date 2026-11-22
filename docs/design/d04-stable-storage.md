# D04 Stable Storage Design

Historical [port milestone](README.md#completed-reference-port). For current
requirements, see
[storage reads](../public-api.md#storage-reads) and [restart](../public-api.md#restart).

D04 made stable log reads a host-provided interface, not a write API. The core
can run against [`MemoryStorage`](../../src/DotnetRaft/Storage/MemoryStorage.cs)
or another implementation without knowing how a batch becomes durable.
The [SQLite package](../../src/DotnetRaft.Sqlite/README.md) follows that
boundary rather than moving database transactions into consensus.

Compaction removes old commands, not the term at the retained boundary.
`GetTerm(FirstIndex - 1)` must still work so the next append can prove its
preceding entry matches. Detached protobuf values prevent a caller from
mutating the stable view through a returned entry or snapshot.

See [memory storage tests](../../tests/DotnetRaft.Tests/Storage/MemoryStorageTests.cs)
for boundary, ownership, and size-limit examples.
