# Design records

Use the current contract for integration; use these records to understand
why the implementation was built this way.

| Question | Start here |
|---|---|
| How do I integrate either facade? | [Host contract](../public-api.md) |
| How do I persist and recover with SQLite? | [SQLite package guide](../../src/DotnetRaft.Sqlite/README.md) |
| How do I run the example? | [KV example README](../../examples/DotnetRaft.KvCluster/README.md) |
| Which component owns a rule? | [Architecture](../reference-architecture.md) |
| Which tests cover the pinned behavior? | [Parity matrix](../parity-matrix.md) |

The numbered D04–D26 documents and the [implementation DAG](../implementation-dag.md)
record the completed reference port. Their scope and examples reflect their
milestones, not a second copy of the current integration contract. For internal
detail about the two storage protocols, see [D21](d21-rawnode-ready.md) and
[D25](d25-asynchronous-storage-writes.md).

[ADRs](../adr) record decisions and rationale. The
[original volatile gRPC design](grpc-kv-cluster-example.md) is historical;
the [durable host design](durable-grpc-kv-host.md) describes the current
example's application semantics. The
[SQLite storage design](sqlite-durable-storage.md) describes the separate
consensus store's internal invariants and schema.
