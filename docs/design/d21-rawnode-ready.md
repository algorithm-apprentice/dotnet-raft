# D21 RawNode and Ready Design

Historical [port milestone](README.md#completed-reference-port). Use the
[synchronous Ready contract](../public-api.md#synchronous-ready-loop)
for host ordering. [D25](d25-asynchronous-storage-writes.md) later added the
alternative storage response protocol to the same facade.

Ready separates a consensus decision from its externally visible effects.
Accepting a batch marks exactly that work in flight, including committed
entries delivered for application. The host can still feed other inputs to
the node while it processes the batch.

## Advance

`RawNode` retains detached acknowledgement metadata, so advancing an exact
Ready neither acknowledges new work nor reinterprets a mutated protobuf
from the host. A self-directed append response is executed on advancement,
not exposed for transport. The node can retain the configuration of an earlier
delivered prefix for ordered application even while a newer snapshot supplies
the live consensus configuration.

Raft acknowledgement is not the same as actual state-machine completion.
Use [physical application progress](../public-api.md#application-progress)
for reads, snapshots, and recovery. The
[Ready tests](../../tests/DotnetRaft.Tests/RawNode/RawNodeReadyTests.cs) cover
batch identity and snapshot overlap; the
[persistence tests](../../tests/DotnetRaft.Tests/RawNode/RawNodePersistenceTests.cs)
cover the replay boundary after early Advance.
