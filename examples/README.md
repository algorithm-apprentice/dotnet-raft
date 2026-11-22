# Runnable examples

Start with the counter to see which work Raft does and which work belongs to
your host. The KV example then shows how the same contract fits a real service.

| Start here if you want to… | Example | Node API | Storage protocol |
|---|---|---|---|
| Elect a leader and replicate a command without running a server | [Replicated counter](DotnetRaft.Counter/README.md#ready-and-advance) | `RawNode` | Ready / Advance |
| See append and apply workers hand work back without `Advance` | [Counter with async storage](DotnetRaft.Counter/README.md#asynchronous-storage-writes) | `RawNode` | `AsyncStorageWrites` |
| Build a concurrent service that survives process restarts | [Durable gRPC KV cluster](DotnetRaft.KvCluster/README.md) | `RaftNode` | Ready / Advance, backed by SQLite |

The two counter runs share one program. They use memory and a simulated
network, so neither demonstrates crash durability. The KV cluster is an
educational loopback host, not a production deployment template.

Use the [host integration contract](../docs/public-api.md) when adapting
any example. In particular, choosing `RaftNode` for async calls does not
require opting into asynchronous storage writes.
