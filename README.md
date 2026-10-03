# dotnet-raft

`dotnet-raft` is an educational, behavior-oriented C# implementation of the
deterministic Raft consensus state machine from
[`etcd-io/raft`](https://github.com/etcd-io/raft), pinned to commit
`1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`.

It implements consensus only. Network transport, durable storage engines,
application state, snapshots, compaction policy, timers, and retries remain
host responsibilities.

## Install

```bash
dotnet add package DotnetRaft --version 1.0.0
```

The package targets `net10.0`.

## Choose an integration API

- `RawNode` is synchronous and thread-unsafe. The host owns one serialized
  event loop and passes every returned `Ready` back to `Advance`.
- `RaftNode` is a concurrent asynchronous facade. One background owner loop
  serializes commands and exposes cancellable `ValueTask` operations.
- `AsyncStorageWrites` replaces `Advance` with reliable local
  `MsgStorageAppend` and `MsgStorageApply` request/response queues.

```csharp
using DotnetRaft;
using DotnetRaft.Storage;

var storage = new MemoryStorage();
var config = new RaftConfig
{
    Id = 1,
    ElectionTick = 10,
    HeartbeatTick = 1,
    Storage = storage,
};

await using RaftNode node = RaftNode.Start(
    config,
    [new Peer(1)]);

Ready ready = await node.WaitForReadyAsync();
// Persist ready.Snapshot, ready.Entries, and ready.HardState;
// apply ready.CommittedEntries in order; send ready.Messages.
await node.AdvanceAsync();
```

The abbreviated sample does not replace the durability, configuration,
physical-application, and recovery rules in the
[public integration guide](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/public-api.md).

## Documentation

- [Public API and host responsibilities](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/public-api.md)
- [Behavioral parity matrix](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/parity-matrix.md)
- [Reference architecture](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/reference-architecture.md)
- [Implementation DAG](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/implementation-dag.md)
- [Performance methodology and baseline](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/performance.md)
- [Release and compatibility ADR](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/adr/0002-release-and-compatibility-contract.md)
- [Apache-2.0 license](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/LICENSE)
- [Third-party notices](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/THIRD-PARTY-NOTICES.md)

## Project status

- **Completed milestone:** M7 Parity release
- **Completed nodes:** D00-D26
- **Next work:** behavior-preserving post-parity `RaftCore` refactoring

Development preserves the pinned implementation's behavior and invariants
before performing the separately reviewed post-parity structural refactor.
