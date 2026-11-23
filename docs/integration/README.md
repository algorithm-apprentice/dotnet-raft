# Integrate dotnet-raft into a service

This guide is the host contract. Read the choices below first, then use the
workflow pages in the order a service uses them. The
[counter](../../examples/DotnetRaft.Counter/README.md) gives a small running
host; the [KV runbook](../../examples/DotnetRaft.KvCluster/README.md) runs a host
with real peers and durable storage. [Install a local package](install.md) to
use the library in another project.

## Choose two things independently

**Who serializes node calls?**

| Facade | Choose it when… | How to receive work |
|---|---|---|
| `RawNode` | Your host already owns one serialized event loop. It is not thread-safe; route every call through that owner. | When `HasReady()` returns true, call `Ready()`. |
| `RaftNode` | Multiple tasks call the node. A single internal owner serializes calls to `RawNode`; it does not run the host's Ready consumer. | One consumer awaits `WaitForReadyAsync`. |

**Who acknowledges storage and application work?**

| Protocol | Choose it when… | Completion |
|---|---|---|
| [Ready / Advance](ready.md#synchronous-ready-loop) (default) | One ordered Ready loop is enough. Start here unless you need a pipeline. | Finish the required work for one batch before `Advance(ready)` or `AdvanceAsync()`. |
| [Asynchronous storage writes](ready.md#asynchronous-storage-writes) | You want independent FIFO append and apply workers and multiple batches in flight. | Return each issued `StorageCompletion` to its node after the required work. Do not call `Advance`. |

Either facade can use either protocol. `AsyncStorageWrites` changes the
write-completion protocol, not whether the facade has async methods or whether
`IStorage` reads block on disk.

## Follow the host workflow

| Step | Contract |
|---|---|
| Configure and start a node; route ticks and peer messages | [Node lifecycle](node.md), [transport](node.md#network-transport) |
| Persist, send, apply, and acknowledge batches | [Ready processing](ready.md) |
| Finish a proposal or a linearizable read; apply membership changes | [Application and client results](application.md) |
| Take snapshots, compact, or restart after a fault | [Snapshots and recovery](recovery.md) |
| Implement a storage engine rather than using SQLite | [Storage interface and publication](storage.md) |

## Where the library stops

Raft chooses a committed log prefix and the membership and read transitions
associated with it. The host provides ticks, peer transport and identity,
atomic publication of durable Raft batches, and a state machine that applies
the resulting commands. The host also owns request correlation, retries,
snapshots of application state, and restart reconciliation.

[`DotnetRaft.Sqlite`](../../src/DotnetRaft.Sqlite/README.md) provides durable
consensus storage, not an application database. The
[implementation map](../internals/README.md) explains the internal boundaries;
use this guide, not that map, to decide what your host must do.

The [failure model](../learning/01-safety-and-elections.md#start-with-the-problem-not-the-roles)
allows crashes and lost or reordered messages, not peers that forge votes
or claim writes they did not persist. A restarted voter must recover its
durable state before it can participate again with the same identity. Without
a live voting quorum, do not promise that a new operation will complete.
