# Software architecture and source guide

<a name="how-dotnet-raft-works"></a>

Start here to read the C# implementation. In each replica, the library decides
what can be committed and describes the work to do; the host drives it and
performs the effects. Read the two views below, then follow one Counter write
before opening the election, snapshot, or membership code.

## Package dependencies

These are direct project references, not the order of runtime calls.

| Project | References inside the repository |
|---|---|
| [`DotnetRaft`](../../src/DotnetRaft/DotnetRaft.csproj) | None. Owns consensus, the node APIs, and the domain values and interfaces the host uses. |
| [`DotnetRaft.Protobuf`](../../src/DotnetRaft.Protobuf/DotnetRaft.Protobuf.csproj) | `DotnetRaft`. Maps its domain values to and from the existing `raftpb` format. |
| [`DotnetRaft.Sqlite`](../../src/DotnetRaft.Sqlite/DotnetRaft.Sqlite.csproj) | `DotnetRaft` and `DotnetRaft.Protobuf`. Implements stable Raft storage using the codec for stored records. |
| [`DotnetRaft.Counter`](../../examples/DotnetRaft.Counter/DotnetRaft.Counter.csproj) | `DotnetRaft` and `DotnetRaft.Protobuf`. Uses the codec as its log format with `MemoryStorage`; its simulated network passes domain messages directly. |
| [`DotnetRaft.KvCluster`](../../examples/DotnetRaft.KvCluster/DotnetRaft.KvCluster.csproj) | `DotnetRaft` and `DotnetRaft.Sqlite`, with the codec available transitively. Uses it for the Raft payload inside the example's gRPC envelope. |

The core has no protobuf, SQLite, or gRPC dependency. At runtime it can read
the SQLite log because the host supplies an implementation of
[`IStorage`](../../src/DotnetRaft/Storage/IStorage.cs), an interface owned by
the core. The supplied [`IRaftLogFormat`](../../src/DotnetRaft/Protocol/IRaftLogFormat.cs)
defines encoded entry sizes and configuration-change payloads without giving
consensus a dependency on generated types. See the
[domain/codec decision](../adr/0011-complete-domain-codec-separation.md).

## Components and ownership

```text
host tick, proposal, or decoded peer message -> RawNode -> RaftCore
pending effects -> RawNode.Ready() -> host stores, sends, and applies
host acknowledgement -> RawNode -> next RaftCore transition
```

The host can serialize calls to `RawNode` itself, or use `RaftNode` to serialize
them on its owner loop. That choice is independent of how the host
acknowledges work: `Advance` in synchronous mode or typed `StorageCompletion`
receipts in asynchronous mode. Neither facade runs the host's workers.
Stable log reads through `IStorage` are synchronous in both modes. Use the
[integration contract](../integration/README.md) for the required ordering.

| Owner | What it decides or maintains |
|---|---|
| [`RaftCore`](../../src/DotnetRaft/Core/RaftCore.cs) | Elections, log matching, quorum commitment, membership, and read barriers. Coordinates changes across its [state owners](consensus.md#coordinate-transitions-in-one-place); does not send messages or publish writes. |
| [`RawNode`](../../src/DotnetRaft/RawNode.cs) | Delivers pending effects as `Ready` and binds acknowledgements to that work. `Ready` describes work to perform, not effects already performed. |
| [`RaftNode`](../../src/DotnetRaft/RaftNode.cs) | Serializes callers, manages cancellation and shutdown, and calls the same `RawNode`. [Node internals](node.md) explain both facades and their completion paths. |
| [`MemoryStorage`](../../src/DotnetRaft/Storage/MemoryStorage.cs) or [`SqliteStorage`](../../src/DotnetRaft.Sqlite/SqliteStorage.cs) | Publishes and reads the stable log. Memory storage does not survive a process restart; the SQLite backend owns its transactions. Neither decides what the quorum committed. |
| [`ProtobufRaftCodec`](../../src/DotnetRaft.Protobuf/ProtobufRaftCodec.cs) | Encodes and decodes [.NET messages and records](messages.md). Only the adapter needs the generated Raft types. |
| Host, e.g. [KV cluster](kv-host.md) | Supplies ticks and peer transport (including identity policy), runs the Ready consumer, applies commands, and owns client results and application recovery. |

<a name="follow-a-request"></a>

## Follow a write

Run the synchronous [Counter write trace](../../examples/DotnetRaft.Counter/README.md#trace-one-write).
Follow the `increment` at index 5 from [`Program.SubmitIncrement`](../../examples/DotnetRaft.Counter/Program.cs).
Skip election and bootstrap on the first pass: the program has already
elected node 1 in term 2 and applied entries through index 4. Use the method
names below as breakpoints. At each stop, record the replica, term, and index,
and ask what the trace actually proves.

| Evidence in the trace | Code to follow |
|---|---|
| **Proposed:** the leader appends an unstable entry and **queues** a self-response. It has not yet counted its own copy. | [`RawNode.Propose`](../../src/DotnetRaft/RawNode.cs) builds a `MsgProp`. [`RaftCore.Step`](../../src/DotnetRaft/Core/RaftCore.cs) routes it to [`HandleLeaderProposal`](../../src/DotnetRaft/Core/RaftCore.Replication.cs), which appends and broadcasts. |
| **Stored by a replica:** node 2 stores the entry before node 1 receives its reply. Queuing the reply does not prove storage. | [`HandleAppendEntries`](../../src/DotnetRaft/Core/RaftCore.Replication.cs) calls [`RaftLog.MaybeAppendEntries`](../../src/DotnetRaft/Core/RaftLog.cs) to match the prefix and extend or replace the suffix. [`RaftOutput.Enqueue`](../../src/DotnetRaft/Core/RaftOutput.cs) puts `MsgAppResp` in the after-append queue. [`CounterCluster.ProcessSynchronousReady`](../../examples/DotnetRaft.Counter/CounterCluster.cs) writes to memory before enqueuing peer replies or calling `Advance`. |
| **Committed:** in this run, node 2's reply lets node 1 advance `Commit`. | [`HandleAppendResponse`](../../src/DotnetRaft/Core/RaftCore.Replication.cs) updates that replica's `Match`; `MaybeCommit` combines the [voter quorum](../../src/DotnetRaft/Tracker/ProgressTracker.cs) with the current-term rule. The leader counts its own copy only after processing its self-response, but a quorum need not include the leader. |
| **Applied and acknowledged:** the counter changes before `Applied` advances in this host. | [`RawNode.CreateReady`](../../src/DotnetRaft/RawNode.cs) marks the delivered slice as `Applying`. [`CounterReplica.Apply`](../../examples/DotnetRaft.Counter/CounterReplica.cs) updates the counter and its index. [`RawNode.AdvanceCore`](../../src/DotnetRaft/RawNode.cs) then acknowledges the batch. |

The Counter demonstrates ordering, not disk durability. Do not use one
cursor as evidence for another: `Commit` is a consensus decision; `Applying`
tracks delivery; `Applied` tracks what Raft has been told about application.
Client results and recovery depend on the host's
[physical application progress](../integration/application.md#application-progress).

Read [`SingletonSelfAppendWaitsForAdvance`](../../tests/DotnetRaft.Tests/RawNode/RawNodeReadyTests.cs)
to isolate the self-response barrier, then
[`CurrentTermRuleBlocksOlderEntriesUntilNoOpIsAcknowledged`](../../tests/DotnetRaft.Tests/Core/RaftCoreReplicationTests.cs)
to isolate the quorum and term rule. Both tests are smaller than the full
Counter schedule.

### Change only the storage protocol

Now use the async variant of the same trace. The example still uses `RawNode`
and the same consensus rules. Start at
[`CounterCluster.DispatchAsyncStorageWork`](../../examples/DotnetRaft.Counter/CounterCluster.cs):
append and apply go to separate FIFO queues. After storing, `ProcessAppend`
queues deferred replies before any append receipt. The apply worker returns
its own receipt after changing the counter. While the example pauses apply,
watch `Commit` reach 5 while the counter is still zero. See
[Ready internals](node.md#why-asynchronous-completions-carry-evidence) for why
the append and apply receipts carry different evidence.

## Storage and application hosts

Next follow [a fresh KV write](kv-host.md#follow-a-fresh-kv-write). This host
uses the concurrent facade `RaftNode` but returns to synchronous Ready/Advance.
Find where it publishes entries to `raft.db`, applies the command and its
request result to `application.db`, and releases the HTTP waiter. They are
distinct steps: a commit does not return the application result, and the two
databases do not share a transaction.

- [SQLite storage internals](sqlite-storage.md): why the schema uses unsigned
  keys, where a write becomes durable, and what the pending snapshot marker
  can and cannot prove.
- [KV restart](kv-host.md#startup-and-recovery): reconcile the durable Raft
  prefix with the application position before serving again.
- [Read path](../learning/04-membership-reads-and-liveness.md#derive-readindex-from-two-separate-conditions):
  Raft supplies a safe index; the host waits for physical application before
  reading. A linearizable read need not append a new log entry.

From here, use the [learning chapters](../learning/README.md#implementation-deep-dives)
to work through elections, replication, and failures; the
[test map](../maintaining/testing.md) to verify a boundary; and the
[decision index](../adr/README.md) for the trade-offs behind the design.
