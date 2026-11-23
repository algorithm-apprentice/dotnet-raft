# How dotnet-raft works

Start here to navigate the C# implementation. For a proof or worked example,
use the linked [learning chapters](../learning/README.md); for requirements
on a real host, use the [integration contract](../integration/README.md).

## Components and ownership

```text
ticks, proposals, peer messages, completions
    -> serialized RawNode call -> RaftCore transition
    -> Ready -> host writes, sends, and applies
    -> Advance or local storage responses -> next transition
```

[`RaftNode`](../../src/DotnetRaft/RaftNode.cs) optionally serializes the first
step. It does not run the host's storage workers. Stable log **reads** via
`IStorage` are synchronous even if Ready drives asynchronous local writes.

| Concern | Code owner | Reason for the boundary |
|---|---|---|
| Message and entry representation | [Messages and records](messages.md), backed by [`raft.proto`](../../src/DotnetRaft/Protocol/raft.proto) | One protobuf model supplies optional fields, cloning, and the encoded sizes used by log and queue limits. The example's transport wraps message bytes; it does not define a competing Raft schema. |
| Roles, log matching, commitment, membership | [`RaftCore`](../../src/DotnetRaft/Core/RaftCore.cs) and [consensus internals](consensus.md) | The core owns transitions that touch multiple components. It chooses effects without doing network or disk I/O itself, apart from stable reads. |
| Ready and acknowledgement | [`RawNode`](../../src/DotnetRaft/RawNode.cs), with the concurrent [`RaftNode`](../../src/DotnetRaft/RaftNode.cs) facade; see [node internals](node.md) | One implementation binds persistence and application completions to the work delivered. Concurrency cannot create a separate consensus or persistence path. |
| Stable view | [`IStorage`](../../src/DotnetRaft/Storage/IStorage.cs), [`MemoryStorage`](../../src/DotnetRaft/Storage/MemoryStorage.cs), optional [`SqliteStorage`](../../src/DotnetRaft.Sqlite/SqliteStorage.cs) | The backend owns publication, read consistency, and durability. Neither storage implementation decides what the quorum committed. |
| Application and transport | The consuming service; [KV host design](kv-host.md) | Only the service knows how to apply commands, bind peer identity, return client results, and recover application bytes. |

<a name="follow-a-request"></a>

## Follow a write

Run the [Counter write trace](../../examples/DotnetRaft.Counter/README.md#trace-one-write).
Follow index 5 from the `Propose` call through the application acknowledgement.
For a first pass, skip election and bootstrap: the program has already
elected node 1 in term 2 and applied entries through index 4.

| Evidence in the trace | Code to follow |
|---|---|
| The leader appends an unstable entry and **queues** a self-response. | [`RawNode.Propose`](../../src/DotnetRaft/RawNode.cs#L123) builds a `MsgProp`; [`RaftCore.Step`](../../src/DotnetRaft/Core/RaftCore.cs#L196) routes it. [`HandleLeaderProposal`](../../src/DotnetRaft/Core/RaftCore.Replication.cs#L38) calls `AppendLeaderEntries` and `BroadcastAppend`. |
| Each receiving follower checks the prefix and queues a reply; only the host can store the entry and deliver the reply. | [`HandleAppendEntries`](../../src/DotnetRaft/Core/RaftCore.Replication.cs#L264) calls `RaftLog.MaybeAppendEntries` to check the preceding tuple and replace a divergent suffix. [`RaftOutput.Enqueue`](../../src/DotnetRaft/Core/RaftOutput.cs#L16) places `MsgAppResp` in the after-append queue. In [`CounterCluster.ProcessSynchronousReady`](../../examples/DotnetRaft.Counter/CounterCluster.cs) the host stores the batch before delivering remote output or calling `Advance`. |
| Node 1 processes the response from node 2 and advances `Commit` in this run. | [`HandleAppendResponse`](../../src/DotnetRaft/Core/RaftCore.Replication.cs#L325) updates that replica's `Match`; `MaybeCommit` combines the [voter quorum](../../src/DotnetRaft/Tracker/ProgressTracker.cs) and the current-term check. The leader only counts its own copy after its self-response, but a quorum does not have to include the leader. |
| The counter changes, then Raft's acknowledgement cursor moves. | [`RawNode.CreateReady`](../../src/DotnetRaft/RawNode.cs#L487) marks the delivered slice as `Applying`. `CounterReplica.Apply` changes application state and updates its own index. [`RawNode.AdvanceCore`](../../src/DotnetRaft/RawNode.cs#L651) subsequently acknowledges the batch and advances Raft `Applied`. |

The async trace has the same append and quorum decisions. `CreateReady`
routes work through local `MsgStorageAppend` / `MsgStorageApply` requests
instead, and completed workers return the generated responses. See
[Ready internals](node.md#delivery-and-acknowledgement) for how the two
protocols record delivery and completion.

Read [`SingletonSelfAppendWaitsForAdvance`](../../tests/DotnetRaft.Tests/RawNode/RawNodeReadyTests.cs)
to isolate the self-response barrier, then
[`CurrentTermRuleBlocksOlderEntriesUntilNoOpIsAcknowledged`](../../tests/DotnetRaft.Tests/Core/RaftCoreReplicationTests.cs)
to isolate the quorum and term rule. Continue with
[a fresh KV write](kv-host.md#follow-a-fresh-kv-write) to replace the
simulated host with real storage and a client result.

A linearizable read does not need a new log entry per call. The leader
establishes a safe commit index and authority for the request; the host
waits for physical application before reading. See
[the read path](../learning/04-membership-reads-and-liveness.md#derive-readindex-from-two-separate-conditions).

## Storage and application hosts

- [SQLite storage internals](sqlite-storage.md): why the schema uses unsigned
  keys, where a write becomes durable, and what the pending snapshot marker
  can and cannot prove.
- [Durable KV host](kv-host.md): why client request results and application
  progress must survive together, and what happens between the two commits
  during snapshot recovery.

Use the [test map](../maintaining/testing.md) to verify a boundary and the
[decision index](../adr/README.md) to find trade-offs that still constrain it.
