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

## Follow a request

1. A proposal reaches the leader. `RaftCore` admits it, assigns term and
   index, and appends it to its unstable log. Followers check the preceding
   tuple before accepting the suffix. See
   [replication and the log](../learning/02-log-and-replication.md).
2. A successful append acknowledgement is evidence of durable work at one
   replica. A quorum can advance leader commit only through a current-term
   entry. The leader's own append also needs acknowledgement; an in-memory
   append is not replication evidence. See [election and commit safety](../learning/01-safety-and-elections.md).
3. Ready exposes application work for the selected committed prefix. An
   acknowledgement releases application accounting; proposal return, commit,
   and physical application are different events. See
   [Ready internals](node.md#delivery-and-acknowledgement)
   and the [recovery walkthrough](../learning/03-ready-and-recovery.md).
4. A linearizable read does not need a new log entry per call. The leader
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
