# Find an older documentation link

Use the [documentation map](../README.md) to browse the current system.
The former entry pages linked below remain as pointers because earlier
package READMEs link to them. They do not carry a second copy of any rule.

| Older path or topic | Current home |
|---|---|
| [`public-api.md`](../public-api.md) | [Host integration](../integration/README.md): startup, Ready, application, and recovery have separate pages. |
| [`raft-learning-guide.md`](../raft-learning-guide.md) | [Learning path](../learning/README.md). The five chapter URLs are unchanged. |
| [`reference-architecture.md`](../reference-architecture.md) | [Implementation map](../internals/README.md) and [consensus owners](../internals/consensus.md). |
| [`parity-matrix.md`](../parity-matrix.md) | [Executable test map](testing.md); external fixture attribution is in the [notices](../../THIRD-PARTY-NOTICES.md). |
| [`performance.md`](../performance.md) | [Benchmark guide](benchmarks.md). |
| `implementation-dag.md` | [Current code ownership](../internals/README.md); the original work order is in the archive below. |
| [`design/sqlite-durable-storage.md`](../design/sqlite-durable-storage.md) and `design/durable-grpc-kv-host.md` | [SQLite storage internals](../internals/sqlite-storage.md) and [KV host internals](../internals/kv-host.md). The SQLite URL is kept for earlier package READMEs. |
| Protocol input and concurrency design notes | [Message validation](../internals/node.md#validation-and-failure) and [deterministic race tests](testing.md#exercise-the-boundary-that-changed). |
| [`design/README.md`](../design/README.md) and the numbered design notes | Follow a [current topic](#numbered-notes-by-topic) or the [decision records](../adr/README.md). |

## Numbered notes by topic

| Older notes | Where the mechanism is explained now |
|---|---|
| D04–D06: stable storage, unstable overlay, merged log | [Storage contract](../integration/storage.md) and [replication walkthrough](../learning/02-log-and-replication.md#stable-storage-and-the-unstable-overlay). |
| D07–D09, D14–D16: replication, flow control, snapshots | [Replication internals](../internals/consensus.md#replicate-without-treating-a-prediction-as-evidence) and the [snapshot walkthrough](../learning/02-log-and-replication.md#when-entries-are-no-longer-available-snapshots). |
| D12–D13, D19–D20: role state, elections, availability | [Election safety](../learning/01-safety-and-elections.md) and [availability mechanisms](../learning/04-membership-reads-and-liveness.md#put-availability-features-in-their-place). |
| D10–D11, D17–D18: configuration and reads | [Membership and reads](../learning/04-membership-reads-and-liveness.md). |
| D21–D22, D24–D25: Ready, bootstrap, concurrency, async storage | [Integration workflow](../integration/README.md) and [node internals](../internals/node.md). |
| D23, D26: tests and packaging | [Test map](testing.md) and [release checks](release.md). |

The [last documentation tree](https://github.com/algorithm-apprentice/dotnet-raft/tree/2417d2efb7a0a683a7cbf05d508650eb43bfe437/docs)
before this redesign has the old pages and original outline. For the
earlier full notes and `implementation-dag.md`, see the
[archived tree](https://github.com/algorithm-apprentice/dotnet-raft/tree/45160468cb7a0a3465c5fbebf088b5998188890b/docs).
The commits also work offline with `git show <commit>:<path>`.
