# Documentation

Choose the task you are doing. The tutorials explain the mechanisms; the
integration pages specify what a host must do. Neither depends on reading our
development history.

| Task | Where to go | What you will get |
|---|---|---|
| **Run** | [Examples](../examples/README.md) | A three-voter counter, then a restartable gRPC KV cluster. |
| **Learn** | [Raft learning path](learning/README.md) | Worked examples from elections to recovery and a client-history lab. |
| **Integrate** | [Host integration](integration/README.md) | Choose the node API and storage protocol, then start, drive, and recover a service. |
| **Read the code** | [Software architecture and source guide](internals/README.md) | Map project dependencies and runtime ownership, then trace one write from proposal through application. |
| **Maintain** | [Maintainer guide](maintaining/README.md) | Find the tests, record a decision, and verify compatibility or performance. |

### Three places to look for a rule

- **Any host:** [Host integration](integration/README.md) owns the rules for
  Ready, persistence and application, transport, and recovery.
- **A SQLite storage user:** the [SQLite package guide](../src/DotnetRaft.Sqlite/README.md)
  owns the database API, pending snapshot procedure, and backup limits.
- **A runnable example:** its [runbook](../examples/README.md) owns the commands
  and observable behavior. Example-specific implementation choices are in the
  [implementation map](internals/README.md#storage-and-application-hosts).

See [maintaining the docs](maintaining/README.md#maintaining-the-docs) for where
to put new material.
