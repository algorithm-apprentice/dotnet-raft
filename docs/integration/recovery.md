# Snapshots, compaction, and restart

Part of [host integration](README.md). Restart reconciles Raft storage and
the application; it is not another bootstrap and does not reconstruct one
store from the other's cursor.

## Snapshots and compaction

Take an application snapshot of its physical prefix. Capture the bytes, index,
and exact `ConfState` for the same prefix, and get the log term at that index
before it becomes unavailable. Publish the matching snapshot durably before
deleting entries it replaces:

```text
snapshot index <= physicalApplied
compaction index <= durably published application snapshot index
```

Never label a snapshot with a logical admission cursor while the state
machine is behind. For a received snapshot, follow the restore ordering for
your [Ready protocol](ready.md). A transport send report is not an
application restore acknowledgement.

## Restart

1. Ensure [earlier workers can no longer write](#fault-handling), then open
   the actual durable Raft and application stores as a coherent recovery set.
2. Resolve any pending snapshot restoration. With SQLite, follow the
   [pending snapshot procedure](../../src/DotnetRaft.Sqlite/README.md#open-and-restart),
   even if the application had already restored the bytes before the crash.
3. Recover the application's bytes, **physical** applied index, and membership
   at that index. If the application is behind the compacted boundary,
   restore a snapshot covering the missing prefix. Raft cannot replay
   entries that have been compacted.
4. Reconcile the recovered state using the constraints below, then call
   `RawNode.Restart(config)` or `RaftNode.Restart(config)` with the actual
   `RaftConfig.Applied`. Drain the remaining committed work through Ready.
   Do not call `Start` to recover a previously initialized cluster.

```text
RaftConfig.Applied = physically recovered application index
FirstIndex - 1 <= RaftConfig.Applied <= effective commit <= LastIndex
IStorage.GetInitialState().ConfState = membership at RaftConfig.Applied
```

The node initializes its cursors at `FirstIndex - 1` (zero for empty
storage). A nonempty `HardState` supplies the effective commit. A null or
all-zero state leaves the initial commit in place; it does not reconstruct
lost durable term or vote. `Applied = 0` cannot request replay below the
retained boundary.

If a non-forced commit update was lost while a later application prefix
survived, verify that the recovered application is trustworthy and covered by
the retained durable log or snapshot. Durably repair only `Commit` before
constructing the node. Otherwise recover an earlier compatible application
prefix, if available, or fail startup. Never infer `Term` or `Vote` from an
application cursor or a snapshot term.

`Applied` suppresses replay; it does not load membership from the application
database. `MemoryStorage` and `SqliteStorage` return the retained snapshot's
`ConfState`. If the recovered application includes a later accepted change,
publish a snapshot with its matching membership, supply the correct initial
configuration from your backend, or restore a compatible earlier state and
replay. Do not advance `Applied` past a membership change while returning
the old configuration.

The [durable KV example](../internals/kv-host.md#startup-and-recovery) shows
these checks across two SQLite databases. The
[recovery chapter](../learning/03-ready-and-recovery.md#evaluate-crashes-at-boundaries-not-at-arbitrary-source-lines)
walks through the results of crashing at different points.

## Fault handling

Do not retry a partially dispatched operation on a faulted instance. Stop
its Ready consumer and append/application workers. Before reading the
recovery point, wait until no in-flight operation from the old instance can
still write to either store. Discard unprocessed work and undelivered local
responses, not effects already published. Then reconcile the durable append
prefix and the actual application state and `ConfState` as above, and
construct a new node. Quiesce the same host workers when stopping a healthy
node for backup or restart; stopping the facade alone does not stop them.
