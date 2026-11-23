# DotnetRaft.Sqlite

`DotnetRaft.Sqlite` provides durable SQLite consensus storage for
[`DotnetRaft`](https://github.com/algorithm-apprentice/dotnet-raft).

It persists:

- Raft log entries;
- `HardState`;
- retained snapshots and configuration;
- prefix compaction; and
- atomic synchronous `Ready` and asynchronous `MsgStorageAppend` batches.

## Install

Follow the [local package installation guide](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/integration/install.md)
to build both local packages and install `DotnetRaft.Sqlite`. The package
targets `net10.0` and uses `Microsoft.Data.Sqlite`.

## Open and restart

Resolve the pending snapshot before constructing a restarted node.
`RestoreApplicationSnapshot` is a host-supplied operation that must
transactionally restore the snapshot's application bytes, applied index,
and `ConfState`. It must be safe to repeat after a crash.

```csharp
using DotnetRaft;
using DotnetRaft.Protocol;
using DotnetRaft.Storage.Sqlite;

using var storage = new SqliteStorage("data/raft.db");

Snapshot? pending =
    storage.GetPendingApplicationSnapshot();
if (pending is not null)
{
    RaftHardState hardState = storage.GetHardState()
        ?? throw new InvalidDataException(
            "A pending non-bootstrap snapshot requires durable HardState.");
    ulong lastTerm = storage.GetTerm(storage.GetLastIndex());
    if (hardState.Term < Math.Max(pending.Metadata.Term, lastTerm))
    {
        throw new InvalidDataException(
            "Recovered HardState term is older than the retained Raft state.");
    }

    RestoreApplicationSnapshot(pending);
    if (hardState.Commit < pending.Metadata.Index)
    {
        storage.SetHardState(hardState with { Commit = pending.Metadata.Index });
    }

    storage.AcknowledgeApplicationSnapshot(pending.Metadata.Index);
}
```

`GetInitialState` rejects startup while a pending snapshot remains.
Acknowledgement requires the exact pending index and durable commit covering
it. The snippet resolves only an incoming snapshot already marked pending;
do not use it to synthesize lost Raft state. A null marker does not prove the
application database is current. Continue
with the [host restart contract](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/integration/recovery.md#restart).
In particular, `SqliteStorage.GetInitialState()` takes `ConfState` from the
retained snapshot; setting `RaftConfig.Applied` does not update it.

## Persisting Ready and local append work

Call `storage.PersistReady(ready)` at the persistence step of the
[synchronous Ready loop](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/integration/ready.md#synchronous-ready-loop).
Call `storage.PersistStorageAppend(request)` in the
[asynchronous append worker](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/integration/ready.md#append-worker)
instead when using asynchronous storage writes. Both methods publish snapshot,
entries, and hard state in one `synchronous=FULL` transaction. Neither sends
messages nor applies application state.

For an incoming snapshot, restore the application, then acknowledge its exact
index with `storage.AcknowledgeApplicationSnapshot(index)`. Do this before
`Advance` in synchronous mode or before delivering the append responses in
asynchronous mode, under the host's application barrier. Finish reconciliation
before processing the next storage batch. In asynchronous mode, follow the
linked worker protocol: step only responses addressed to the local node and
send responses addressed to remote peers over the network.

`GetEntries` reads a bounded prefix. It includes a first entry even if it
exceeds `maxSize` and stops at the budget boundary without decoding the rest
of the requested range.

## Operational constraints

- keep the database, `-wal`, and `-shm` files on one local filesystem;
- do not access one database through hard-link aliases;
- only one `SqliteStorage` owner may open a database at a time; and
- dispose the storage during orderly shutdown.

The package does not expose a live backup API. An external SQLite `.backup`
against the live, exclusively owned Raft database is not supported. Stop
the host, including its storage and application workers, dispose storage,
and preserve the complete recovery set together. Never copy only the main
file of a live WAL database.

The package stores Raft consensus state only. It does not implement an
application state machine, MVCC, lease/watch/auth, or cross-database
transactions. If application state is stored separately, treat the Raft and
application databases as one recovery set. The pending marker handles process
crashes, not mismatched backups. Preserve the entire stopped node's data
directory rather than backing up or restoring one database independently.

For the layout and transaction rationale, see the
[SQLite storage internals](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/internals/sqlite-storage.md).
