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

Follow the [repository installation instructions](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/README.md#install)
to build both local packages and install `DotnetRaft.Sqlite`. The package
targets `net10.0` and uses `Microsoft.Data.Sqlite`.

## Open and restart

Run the pending-snapshot reconciliation before constructing a restarted node.
`RestoreApplicationSnapshot` below is supplied by the host.

```csharp
using DotnetRaft.Protocol;
using DotnetRaft.Storage.Sqlite;

using var storage = new SqliteStorage("data/raft.db");

Snapshot? pending =
    storage.GetPendingApplicationSnapshot();
if (pending is not null)
{
    // Atomically restore application bytes, physical-applied index,
    // and pending.Metadata.ConfState before acknowledging.
    RestoreApplicationSnapshot(pending);

    HardState hardState =
        storage.GetHardState()
        ?? throw new InvalidDataException(
            "A pending non-bootstrap snapshot requires durable HardState.");
    if (hardState.Term
        < pending.Metadata.Term)
    {
        throw new InvalidDataException(
            "Recovered HardState term is older than the pending snapshot.");
    }

    if (hardState.Commit
        < pending.Metadata.Index)
    {
        hardState.Commit =
            pending.Metadata.Index;
        storage.SetHardState(hardState);
    }

    storage.AcknowledgeApplicationSnapshot(
        pending.Metadata.Index);
}
```

`GetInitialState` rejects startup while a pending snapshot remains.
Acknowledgement requires the exact pending index and durable commit covering
it. Repair commit only from trusted recovery evidence; never infer term and
vote from snapshot metadata. Restoring the same snapshot after a crash must be
safe and idempotent. Then follow the
[host restart contract](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/public-api.md#rawnode)
to supply the recovered physical application cursor and its configuration.

## Persisting Ready and local append work

Call `storage.PersistReady(ready)` at the persistence step of the
[synchronous Ready loop](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/public-api.md#synchronous-ready-loop).
Call `storage.PersistStorageAppend(request)` in the
[asynchronous append worker](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/public-api.md#append-worker)
instead when using asynchronous storage writes. Both methods publish snapshot,
entries, and hard state in one `synchronous=FULL` transaction. Neither sends
messages nor applies application state.

For an incoming snapshot, restore the application, then acknowledge its exact
index with `storage.AcknowledgeApplicationSnapshot(index)`. Do this before
`Advance` in synchronous mode or before delivering the append responses in
asynchronous mode, under the host's application barrier. In asynchronous mode,
follow the linked worker protocol: step only responses addressed to the local
node and send responses addressed to remote peers over the network.

`GetEntries` reads a bounded prefix. It includes a first entry even if it
exceeds `maxSize` and stops at the budget boundary without decoding the rest
of the requested range.

## Operational constraints

- keep the database, `-wal`, and `-shm` files on one local filesystem;
- do not access one database through hard-link aliases;
- only one `SqliteStorage` owner may open a database at a time;
- copy live databases only through SQLite's backup facilities, never by
  copying the main file alone; and
- dispose the storage during orderly shutdown.

The package stores Raft consensus state only. It does not implement an
application state machine, MVCC, lease/watch/auth, or cross-database
transactions. If application state is stored separately, treat the Raft and
application databases as one recovery set. The pending marker handles process
crashes, not mismatched backups. Stop the host and preserve its complete data
directory together, or use a coordinated backup protocol; backing up one live
SQLite database is not a coordinated backup of both.
