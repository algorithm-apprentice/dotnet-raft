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

Version `1.0.0` is not currently published to NuGet.org. Build both packages
from a repository checkout so the SQLite package can resolve its core
dependency:

```bash
dotnet restore DotnetRaft.sln
dotnet pack src/DotnetRaft/DotnetRaft.csproj -c Release
dotnet pack src/DotnetRaft.Sqlite/DotnetRaft.Sqlite.csproj -c Release
```

In the consumer project, create `NuGet.Config` if needed, add the generated
package directory alongside NuGet.org, and install:

```bash
dotnet new nugetconfig
dotnet nuget add source /absolute/path/to/dotnet-raft/artifacts/package \
  --name dotnet-raft-local --configfile NuGet.Config
dotnet add package DotnetRaft.Sqlite --version 1.0.0
```

The package targets `net10.0` and uses `Microsoft.Data.Sqlite`.

## Open and restart

```csharp
using DotnetRaft;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;
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

StorageState state = storage.GetInitialState();
var config = new RaftConfig
{
    Id = 1,
    ElectionTick = 10,
    HeartbeatTick = 1,
    Storage = storage,
    Applied = physicalApplied,
};

await using RaftNode node =
    RaftNode.Restart(config);
```

Commit may be repaired only from trusted durable/application recovery
evidence. Term and vote must never be guessed from snapshot metadata. Before
constructing the node, the host must enforce:

```text
HardState.Commit >= physicalApplied
snapshot/application ConfState = membership at physicalApplied
```

See the
[`DotnetRaft` integration guide](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/public-api.md)
for the complete recovery contract.

## Synchronous Ready

```csharp
Ready ready = await node.WaitForReadyAsync();

storage.PersistReady(ready);

foreach (Message message in ready.Messages)
{
    await transport.SendAsync(message);
}

if (ready.Snapshot is not null)
{
    RestoreApplicationSnapshot(ready.Snapshot);
    storage.AcknowledgeApplicationSnapshot(
        ready.Snapshot.Metadata.Index);
}

ApplyCommittedEntries(ready.CommittedEntries);
await node.AdvanceAsync();
```

`PersistReady` publishes snapshot, entries, and hard state in one
`synchronous=FULL` SQLite transaction.

## Asynchronous append worker

```csharp
storage.PersistStorageAppend(request);

if (request.Snapshot is not null)
{
    RestoreApplicationSnapshot(request.Snapshot);
    storage.AcknowledgeApplicationSnapshot(
        request.Snapshot.Metadata.Index);
}

foreach (Message response in request.Responses)
{
    await node.StepAsync(response);
}
```

Append requests must remain FIFO and reliable. Never deliver responses after
a failed persistence or application-snapshot restore.

## Operational constraints

- keep the database, `-wal`, and `-shm` files on one local filesystem;
- do not access one database through hard-link aliases;
- only one `SqliteStorage` owner may open a database at a time;
- copy live databases only through SQLite's backup facilities, never by
  copying the main file alone; and
- dispose the storage during orderly shutdown.

The package stores Raft consensus state only. It does not implement an
application state machine, MVCC, lease/watch/auth, or cross-database
transactions.
