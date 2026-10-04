using System.Diagnostics;
using System.Reflection;
using System.Text;

using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Sqlite.CrashHarness;
using DotnetRaft.Storage;
using DotnetRaft.Storage.Sqlite;

using Google.Protobuf;

using Microsoft.Data.Sqlite;

using static DotnetRaft.Sqlite.Tests.SqliteStorageTestSupport;

namespace DotnetRaft.Sqlite.Tests;

public sealed class SqliteStorageDurabilityTests
{
    [Fact]
    public void DirectMutationsSurviveReopen()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        var confState = new ConfState();
        confState.Voters.Add([1, 2, 3]);

        using (SqliteStorage storage =
               CreateStorage(directory))
        {
            storage.SetHardState(
                new HardState
                {
                    Term = 2,
                    Vote = 1,
                });
            storage.Append(
                Entries((1, 1), (2, 1), (3, 2)));
            storage.SetHardState(
                new HardState
                {
                    Term = 2,
                    Vote = 1,
                    Commit = 2,
                });
            storage.CreateSnapshot(
                2,
                confState,
                ByteString.CopyFromUtf8("state"));
            storage.Compact(2);
        }

        using SqliteStorage reopened =
            CreateStorage(directory);
        StorageState state =
            reopened.GetInitialState();
        Assert.Equal(2UL, state.HardState?.Term);
        Assert.Equal(1UL, state.HardState?.Vote);
        Assert.Equal(2UL, state.HardState?.Commit);
        Assert.Equal([1UL, 2UL, 3UL],
            state.ConfState.Voters);
        Assert.Equal(3UL, reopened.GetFirstIndex());
        Assert.Equal(3UL, reopened.GetLastIndex());
        Assert.Equal(1UL, reopened.GetTerm(2));
        Assert.Equal(
            "state",
            reopened.GetSnapshot()
                .Data.ToStringUtf8());
    }

    [Fact]
    public void PersistReadyPublishesBootstrapAtomically()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        Ready ready = BootstrapReady();

        using (SqliteStorage storage =
               CreateStorage(directory))
        {
            storage.PersistReady(ready);
        }

        using SqliteStorage reopened =
            CreateStorage(directory);
        Assert.Equal(
            ready.HardState,
            reopened.GetInitialState().HardState);
        AssertEntries(
            ready.Entries,
            reopened.GetEntries(
                1,
                reopened.GetLastIndex() + 1,
                ulong.MaxValue));
    }

    [Fact]
    public void PersistStorageAppendRequiresSnapshotReconciliation()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        var confState = new ConfState();
        confState.Voters.Add([1, 2, 3]);
        var request = new Message
        {
            Type = MessageType.MsgStorageAppend,
            From = 1,
            To =
                RaftLocalMessageTargets.AppendThread,
            Term = 3,
            Vote = 2,
            Commit = 6,
            Snapshot = SnapshotAt(
                4,
                2,
                confState,
                "snapshot"),
        };
        request.Entries.Add(
            Entries((5, 3), (6, 3)));

        using (SqliteStorage storage =
               CreateStorage(directory))
        {
            storage.PersistStorageAppend(request);
        }

        using (SqliteStorage reopened =
               CreateStorage(directory))
        {
            Snapshot pending =
                Assert.IsType<Snapshot>(
                    reopened
                        .GetPendingApplicationSnapshot());
            Assert.Equal(4UL, pending.Metadata.Index);
            Assert.Throws<InvalidOperationException>(
                reopened.GetInitialState);
            reopened.AcknowledgeApplicationSnapshot(
                4);
        }

        using SqliteStorage recovered =
            CreateStorage(directory);
        StorageState state =
            recovered.GetInitialState();
        Assert.Equal(3UL, state.HardState?.Term);
        Assert.Equal(6UL, state.HardState?.Commit);
        Assert.Equal([1UL, 2UL, 3UL],
            state.ConfState.Voters);
        Assert.Equal(5UL, recovered.GetFirstIndex());
        Assert.Equal(6UL, recovered.GetLastIndex());
    }

    [Fact]
    public void MalformedStorageAppendNeverMutates()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateStorage(directory);

        Message[] malformed =
        [
            new Message
            {
                Type =
                    MessageType.MsgStorageAppend,
                From = 1,
                To =
                    RaftLocalMessageTargets
                        .AppendThread,
                Term = 1,
            },
            new Message
            {
                Type =
                    MessageType.MsgStorageAppend,
                From =
                    RaftLocalMessageTargets
                        .ApplyThread,
                To =
                    RaftLocalMessageTargets
                        .AppendThread,
            },
            new Message
            {
                Type =
                    MessageType.MsgStorageAppend,
                From = 1,
                To =
                    RaftLocalMessageTargets
                        .AppendThread,
                Index = 1,
            },
            new Message
            {
                Type =
                    MessageType.MsgStorageAppend,
                From = 1,
                To =
                    RaftLocalMessageTargets
                        .AppendThread,
                Snapshot = new Snapshot(),
            },
        ];

        foreach (Message request in malformed)
        {
            Assert.Throws<ArgumentException>(
                () => storage
                    .PersistStorageAppend(request));
            Assert.Equal(0UL, storage.GetLastIndex());
            Assert.Null(
                storage.GetInitialState().HardState);
        }

        var commitPastLog = new Message
        {
            Type = MessageType.MsgStorageAppend,
            From = 1,
            To =
                RaftLocalMessageTargets.AppendThread,
            Term = 2,
            Vote = 1,
            Commit = 5,
        };
        commitPastLog.Entries.Add(
            Entries((1, 2)));
        Assert.Throws<InvalidOperationException>(
            () => storage.PersistStorageAppend(
                commitPastLog));
        Assert.Equal(0UL, storage.GetLastIndex());

        var termBehindSnapshot = new Message
        {
            Type = MessageType.MsgStorageAppend,
            From = 1,
            To =
                RaftLocalMessageTargets.AppendThread,
            Term = 1,
            Vote = 1,
            Commit = 4,
            Snapshot = SnapshotAt(
                4,
                2,
                data: "snapshot"),
        };
        Assert.Throws<InvalidOperationException>(
            () => storage.PersistStorageAppend(
                termBehindSnapshot));
        Assert.Equal(0UL, storage.GetLastIndex());
    }

    [Fact]
    public void DirectMutationsRejectUnrecoverableState()
    {
        using TemporaryStorageDirectory first =
            CreateDirectory();
        using (SqliteStorage storage =
               CreateStorage(first))
        {
            storage.SetHardState(
                new HardState
                {
                    Term = 4,
                });
            storage.Append(
                Entries(
                    (1, 1),
                    (2, 2),
                    (3, 3),
                    (4, 4)));
            storage.SetHardState(
                new HardState
                {
                    Term = 4,
                    Commit = 1,
                });

            Assert.Throws<InvalidOperationException>(
                () => storage.CreateSnapshot(
                    4,
                    new ConfState(),
                    ByteString.Empty));
            Assert.Equal(
                0UL,
                storage.GetSnapshot()
                    .Metadata.Index);

            storage.SetHardState(
                new HardState
                {
                    Term = 4,
                    Commit = 4,
                });
            storage.CreateSnapshot(
                4,
                new ConfState(),
                ByteString.Empty);
            Assert.Throws<InvalidOperationException>(
                () => storage.SetHardState(
                    new HardState
                    {
                        Term = 4,
                        Commit = 3,
                    }));
        }

        using TemporaryStorageDirectory second =
            CreateDirectory();
        using SqliteStorage suffix =
            CreateStorage(second);
        suffix.SetHardState(
            new HardState
            {
                Term = 5,
            });
        suffix.Append(
            Entries(
                (1, 1),
                (2, 2),
                (3, 3),
                (4, 4),
                (5, 5)));
        suffix.SetHardState(
            new HardState
            {
                Term = 5,
                Commit = 5,
            });

        Assert.Throws<InvalidOperationException>(
            () => suffix.Append(
                Entries((4, 5))));
        Assert.Equal(5UL, suffix.GetLastIndex());
        Assert.Equal(5UL, suffix.GetTerm(5));
    }

    [Fact]
    public void ConnectionUsesRequiredDurabilityPragmas()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateStorage(directory);
        SqliteConnection connection =
            GetConnection(storage);

        Assert.Equal(
            "exclusive",
            Scalar<string>(
                connection,
                "PRAGMA locking_mode;"));
        Assert.Equal(
            "wal",
            Scalar<string>(
                connection,
                "PRAGMA journal_mode;"));
        Assert.Equal(
            2L,
            Scalar<long>(
                connection,
                "PRAGMA synchronous;"));
        Assert.Equal(
            1L,
            Scalar<long>(
                connection,
                "PRAGMA fullfsync;"));
        Assert.Equal(
            1L,
            Scalar<long>(
                connection,
                "PRAGMA checkpoint_fullfsync;"));
    }

    [Fact]
    public void ExclusiveOwnershipRejectsOtherOpeners()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateStorage(directory);

        AssertStorageError(
            StorageError.Unknown,
            () => _ = CreateStorage(directory));

        using SqliteConnection raw =
            OpenRaw(
                directory.DatabasePath,
                timeoutSeconds: 1);
        using SqliteCommand command =
            raw.CreateCommand();
        command.CommandText =
            "SELECT COUNT(*) FROM raft_metadata;";
        command.CommandTimeout = 1;
        Assert.Throws<SqliteException>(
            command.ExecuteScalar);

        if (!OperatingSystem.IsWindows())
        {
            string alias = Path.Combine(
                directory.DirectoryPath,
                "alias.db");
            File.CreateSymbolicLink(
                alias,
                directory.DatabasePath);
            AssertStorageError(
                StorageError.Unknown,
                () => _ = new SqliteStorage(alias));
        }
    }

    [Fact]
    public void OpenRejectsUnknownOrAlteredSchema()
    {
        using TemporaryStorageDirectory unrelated =
            CreateDirectory();
        using (SqliteConnection connection =
               OpenRaw(unrelated.DatabasePath))
        {
            using SqliteCommand command =
                connection.CreateCommand();
            command.CommandText =
                "CREATE TABLE unrelated(value TEXT);";
            command.ExecuteNonQuery();
        }

        AssertStorageError(
            StorageError.Unknown,
            () => _ = CreateStorage(unrelated));

        using TemporaryStorageDirectory changed =
            CreateDirectory();
        using (SqliteStorage storage =
               CreateStorage(changed))
        {
        }

        using (SqliteConnection connection =
               OpenRaw(changed.DatabasePath))
        {
            using SqliteCommand command =
                connection.CreateCommand();
            command.CommandText =
                """
                CREATE TRIGGER unexpected
                BEFORE INSERT ON raft_entries
                BEGIN
                    SELECT RAISE(ABORT, 'unexpected');
                END;
                """;
            command.ExecuteNonQuery();
        }

        AssertStorageError(
            StorageError.Unknown,
            () => _ = CreateStorage(changed));
    }

    [Fact]
    public void OpenRejectsUnknownSchemaVersion()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using (SqliteStorage storage =
               CreateStorage(directory))
        {
        }

        using (SqliteConnection connection =
               OpenRaw(directory.DatabasePath))
        {
            using SqliteCommand command =
                connection.CreateCommand();
            command.CommandText =
                "PRAGMA user_version = 2;";
            command.ExecuteNonQuery();
        }

        AssertStorageError(
            StorageError.Unknown,
            () => _ = CreateStorage(directory));
    }

    [Fact]
    public void SchemaMatchesApproval()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using (SqliteStorage storage =
               CreateStorage(directory))
        {
        }

        var actual = new StringBuilder();
        using (SqliteConnection connection =
               OpenRaw(directory.DatabasePath))
        {
            using (SqliteCommand version =
                   connection.CreateCommand())
            {
                version.CommandText =
                    "PRAGMA user_version;";
                actual.Append("user_version=")
                    .Append(
                        version.ExecuteScalar())
                    .Append("\n\n");
            }

            using SqliteCommand schema =
                connection.CreateCommand();
            schema.CommandText =
                """
                SELECT type, name, sql
                FROM sqlite_schema
                WHERE name NOT LIKE 'sqlite_%'
                ORDER BY type, name;
                """;
            using SqliteDataReader reader =
                schema.ExecuteReader();
            while (reader.Read())
            {
                actual.Append(reader.GetString(0))
                    .Append(' ')
                    .Append(reader.GetString(1))
                    .Append('\n')
                    .Append(reader.GetString(2))
                    .Append("\n\n");
            }
        }

        string approved = Path.Combine(
            AppContext.BaseDirectory,
            "Schema",
            "SqliteSchema.approved.sql");
        Assert.Equal(
            File.ReadAllText(approved)
                .Replace(
                    "\r\n",
                    "\n",
                    StringComparison.Ordinal),
            actual.ToString());
    }

    [Fact]
    public void OpenRejectsEntryGapAndCorruptPayload()
    {
        using TemporaryStorageDirectory gap =
            CreateDirectory();
        using (SqliteStorage storage =
               CreateStorage(gap))
        {
            storage.SetHardState(
                new HardState
                {
                    Term = 1,
                });
            storage.Append(
                Entries((1, 1), (2, 1), (3, 1)));
        }

        using (SqliteConnection connection =
               OpenRaw(gap.DatabasePath))
        {
            using SqliteCommand command =
                connection.CreateCommand();
            command.CommandText =
                "DELETE FROM raft_entries WHERE entry_index = $index;";
            command.Parameters.AddWithValue(
                "$index",
                EncodeIndex(2));
            command.ExecuteNonQuery();
        }

        AssertStorageError(
            StorageError.Unknown,
            () => _ = CreateStorage(gap));

        using TemporaryStorageDirectory corrupt =
            CreateDirectory();
        using (SqliteStorage storage =
               CreateStorage(corrupt))
        {
            storage.SetHardState(
                new HardState
                {
                    Term = 1,
                });
            storage.Append(
                Entries((1, 1)));
        }

        using (SqliteConnection connection =
               OpenRaw(corrupt.DatabasePath))
        {
            using SqliteCommand command =
                connection.CreateCommand();
            command.CommandText =
                """
                UPDATE raft_entries
                SET payload = X'FFFF'
                WHERE entry_index = $index;
                """;
            command.Parameters.AddWithValue(
                "$index",
                EncodeIndex(1));
            command.ExecuteNonQuery();
        }

        AssertStorageError(
            StorageError.Unknown,
            () => _ = CreateStorage(corrupt));
    }

    [Fact]
    public void FailedBatchRollsBackEveryCategory()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using (SqliteStorage storage =
               CreateStorage(directory))
        {
            storage.SetHardState(
                new HardState
                {
                    Term = 1,
                    Vote = 1,
                });
            storage.Append(
                Entries((1, 1)));
            storage.SetHardState(
                new HardState
                {
                    Term = 1,
                    Vote = 1,
                    Commit = 1,
                });
            SqliteConnection connection =
                GetConnection(storage);
            using (SqliteCommand trigger =
                   connection.CreateCommand())
            {
                trigger.CommandText =
                    """
                    CREATE TRIGGER fail_entry_insert
                    BEFORE INSERT ON raft_entries
                    BEGIN
                        SELECT RAISE(ABORT, 'injected');
                    END;
                    """;
                trigger.ExecuteNonQuery();
            }

            var request = new Message
            {
                Type =
                    MessageType.MsgStorageAppend,
                From = 1,
                To =
                    RaftLocalMessageTargets
                        .AppendThread,
                Term = 3,
                Vote = 1,
                Commit = 3,
                Snapshot = SnapshotAt(
                    2,
                    2,
                    data: "new"),
            };
            request.Entries.Add(
                Entries((3, 3)));

            StorageException failure =
                Assert.Throws<StorageException>(
                    () => storage
                        .PersistStorageAppend(request));
            Assert.Equal(
                StorageError.Unknown,
                failure.Error);
            StorageException future =
                Assert.Throws<StorageException>(
                    () => storage.GetLastIndex());
            Assert.Same(failure, future);
            StorageException emptyAppend =
                Assert.Throws<StorageException>(
                    () => storage.Append([]));
            Assert.Same(failure, emptyAppend);
        }

        using (SqliteConnection connection =
               OpenRaw(directory.DatabasePath))
        {
            using SqliteCommand drop =
                connection.CreateCommand();
            drop.CommandText =
                "DROP TRIGGER fail_entry_insert;";
            drop.ExecuteNonQuery();
        }

        using SqliteStorage reopened =
            CreateStorage(directory);
        Assert.Equal(1UL, reopened.GetLastIndex());
        Assert.Equal(1UL, reopened.GetTerm(1));
        Assert.Equal(
            1UL,
            reopened.GetInitialState()
                .HardState?.Term);
        Assert.Equal(
            0UL,
            reopened.GetSnapshot()
                .Metadata.Index);
        Assert.Null(
            reopened.GetPendingApplicationSnapshot());
    }

    [Fact]
    public void OpenRejectsIndexesWithoutSuccessors()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using (SqliteStorage storage =
               CreateStorage(directory))
        {
        }

        using (SqliteConnection connection =
               OpenRaw(directory.DatabasePath))
        {
            using SqliteTransaction transaction =
                connection.BeginTransaction(
                    deferred: false);
            using (SqliteCommand delete =
                   connection.CreateCommand())
            {
                delete.Transaction = transaction;
                delete.CommandText =
                    "DELETE FROM raft_entries;";
                delete.ExecuteNonQuery();
            }

            byte[] maximum =
                EncodeIndex(ulong.MaxValue);
            using (SqliteCommand insert =
                   connection.CreateCommand())
            {
                insert.Transaction = transaction;
                insert.CommandText =
                    """
                    INSERT INTO raft_entries (
                        entry_index,
                        entry_term,
                        payload)
                    VALUES ($index, $term, $payload);
                    """;
                insert.Parameters.AddWithValue(
                    "$index",
                    maximum);
                insert.Parameters.AddWithValue(
                    "$term",
                    EncodeIndex(1));
                insert.Parameters.AddWithValue(
                    "$payload",
                    new Entry
                    {
                        Index = ulong.MaxValue,
                        Term = 1,
                    }.ToByteArray());
                insert.ExecuteNonQuery();
            }

            using (SqliteCommand update =
                   connection.CreateCommand())
            {
                update.Transaction = transaction;
                update.CommandText =
                    """
                    UPDATE raft_metadata
                    SET compacted_index = $index,
                        last_index = $index
                    WHERE singleton = 1;
                    """;
                update.Parameters.AddWithValue(
                    "$index",
                    maximum);
                update.ExecuteNonQuery();
            }

            transaction.Commit();
        }

        AssertStorageError(
            StorageError.Unknown,
            () => _ = CreateStorage(directory));
    }

    [Theory]
    [InlineData("committed")]
    [InlineData("uncommitted")]
    public async Task AbruptProcessTerminationRecoversAtomically(
        string mode)
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        if (mode == "uncommitted")
        {
            using SqliteStorage initialize =
                CreateStorage(directory);
        }

        await RunCrashHarness(
            mode,
            directory.DatabasePath);

        using SqliteStorage reopened =
            CreateStorage(directory);
        StorageState state =
            reopened.GetInitialState();
        if (mode == "committed")
        {
            Assert.Equal(7UL, state.HardState?.Term);
            Assert.Equal(1UL, reopened.GetLastIndex());
            Assert.Equal(7UL, reopened.GetTerm(1));
        }
        else
        {
            Assert.Null(state.HardState);
            Assert.Equal(0UL, reopened.GetLastIndex());
        }
    }

    [Fact]
    public void ConstructorRejectsNonFilePaths()
    {
        Assert.Throws<ArgumentException>(
            () => _ = new SqliteStorage(
                ":memory:"));
        Assert.Throws<ArgumentException>(
            () => _ = new SqliteStorage(
                "file:///tmp/raft.db"));
        Assert.Throws<ArgumentException>(
            () => _ = new SqliteStorage(
                " "));
    }

    [Fact]
    public void SmallMetadataWriteDoesNotReserializeSnapshot()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateStorage(directory);
        storage.SetHardState(
            new HardState
            {
                Term = 1,
            });
        storage.Append(
            Entries((1, 1)));
        storage.SetHardState(
            new HardState
            {
                Term = 1,
                Commit = 1,
            });
        storage.CreateSnapshot(
            1,
            new ConfState(),
            ByteString.CopyFrom(
                new byte[8 * 1024 * 1024]));

        long before =
            GC.GetAllocatedBytesForCurrentThread();
        storage.SetHardState(
            new HardState
            {
                Term = 2,
                Commit = 1,
            });
        long allocated =
            GC.GetAllocatedBytesForCurrentThread()
            - before;

        Assert.InRange(
            allocated,
            0,
            1024 * 1024);
    }

    [Fact]
    public void EmptyAppendChecksDisposedState()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        var storage = CreateStorage(directory);
        Ready emptyReady = RawNode.Restart(
                new RaftConfig
                {
                    Id = 1,
                    ElectionTick = 10,
                    HeartbeatTick = 1,
                    Storage = new MemoryStorage(),
                })
            .Ready();
        storage.Dispose();

        Assert.Throws<ObjectDisposedException>(
            () => storage.Append([]));
        Assert.Throws<ObjectDisposedException>(
            () => storage.PersistReady(
                emptyReady));
    }

    private static Ready BootstrapReady()
    {
        var memory = new MemoryStorage();
        return RawNode.Start(
                new RaftConfig
                {
                    Id = 1,
                    ElectionTick = 10,
                    HeartbeatTick = 1,
                    Storage = memory,
                },
                [new Peer(1)])
            .Ready();
    }

    private static SqliteConnection GetConnection(
        SqliteStorage storage)
    {
        FieldInfo field =
            typeof(SqliteStorage).GetField(
                "_connection",
                BindingFlags.Instance
                | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException(
                "SqliteStorage connection field is missing.");
        return Assert.IsType<SqliteConnection>(
            field.GetValue(storage));
    }

    private static T Scalar<T>(
        SqliteConnection connection,
        string sql)
    {
        using SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = sql;
        return (T)command.ExecuteScalar()!;
    }

    private static async Task RunCrashHarness(
        string mode,
        string databasePath)
    {
        string assembly =
            typeof(CrashHarnessMarker)
                .Assembly.Location;
        var start = new ProcessStartInfo(
            "dotnet")
        {
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
        };
        start.ArgumentList.Add(assembly);
        start.ArgumentList.Add(mode);
        start.ArgumentList.Add(databasePath);

        using Process process =
            Process.Start(start)
            ?? throw new InvalidOperationException(
                "Failed to start crash harness.");
        Task<string> output =
            process.StandardOutput.ReadToEndAsync();
        Task<string> error =
            process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync()
            .WaitAsync(TimeSpan.FromSeconds(10));
        string standardOutput = await output;
        _ = await error;
        Assert.NotEqual(0, process.ExitCode);
        Assert.Contains(
            mode == "committed"
                ? "COMMIT_RETURNED"
                : "UNCOMMITTED_MUTATION_EXECUTED",
            standardOutput,
            StringComparison.Ordinal);
    }
}
