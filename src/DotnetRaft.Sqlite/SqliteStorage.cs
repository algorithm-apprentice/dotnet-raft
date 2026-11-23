using DotnetRaft.Core;
using DotnetRaft.Protobuf;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Microsoft.Data.Sqlite;

namespace DotnetRaft.Storage.Sqlite;

public sealed class SqliteStorage : IStorage, IDisposable
{
    public IRaftLogFormat LogFormat => ProtobufRaftCodec.Instance;

    private static readonly object
        OwnershipGate = new();
    private static readonly HashSet<string>
        OwnedPaths = new(
            OperatingSystem.IsWindows()
                ? StringComparer.OrdinalIgnoreCase
                : StringComparer.Ordinal);

    private readonly object _gate = new();
    private readonly SqliteConnection _connection;
    private Metadata _metadata = null!;
    private StorageException? _terminalFailure;
    private bool _disposed;
    private bool _ownsPath;

    public SqliteStorage(string databasePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            databasePath);
        DatabasePath = NormalizePath(databasePath);
        string? directory =
            Path.GetDirectoryName(DatabasePath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var builder =
            new SqliteConnectionStringBuilder
            {
                DataSource = DatabasePath,
                Mode =
                    SqliteOpenMode.ReadWriteCreate,
                Cache = SqliteCacheMode.Private,
                Pooling = false,
                DefaultTimeout = 30,
            };
        _connection =
            new SqliteConnection(
                builder.ToString());
        try
        {
            ClaimPath(DatabasePath);
            _ownsPath = true;
            _connection.Open();
            ConfigureConnection();
            InitializeOrValidate();
            AcquireExclusiveLock();
        }
        catch (Exception exception)
        {
            _connection.Dispose();
            ReleasePath();
            if (exception is StorageException)
            {
                throw;
            }

            throw SqliteStorageCodec.Unknown(
                $"Failed to open SQLite Raft storage at '{DatabasePath}'.",
                exception);
        }
    }

    public string DatabasePath { get; }

    public StorageState GetInitialState()
    {
        return Execute(
            "read initial state",
            () =>
            {
                Metadata metadata =
                    _metadata;
                if (metadata.PendingSnapshotIndex
                    is not null)
                {
                    throw new InvalidOperationException(
                        $"Application snapshot {metadata.PendingSnapshotIndex.Value} must be restored and acknowledged before starting Raft.");
                }

                return new StorageState(
                    metadata.HardState,
                    metadata.Snapshot.Metadata
                        .ConfState.Clone());
            });
    }

    public IReadOnlyList<Entry> GetEntries(
        ulong lowInclusive,
        ulong highExclusive,
        ulong maxSize)
    {
        return Execute(
            "read entries",
            () =>
            {
                Metadata metadata =
                    _metadata;
                if (lowInclusive
                    <= metadata.CompactedIndex)
                {
                    throw new StorageException(
                        StorageError.Compacted,
                        $"Entry index {lowInclusive} has been compacted through {metadata.CompactedIndex}.");
                }

                if (highExclusive < lowInclusive)
                {
                    throw new InvalidOperationException(
                        $"Entry range [{lowInclusive}, {highExclusive}) is reversed.");
                }

                if ((highExclusive - 1)
                    > metadata.LastIndex)
                {
                    throw new InvalidOperationException(
                        $"Entry range [{lowInclusive}, {highExclusive}) exceeds last index {metadata.LastIndex}.");
                }

                if (metadata.CompactedIndex
                    == metadata.LastIndex)
                {
                    throw new StorageException(
                        StorageError.Unavailable,
                        "No log entries are available.");
                }

                using SqliteCommand command =
                    CreateCommand(
                        """
                        SELECT entry_index, entry_term, length(payload), payload
                        FROM raft_entries
                        WHERE entry_index >= $low
                          AND entry_index < $high
                        ORDER BY entry_index;
                        """);
                AddBlob(
                    command,
                    "$low",
                    SqliteStorageCodec.EncodeUInt64(
                        lowInclusive));
                AddBlob(
                    command,
                    "$high",
                    SqliteStorageCodec.EncodeUInt64(
                        highExclusive));
                using SqliteDataReader reader =
                    command.ExecuteReader();
                var entries = new List<Entry>();
                ulong expected = lowInclusive;
                ulong size = 0;
                while (reader.Read())
                {
                    ulong index =
                        SqliteStorageCodec.DecodeUInt64(
                            reader.GetFieldValue<byte[]>(0),
                            "Entry index");
                    if (index != expected)
                    {
                        throw SqliteStorageCodec.Unknown(
                            $"Entry range is discontinuous at index {expected}; found {index}.");
                    }

                    // Payloads are written as the protobuf encoding. Check
                    // their byte length before materializing the next BLOB.
                    ulong nextSize = checked((ulong)reader.GetInt64(2));
                    if (entries.Count != 0
                        && !EntrySizing.FitsWithinBudget(size, nextSize, maxSize))
                    {
                        return entries;
                    }

                    ulong term =
                        SqliteStorageCodec.DecodeUInt64(
                            reader.GetFieldValue<byte[]>(1),
                            "Entry term");
                    entries.Add(
                        SqliteStorageCodec.ParseEntry(
                            reader.GetFieldValue<byte[]>(3),
                            index,
                            term,
                            $"Entry {index}"));
                    size += nextSize;
                    expected++;
                }

                if (expected != highExclusive)
                {
                    throw SqliteStorageCodec.Unknown(
                        $"Entry range [{lowInclusive}, {highExclusive}) ended at {expected}.");
                }

                return entries;
            });
    }

    public ulong GetTerm(ulong index)
    {
        return Execute(
            "read term",
            () =>
            {
                Metadata metadata =
                    _metadata;
                if (index < metadata.CompactedIndex)
                {
                    throw new StorageException(
                        StorageError.Compacted,
                        $"Entry index {index} has been compacted through {metadata.CompactedIndex}.");
                }

                if (index > metadata.LastIndex)
                {
                    throw new StorageException(
                        StorageError.Unavailable,
                        $"Entry index {index} is unavailable.");
                }

                return ReadEntryTerm(index);
            });
    }

    public ulong GetLastIndex()
    {
        return Execute(
            "read last index",
            () => _metadata.LastIndex);
    }

    public ulong GetFirstIndex()
    {
        return Execute(
            "read first index",
            () => checked(
                _metadata.CompactedIndex + 1));
    }

    public Snapshot GetSnapshot()
    {
        return Execute(
            "read snapshot",
            () => _metadata.Snapshot.Clone());
    }

    public RaftHardState? GetHardState()
    {
        return Execute(
            "read hard state",
            () => _metadata.HardState);
    }

    public Snapshot? GetPendingApplicationSnapshot()
    {
        return Execute(
            "read pending application snapshot",
            () =>
            {
                Metadata metadata =
                    _metadata;
                return metadata.PendingSnapshotIndex
                    is null
                    ? null
                    : metadata.Snapshot.Clone();
            });
    }

    public void PersistReady(Ready ready)
    {
        ArgumentNullException.ThrowIfNull(ready);
        Snapshot? snapshot =
            ready.Snapshot is null
                ? null
                : SqliteStorageCodec
                    .NormalizeSnapshot(
                        ready.Snapshot);
        List<Entry> entries =
            SqliteStorageCodec
                .CloneAndValidateEntries(
                    ready.Entries);
        RaftHardState? hardState = ready.HardState;
        if (snapshot is null
            && entries.Count == 0
            && hardState is null)
        {
            EnsureAvailable();
            return;
        }

        ExecuteWrite(
            "persist Ready",
            metadata =>
                PersistBatch(
                    metadata,
                    snapshot,
                    entries,
                    hardState,
                    requireNoPending: true,
                    validateCommitBounds: true));
    }

    public void PersistStorageAppend(StorageAppendWork work)
    {
        ArgumentNullException.ThrowIfNull(work);
        PersistStorageAppendBatch(work.Snapshot, work.Entries, work.HardState);
    }

    /// <summary>Reads a legacy local envelope at the storage adapter boundary.</summary>
    public void PersistStorageAppend(Message request)
    {
        ArgumentNullException.ThrowIfNull(request);
        Message owned = request.Clone();
        RaftHardState? hardState = ValidateStorageAppend(owned);
        PersistStorageAppendBatch(owned.Snapshot, owned.Entries, hardState);
    }

    private void PersistStorageAppendBatch(Snapshot? incoming, IEnumerable<Entry> work, RaftHardState? hardState)
    {
        Snapshot? snapshot =
            incoming is null
                ? null
                : SqliteStorageCodec
                    .NormalizeSnapshot(
                        incoming);
        List<Entry> entries =
            SqliteStorageCodec
                .CloneAndValidateEntries(
                    work);
        ExecuteWrite(
            "persist storage append",
            metadata =>
                PersistBatch(
                    metadata,
                    snapshot,
                    entries,
                    hardState,
                    requireNoPending: true,
                    validateCommitBounds: true));
    }

    public void AcknowledgeApplicationSnapshot(
        ulong index)
    {
        ExecuteWrite(
            "acknowledge application snapshot",
            metadata =>
            {
                if (metadata.PendingSnapshotIndex
                    is null)
                {
                    throw new InvalidOperationException(
                        "No application snapshot restore is pending.");
                }

                if (metadata.PendingSnapshotIndex.Value
                    != index)
                {
                    throw new InvalidOperationException(
                        $"Pending application snapshot index is {metadata.PendingSnapshotIndex.Value}, not {index}.");
                }

                if (metadata.HardState is not { } state
                    || state.Commit
                        < index
                    || state.Commit
                        > metadata.LastIndex)
                {
                    throw new InvalidOperationException(
                        $"HardState commit must cover pending snapshot {index} before acknowledgement.");
                }

                return metadata with
                {
                    PendingSnapshotIndex = null,
                };
            },
            allowPending: true);
    }

    public void SetHardState(RaftHardState state)
    {
        ExecuteWrite(
            "set hard state",
            metadata =>
            {
                ValidateHardStateTransition(
                    metadata.HardState,
                    state);
                if (state.Commit
                    > metadata.LastIndex)
                {
                    throw new InvalidOperationException(
                        $"HardState commit {state.Commit} exceeds last index {metadata.LastIndex}.");
                }

                if (metadata.PendingSnapshotIndex
                        is ulong pending
                    && state.Commit < pending)
                {
                    throw new InvalidOperationException(
                        $"HardState commit {state.Commit} does not cover pending snapshot {pending}.");
                }

                return metadata with
                {
                    HardState = state,
                };
            },
            allowPending: true);
    }

    public void ApplySnapshot(Snapshot snapshot)
    {
        Snapshot owned =
            SqliteStorageCodec.NormalizeSnapshot(
                snapshot);
        SqliteStorageCodec.EnsureHasSuccessor(
            owned.Metadata.Index,
            "Snapshot");
        ExecuteWrite(
            "apply snapshot",
            metadata =>
            {
                if (!IsBootstrapSnapshot(owned)
                    && (metadata.HardState is not { } state
                        || state.Term
                            < owned.Metadata.Term))
                {
                    throw new InvalidOperationException(
                        $"HardState term must cover incoming snapshot term {owned.Metadata.Term}.");
                }

                Metadata updated =
                    ApplySnapshotCore(
                        metadata,
                        owned);
                if (updated.HardState is { } updatedState
                    && updatedState.Commit
                        > updated.LastIndex)
                {
                    throw new InvalidOperationException(
                        $"HardState commit {updatedState.Commit} exceeds applied snapshot index {updated.LastIndex}.");
                }

                return updated;
            });
    }

    public Snapshot CreateSnapshot(
        ulong index,
        ConfState? confState,
        RaftBytes data)
    {
        ConfState? ownedConf =
            confState?.Clone();
        Snapshot? result = null;
        ExecuteWrite(
            "create snapshot",
            metadata =>
            {
                ulong current =
                    metadata.Snapshot.Metadata.Index;
                if (index <= current)
                {
                    throw new StorageException(
                        StorageError.SnapshotOutOfDate,
                        $"Snapshot index {index} is not newer than current index {current}.");
                }

                if (index
                    < metadata.CompactedIndex)
                {
                    throw new InvalidOperationException(
                        $"Snapshot index {index} precedes the retained term at index {metadata.CompactedIndex}.");
                }

                if (index > metadata.LastIndex)
                {
                    throw new InvalidOperationException(
                        $"Snapshot index {index} exceeds last index {metadata.LastIndex}.");
                }

                ulong term = ReadEntryTerm(index);
                Snapshot next =
                    metadata.Snapshot.Clone();
                next.Metadata.Index = index;
                next.Metadata.Term = term;
                if (ownedConf is not null)
                {
                    next.Metadata.ConfState =
                        ownedConf.Clone();
                }

                next.Data = data;
                result = next.Clone();
                return metadata with
                {
                    Snapshot = next,
                };
            });
        return result
            ?? throw new InvalidOperationException(
                "Snapshot creation did not produce a result.");
    }

    public void Compact(ulong compactIndex)
    {
        ExecuteWrite(
            "compact log",
            metadata =>
            {
                if (compactIndex
                    <= metadata.CompactedIndex)
                {
                    throw new StorageException(
                        StorageError.Compacted,
                        $"Cannot compact index {compactIndex}; storage is already compacted through {metadata.CompactedIndex}.");
                }

                if (compactIndex
                    > metadata.LastIndex)
                {
                    throw new InvalidOperationException(
                        $"Compact index {compactIndex} exceeds last index {metadata.LastIndex}.");
                }

                ulong term =
                    ReadEntryTerm(compactIndex);
                using (SqliteCommand delete =
                       CreateCommand(
                           """
                           DELETE FROM raft_entries
                           WHERE entry_index < $index;
                           """,
                           CurrentTransaction))
                {
                    AddBlob(
                        delete,
                        "$index",
                        SqliteStorageCodec
                            .EncodeUInt64(
                                compactIndex));
                    delete.ExecuteNonQuery();
                }

                WriteEntry(
                    new Entry
                    {
                        Index = compactIndex,
                        Term = term,
                    },
                    replace: true);
                return metadata with
                {
                    CompactedIndex = compactIndex,
                };
            });
    }

    public void Append(IEnumerable<Entry> entries)
    {
        List<Entry> owned =
            SqliteStorageCodec
                .CloneAndValidateEntries(entries);
        if (owned.Count == 0)
        {
            EnsureAvailable();
            return;
        }

        SqliteStorageCodec.EnsureHasSuccessor(
            owned[^1].Index,
            "Final appended entry");
        ExecuteWrite(
            "append entries",
            metadata => AppendCore(
                metadata,
                owned));
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _connection.Dispose();
            ReleasePath();
        }
    }

    private SqliteTransaction? CurrentTransaction
    {
        get;
        set;
    }

    private static string NormalizePath(
        string databasePath)
    {
        if (databasePath.Contains(
                "://",
                StringComparison.Ordinal)
            || string.Equals(
                databasePath,
                ":memory:",
                StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "SQLite Raft storage requires a local file path.",
                nameof(databasePath));
        }

        string full =
            Path.GetFullPath(databasePath);
        var file = new FileInfo(full);
        if (file.Exists
            && file.LinkTarget is not null)
        {
            FileSystemInfo target =
                file.ResolveLinkTarget(
                    returnFinalTarget: true)
                ?? throw new IOException(
                    $"Cannot resolve symbolic link '{full}'.");
            full = Path.GetFullPath(
                target.FullName);
        }

        return full;
    }

    private static void ClaimPath(string path)
    {
        lock (OwnershipGate)
        {
            if (!OwnedPaths.Add(path))
            {
                throw SqliteStorageCodec.Unknown(
                    $"SQLite Raft storage '{path}' is already open in this process.");
            }
        }
    }

    private void ReleasePath()
    {
        if (!_ownsPath)
        {
            return;
        }

        lock (OwnershipGate)
        {
            OwnedPaths.Remove(DatabasePath);
            _ownsPath = false;
        }
    }

    private void ConfigureConnection()
    {
        RequirePragmaText(
            "PRAGMA locking_mode = EXCLUSIVE;",
            "exclusive");
        RequirePragmaText(
            "PRAGMA journal_mode = WAL;",
            "wal");
        ExecuteNonQuery(
            "PRAGMA synchronous = FULL;");
        RequirePragmaLong(
            "PRAGMA synchronous;",
            2);
        ExecuteNonQuery(
            "PRAGMA fullfsync = ON;");
        RequirePragmaLong(
            "PRAGMA fullfsync;",
            1);
        ExecuteNonQuery(
            "PRAGMA checkpoint_fullfsync = ON;");
        RequirePragmaLong(
            "PRAGMA checkpoint_fullfsync;",
            1);
        ExecuteNonQuery(
            "PRAGMA busy_timeout = 30000;");
        ExecuteNonQuery(
            "PRAGMA trusted_schema = OFF;");
    }

    private void InitializeOrValidate()
    {
        long version =
            Scalar<long>(
                "PRAGMA user_version;");
        List<SchemaObject> objects =
            ReadSchemaObjects();
        if (version == 0)
        {
            if (objects.Count != 0)
            {
                throw SqliteStorageCodec.Unknown(
                    "SQLite database is not an empty DotnetRaft storage file.");
            }

            InitializeSchema();
        }
        else if (version
                 != SqliteStorageSchema.Version)
        {
            throw SqliteStorageCodec.Unknown(
                $"SQLite Raft schema version {version} is unsupported.");
        }

        ValidateSchema();
        _metadata = ReadMetadataFromDatabase();
        ValidateMetadata(_metadata);
    }

    private void InitializeSchema()
    {
        using SqliteTransaction transaction =
            _connection.BeginTransaction(
                deferred: false);
        using (SqliteCommand metadata =
               CreateCommand(
                   SqliteStorageSchema
                       .MetadataTableSql,
                   transaction))
        {
            metadata.ExecuteNonQuery();
        }

        using (SqliteCommand entries =
               CreateCommand(
                   SqliteStorageSchema
                       .EntriesTableSql,
                   transaction))
        {
            entries.ExecuteNonQuery();
        }

        Snapshot snapshot =
            SqliteStorageCodec.NormalizeSnapshot(
                new Snapshot());
        using (SqliteCommand insert =
               CreateCommand(
                   """
                   INSERT INTO raft_metadata (
                       singleton,
                       format,
                       schema_version,
                       hard_state,
                       snapshot,
                       compacted_index,
                       last_index,
                       pending_snapshot_index)
                   VALUES (
                       1,
                       $format,
                       $schemaVersion,
                       NULL,
                       $snapshot,
                       $zero,
                       $zero,
                       NULL);
                   """,
                   transaction))
        {
            insert.Parameters.AddWithValue(
                "$format",
                SqliteStorageSchema.Format);
            insert.Parameters.AddWithValue(
                "$schemaVersion",
                SqliteStorageSchema.Version);
            AddBlob(
                insert,
                "$snapshot",
                ProtobufRaftCodec.EncodeSnapshot(snapshot));
            AddBlob(
                insert,
                "$zero",
                SqliteStorageCodec
                    .EncodeUInt64(0));
            insert.ExecuteNonQuery();
        }

        using (SqliteCommand entry =
               CreateCommand(
                   """
                   INSERT INTO raft_entries (
                       entry_index,
                       entry_term,
                       payload)
                   VALUES ($index, $term, $payload);
                   """,
                   transaction))
        {
            AddBlob(
                entry,
                "$index",
                SqliteStorageCodec
                    .EncodeUInt64(0));
            AddBlob(
                entry,
                "$term",
                SqliteStorageCodec
                    .EncodeUInt64(0));
            AddBlob(
                entry,
                "$payload",
                ProtobufRaftCodec.EncodeEntry(new Entry()));
            entry.ExecuteNonQuery();
        }

        using (SqliteCommand version =
               CreateCommand(
                   $"PRAGMA user_version = {SqliteStorageSchema.Version};",
                   transaction))
        {
            version.ExecuteNonQuery();
        }

        transaction.Commit();
    }

    private void ValidateSchema()
    {
        List<SchemaObject> objects =
            ReadSchemaObjects();
        SchemaObject[] expected =
        [
            new(
                "table",
                "raft_entries",
                "raft_entries",
                SqliteStorageSchema
                    .EntriesTableSql),
            new(
                "table",
                "raft_metadata",
                "raft_metadata",
                SqliteStorageSchema
                    .MetadataTableSql),
        ];
        if (objects.Count != expected.Length)
        {
            throw SqliteStorageCodec.Unknown(
                "SQLite Raft schema contains unexpected objects.");
        }

        for (var index = 0;
             index < expected.Length;
             index++)
        {
            SchemaObject actual = objects[index];
            SchemaObject wanted = expected[index];
            if (!string.Equals(
                    actual.Type,
                    wanted.Type,
                    StringComparison.Ordinal)
                || !string.Equals(
                    actual.Name,
                    wanted.Name,
                    StringComparison.Ordinal)
                || !string.Equals(
                    actual.TableName,
                    wanted.TableName,
                    StringComparison.Ordinal)
                || !string.Equals(
                    SqliteStorageSchema.NormalizeSql(
                        actual.Sql),
                    SqliteStorageSchema.NormalizeSql(
                        wanted.Sql),
                    StringComparison.OrdinalIgnoreCase))
            {
                throw SqliteStorageCodec.Unknown(
                    $"SQLite Raft schema object '{actual.Name}' does not match schema version {SqliteStorageSchema.Version}.");
            }
        }
    }

    private void AcquireExclusiveLock()
    {
        using SqliteTransaction transaction =
            _connection.BeginTransaction(
                deferred: false);
        using SqliteCommand command =
            CreateCommand(
                """
                UPDATE raft_metadata
                SET format = format
                WHERE singleton = 1;
                """,
                transaction);
        command.ExecuteNonQuery();
        transaction.Commit();
    }

    private List<SchemaObject> ReadSchemaObjects()
    {
        using SqliteCommand command =
            CreateCommand(
                """
                SELECT type, name, tbl_name, sql
                FROM sqlite_schema
                WHERE name NOT LIKE 'sqlite_%'
                ORDER BY type, name;
                """);
        using SqliteDataReader reader =
            command.ExecuteReader();
        var result = new List<SchemaObject>();
        while (reader.Read())
        {
            result.Add(
                new SchemaObject(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.IsDBNull(3)
                        ? string.Empty
                        : reader.GetString(3)));
        }

        return result;
    }

    private Metadata ReadMetadataFromDatabase()
    {
        using SqliteCommand command =
            CreateCommand(
                """
                SELECT
                    format,
                    schema_version,
                    hard_state,
                    snapshot,
                    compacted_index,
                    last_index,
                    pending_snapshot_index
                FROM raft_metadata
                WHERE singleton = 1;
                """,
                CurrentTransaction);
        using SqliteDataReader reader =
            command.ExecuteReader();
        if (!reader.Read())
        {
            throw SqliteStorageCodec.Unknown(
                "SQLite Raft metadata row is missing.");
        }

        string format = reader.GetString(0);
        long schemaVersion = reader.GetInt64(1);
        RaftHardState? hardState =
            reader.IsDBNull(2)
                ? null
                : SqliteStorageCodec
                    .ParseHardState(
                        reader.GetFieldValue<byte[]>(2),
                        "Stored hard state");
        Snapshot snapshot =
            SqliteStorageCodec.ParseSnapshot(
                reader.GetFieldValue<byte[]>(3),
                "Stored snapshot");
        ulong compacted =
            SqliteStorageCodec.DecodeUInt64(
                reader.GetFieldValue<byte[]>(4),
                "Compacted index");
        ulong last =
            SqliteStorageCodec.DecodeUInt64(
                reader.GetFieldValue<byte[]>(5),
                "Last index");
        ulong? pending =
            reader.IsDBNull(6)
                ? null
                : SqliteStorageCodec.DecodeUInt64(
                    reader.GetFieldValue<byte[]>(6),
                    "Pending snapshot index");
        if (reader.Read())
        {
            throw SqliteStorageCodec.Unknown(
                "SQLite Raft metadata contains multiple singleton rows.");
        }

        var metadata = new Metadata(
            hardState,
            snapshot,
            compacted,
            last,
            pending);
        if (!string.Equals(
                format,
                SqliteStorageSchema.Format,
                StringComparison.Ordinal)
            || schemaVersion
                != SqliteStorageSchema.Version)
        {
            throw SqliteStorageCodec.Unknown(
                "SQLite Raft metadata format or schema version is invalid.");
        }

        return metadata;
    }

    private void ValidateMetadata(Metadata metadata)
    {
        if (metadata.CompactedIndex
                == ulong.MaxValue
            || metadata.LastIndex
                == ulong.MaxValue
            || metadata.Snapshot.Metadata.Index
                == ulong.MaxValue)
        {
            throw SqliteStorageCodec.Unknown(
                "SQLite Raft metadata contains an index without a representable successor.");
        }

        if (metadata.CompactedIndex
                > metadata.LastIndex
            || metadata.Snapshot.Metadata.Index
                > metadata.LastIndex)
        {
            throw SqliteStorageCodec.Unknown(
                "SQLite Raft metadata index bounds are inconsistent.");
        }

        ulong expectedCount =
            metadata.LastIndex
            - metadata.CompactedIndex
            + 1;
        if (expectedCount > long.MaxValue)
        {
            throw SqliteStorageCodec.Unknown(
                "SQLite Raft retained entry count exceeds the supported database range.");
        }

        using (SqliteCommand count =
               CreateCommand(
                   """
                   SELECT
                       COUNT(*),
                       MIN(entry_index),
                       MAX(entry_index)
                   FROM raft_entries;
                   """,
                   CurrentTransaction))
        using (SqliteDataReader reader =
               count.ExecuteReader())
        {
            if (!reader.Read()
                || reader.GetInt64(0)
                    != (long)expectedCount
                || reader.IsDBNull(1)
                || reader.IsDBNull(2)
                || SqliteStorageCodec
                        .DecodeUInt64(
                            reader.GetFieldValue<byte[]>(1),
                            "Minimum entry index")
                    != metadata.CompactedIndex
                || SqliteStorageCodec
                        .DecodeUInt64(
                            reader.GetFieldValue<byte[]>(2),
                            "Maximum entry index")
                    != metadata.LastIndex)
            {
                throw SqliteStorageCodec.Unknown(
                    "SQLite Raft entry key range is not contiguous with metadata.");
            }
        }

        ulong boundaryTerm = ReadEntryTerm(metadata.CompactedIndex);
        ulong lastTerm = boundaryTerm;
        if (metadata.LastIndex
            != metadata.CompactedIndex)
        {
            lastTerm = ReadEntryTerm(metadata.LastIndex);
        }

        if (metadata.PendingSnapshotIndex
            is ulong pending)
        {
            if (pending
                    != metadata.Snapshot
                        .Metadata.Index
                || IsBootstrapSnapshot(
                    metadata.Snapshot))
            {
                throw SqliteStorageCodec.Unknown(
                    "SQLite Raft pending snapshot marker is invalid.");
            }
        }
        ValidateResultingState(metadata, lastTerm);
    }

    private Metadata PersistBatch(
        Metadata metadata,
        Snapshot? snapshot,
        List<Entry> entries,
        RaftHardState? hardState,
        bool requireNoPending,
        bool validateCommitBounds)
    {
        if (requireNoPending)
        {
            EnsureNoPending(metadata);
        }

        Metadata result = metadata;
        if (hardState is { } state)
        {
            ValidateHardStateTransition(metadata.HardState, state);
            result = metadata with { HardState = state };
        }
        if (snapshot is not null)
        {
            SqliteStorageCodec.EnsureHasSuccessor(
                snapshot.Metadata.Index,
                "Snapshot");
            result = ApplySnapshotCore(
                result,
                snapshot);
        }

        if (entries.Count > 0)
        {
            SqliteStorageCodec.EnsureHasSuccessor(
                entries[^1].Index,
                "Final appended entry");
            result = AppendCore(
                result,
                entries);
        }

        if (validateCommitBounds
            && result.HardState is { } resultingState
            && (resultingState.Commit
                    > result.LastIndex
                || resultingState.Commit
                    < result.Snapshot
                        .Metadata.Index))
        {
            throw new InvalidOperationException(
                $"HardState commit {resultingState.Commit} does not cover retained indexes [{result.Snapshot.Metadata.Index}, {result.LastIndex}].");
        }

        return result;
    }

    private Metadata ApplySnapshotCore(
        Metadata metadata,
        Snapshot snapshot)
    {
        ulong current =
            metadata.Snapshot.Metadata.Index;
        ulong incoming =
            snapshot.Metadata.Index;
        if (current != 0
            && current >= incoming)
        {
            throw new StorageException(
                StorageError.SnapshotOutOfDate,
                $"Snapshot index {incoming} is not newer than current index {current}.");
        }

        using (SqliteCommand delete =
               CreateCommand(
                   "DELETE FROM raft_entries;",
                   CurrentTransaction))
        {
            delete.ExecuteNonQuery();
        }

        WriteEntry(
            new Entry
            {
                Index = incoming,
                Term = snapshot.Metadata.Term,
            },
            replace: false);
        return metadata with
        {
            Snapshot = snapshot.Clone(),
            CompactedIndex = incoming,
            LastIndex = incoming,
            PendingSnapshotIndex =
                IsBootstrapSnapshot(snapshot)
                    ? null
                    : incoming,
        };
    }

    private Metadata AppendCore(
        Metadata metadata,
        List<Entry> incomingEntries)
    {
        ulong firstIndex = checked(
            metadata.CompactedIndex + 1);
        ulong incomingLast =
            incomingEntries[^1].Index;
        if (incomingLast < firstIndex)
        {
            return metadata;
        }

        var entries = incomingEntries;
        if (firstIndex > entries[0].Index)
        {
            int compactedCount = checked(
                (int)(firstIndex
                      - entries[0].Index));
            entries = entries.GetRange(
                compactedCount,
                entries.Count - compactedCount);
        }

        ulong offset =
            entries[0].Index
            - metadata.CompactedIndex;
        ulong retainedCount =
            metadata.LastIndex
            - metadata.CompactedIndex
            + 1;
        if (retainedCount > offset)
        {
            using SqliteCommand delete =
                CreateCommand(
                    """
                    DELETE FROM raft_entries
                    WHERE entry_index >= $index;
                    """,
                    CurrentTransaction);
            AddBlob(
                delete,
                "$index",
                SqliteStorageCodec.EncodeUInt64(
                    entries[0].Index));
            delete.ExecuteNonQuery();
        }
        else if (retainedCount < offset)
        {
            throw new InvalidOperationException(
                $"Missing log entry between last index {metadata.LastIndex} and append index {entries[0].Index}.");
        }

        foreach (Entry entry in entries)
        {
            WriteEntry(
                entry,
                replace: false);
        }

        return metadata with
        {
            LastIndex = entries[^1].Index,
        };
    }

    private ulong ReadEntryTerm(ulong index)
    {
        using SqliteCommand command =
            CreateCommand(
                """
                SELECT entry_term, payload
                FROM raft_entries
                WHERE entry_index = $index;
                """,
                CurrentTransaction);
        AddBlob(
            command,
            "$index",
            SqliteStorageCodec.EncodeUInt64(
                index));
        using SqliteDataReader reader =
            command.ExecuteReader();
        if (!reader.Read())
        {
            throw SqliteStorageCodec.Unknown(
                $"Stored entry {index} is missing.");
        }

        ulong term =
            SqliteStorageCodec.DecodeUInt64(
                reader.GetFieldValue<byte[]>(0),
                $"Entry {index} term");
        ulong parsedTerm =
            SqliteStorageCodec.ReadEntryTerm(
                reader.GetFieldValue<byte[]>(1),
                index,
                term,
                $"Entry {index}");
        if (reader.Read())
        {
            throw SqliteStorageCodec.Unknown(
                $"Stored entry {index} is duplicated.");
        }

        return parsedTerm;
    }

    private void WriteEntry(
        Entry entry,
        bool replace)
    {
        using SqliteCommand command =
            CreateCommand(
                replace
                    ? """
                      INSERT OR REPLACE INTO raft_entries (
                          entry_index,
                          entry_term,
                          payload)
                      VALUES ($index, $term, $payload);
                      """
                    : """
                      INSERT INTO raft_entries (
                          entry_index,
                          entry_term,
                          payload)
                      VALUES ($index, $term, $payload);
                      """,
                CurrentTransaction);
        AddBlob(
            command,
            "$index",
            SqliteStorageCodec.EncodeUInt64(
                entry.Index));
        AddBlob(
            command,
            "$term",
            SqliteStorageCodec.EncodeUInt64(
                entry.Term));
        AddBlob(
            command,
            "$payload",
            ProtobufRaftCodec.EncodeEntry(entry));
        command.ExecuteNonQuery();
    }

    private static RaftHardState? ValidateStorageAppend(
        Message request)
    {
        RaftHardState? hardState = StorageAppendHardState.Read(request);

        if (!request.HasFrom
            || request.From == 0
            || RaftLocalMessageTargets.IsLocal(
                request.From))
        {
            throw new ArgumentException(
                "Storage-append sender must be a nonlocal nonzero Raft node.",
                nameof(request));
        }

        if (request.HasLogTerm
            || request.HasIndex
            || request.HasReject
            || request.HasRejectHint
            || request.HasContext)
        {
            throw new ArgumentException(
                "Storage-append request contains forbidden fields.",
                nameof(request));
        }

        if (request.Snapshot is not null)
        {
            Snapshot snapshot =
                SqliteStorageCodec
                    .NormalizeSnapshot(
                        request.Snapshot);
            if (snapshot.Metadata.Index == 0)
            {
                throw new ArgumentException(
                    "Storage-append snapshot must have a nonzero index.",
                    nameof(request));
            }
        }

        foreach (Entry entry in request.Entries)
        {
            if (entry.Index == 0)
            {
                throw new ArgumentException(
                    "Storage-append entries must have nonzero indexes.",
                    nameof(request));
            }
        }

        if (hardState is null
            && request.Snapshot is null
            && request.Entries.Count == 0
            && request.Responses.Count == 0)
        {
            throw new ArgumentException(
                "Storage-append request contains no work.",
                nameof(request));
        }

        return hardState;
    }

    private void ExecuteWrite(
        string operation,
        Func<Metadata, Metadata> mutation,
        bool allowPending = false)
    {
        Execute(
            operation,
            () =>
            {
                using SqliteTransaction transaction =
                    _connection.BeginTransaction(
                        deferred: false);
                CurrentTransaction = transaction;
                try
                {
                    Metadata current =
                        _metadata;
                    if (!allowPending)
                    {
                        EnsureNoPending(current);
                    }

                    Metadata updated =
                        mutation(current);
                    ValidateResultingState(updated, ReadEntryTerm(updated.LastIndex));
                    WriteMetadata(
                        current,
                        updated);
                    transaction.Commit();
                    _metadata = updated;
                }
                finally
                {
                    CurrentTransaction = null;
                }

                return true;
            });
    }

    private void WriteMetadata(
        Metadata previous,
        Metadata metadata)
    {
        bool snapshotChanged =
            !ReferenceEquals(
                previous.Snapshot,
                metadata.Snapshot);
        using SqliteCommand command =
            CreateCommand(
                snapshotChanged
                    ? """
                      UPDATE raft_metadata
                      SET
                          hard_state = CASE WHEN $hardStateChanged THEN $hardState ELSE hard_state END,
                          snapshot = $snapshot,
                          compacted_index = $compacted,
                          last_index = $last,
                          pending_snapshot_index = $pending
                      WHERE singleton = 1;
                      """
                    : """
                      UPDATE raft_metadata
                      SET
                          hard_state = CASE WHEN $hardStateChanged THEN $hardState ELSE hard_state END,
                          compacted_index = $compacted,
                          last_index = $last,
                          pending_snapshot_index = $pending
                      WHERE singleton = 1;
                      """,
                CurrentTransaction);
        // Keep the original protobuf bytes when no state value changed.
        bool hardStateChanged = previous.HardState != metadata.HardState;
        command.Parameters.AddWithValue("$hardStateChanged", hardStateChanged);
        AddNullableBlob(
            command,
            "$hardState",
            hardStateChanged && metadata.HardState is { } state
                ? ProtobufRaftCodec.EncodeHardState(state)
                : null);
        if (snapshotChanged)
        {
            AddBlob(
                command,
                "$snapshot",
                ProtobufRaftCodec.EncodeSnapshot(metadata.Snapshot));
        }
        AddBlob(
            command,
            "$compacted",
            SqliteStorageCodec.EncodeUInt64(
                metadata.CompactedIndex));
        AddBlob(
            command,
            "$last",
            SqliteStorageCodec.EncodeUInt64(
                metadata.LastIndex));
        AddNullableBlob(
            command,
            "$pending",
            metadata.PendingSnapshotIndex
                is ulong pending
                ? SqliteStorageCodec
                    .EncodeUInt64(pending)
                : null);
        if (command.ExecuteNonQuery() != 1)
        {
            throw SqliteStorageCodec.Unknown(
                "SQLite Raft metadata update did not affect exactly one row.");
        }
    }

    private static void EnsureNoPending(
        Metadata metadata)
    {
        if (metadata.PendingSnapshotIndex
            is ulong pending)
        {
            throw new InvalidOperationException(
                $"Application snapshot {pending} must be restored and acknowledged before mutating Raft storage.");
        }
    }

    private static void ValidateResultingState(
        Metadata metadata,
        ulong lastTerm)
    {
        ulong requiredTerm = Math.Max(
            metadata.Snapshot.Metadata.Term,
            lastTerm);
        if (metadata.HardState is not { } hardState)
        {
            if (requiredTerm != 0
                || metadata.Snapshot.Metadata.Index
                    != 0
                || metadata.LastIndex != 0)
            {
                throw new InvalidOperationException(
                    "Nonempty durable Raft state requires HardState.");
            }

            return;
        }

        if (hardState.Term
            < requiredTerm)
        {
            throw new InvalidOperationException(
                $"HardState term {hardState.Term} is below retained term {requiredTerm}.");
        }

        if (hardState.Commit
            > metadata.LastIndex)
        {
            throw new InvalidOperationException(
                $"HardState commit {hardState.Commit} exceeds last index {metadata.LastIndex}.");
        }

        if (metadata.PendingSnapshotIndex
                is null
            && hardState.Commit
                < metadata.Snapshot.Metadata.Index)
        {
            throw new InvalidOperationException(
                $"HardState commit {hardState.Commit} does not cover snapshot index {metadata.Snapshot.Metadata.Index}.");
        }
    }

    private static void ValidateHardStateTransition(
        RaftHardState? priorState,
        RaftHardState next)
    {
        if (priorState is not { } previous)
        {
            return;
        }

        if (next.Term < previous.Term)
        {
            throw new InvalidOperationException(
                $"HardState term cannot decrease from {previous.Term} to {next.Term}.");
        }

        if (next.Commit < previous.Commit)
        {
            throw new InvalidOperationException(
                $"HardState commit cannot decrease from {previous.Commit} to {next.Commit}.");
        }

        if (next.Term == previous.Term
            && previous.Vote != 0
            && next.Vote != previous.Vote)
        {
            throw new InvalidOperationException(
                $"HardState vote cannot change within term {next.Term}.");
        }
    }

    private static bool IsBootstrapSnapshot(
        Snapshot snapshot)
    {
        return snapshot.Metadata.Index == 0
            && snapshot.Metadata.Term == 0
            && snapshot.Data.IsEmpty;
    }

    private T Execute<T>(
        string operation,
        Func<T> action)
    {
        lock (_gate)
        {
            ThrowIfUnavailable();
            try
            {
                return action();
            }
            catch (StorageException exception)
                when (exception.Error
                      != StorageError.Unknown)
            {
                throw;
            }
            catch (ArgumentException)
            {
                throw;
            }
            catch (InvalidOperationException)
            {
                throw;
            }
            catch (OverflowException)
            {
                throw;
            }
            catch (Exception exception)
            {
                StorageException failure =
                    exception as StorageException
                    ?? SqliteStorageCodec.Unknown(
                        $"SQLite Raft storage failed to {operation}.",
                        exception);
                Fault(failure);
                throw failure;
            }
        }
    }

    private void EnsureAvailable()
    {
        lock (_gate)
        {
            ThrowIfUnavailable();
        }
    }

    private void ThrowIfUnavailable()
    {
        ObjectDisposedException.ThrowIf(
            _disposed,
            this);

        if (_terminalFailure is not null)
        {
            throw _terminalFailure;
        }
    }

    private void Fault(StorageException failure)
    {
        _terminalFailure ??= failure;
        try
        {
            _connection.Close();
        }
        catch
        {
        }

        ReleasePath();
    }

    private SqliteCommand CreateCommand(
        string sql,
        SqliteTransaction? transaction = null)
    {
        SqliteCommand command =
            _connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = 30;
        command.Transaction = transaction;
        return command;
    }

    private void ExecuteNonQuery(string sql)
    {
        using SqliteCommand command =
            CreateCommand(sql);
        command.ExecuteNonQuery();
    }

    private T Scalar<T>(string sql)
    {
        using SqliteCommand command =
            CreateCommand(sql);
        object? value = command.ExecuteScalar();
        return (T)value!;
    }

    private void RequirePragmaText(
        string setSql,
        string expected)
    {
        string actual = Scalar<string>(setSql);
        if (!string.Equals(
                actual,
                expected,
                StringComparison.OrdinalIgnoreCase))
        {
            throw SqliteStorageCodec.Unknown(
                $"SQLite pragma returned '{actual}', expected '{expected}'.");
        }
    }

    private void RequirePragmaLong(
        string querySql,
        long expected)
    {
        long actual = Scalar<long>(querySql);
        if (actual != expected)
        {
            throw SqliteStorageCodec.Unknown(
                $"SQLite pragma returned {actual}, expected {expected}.");
        }
    }

    private static void AddBlob(
        SqliteCommand command,
        string name,
        byte[] value)
    {
        SqliteParameter parameter =
            command.Parameters.Add(
                name,
                SqliteType.Blob);
        parameter.Value = value;
    }

    private static void AddNullableBlob(
        SqliteCommand command,
        string name,
        byte[]? value)
    {
        SqliteParameter parameter =
            command.Parameters.Add(
                name,
                SqliteType.Blob);
        parameter.Value =
            value is null
                ? DBNull.Value
                : value;
    }

    private sealed record Metadata(
    RaftHardState? HardState,
    Snapshot Snapshot,
    ulong CompactedIndex,
    ulong LastIndex,
    ulong? PendingSnapshotIndex);

    private sealed record SchemaObject(
        string Type,
        string Name,
        string TableName,
        string Sql);
}
