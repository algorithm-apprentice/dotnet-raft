using System.Buffers.Binary;
using System.Text.Json;
using System.Text.Json.Serialization;

using DotnetRaft.Protocol;

using Google.Protobuf;

using Microsoft.Data.Sqlite;

namespace DotnetRaft.Examples.KvCluster;

public sealed class SqliteKeyValueStateMachine
    : IDisposable
{
    private const int SchemaVersion = 1;
    private const string Format =
        "dotnet-raft-durable-kv";
    private const string MetadataTableSql =
        """
        CREATE TABLE app_metadata (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            format TEXT NOT NULL,
            schema_version INTEGER NOT NULL,
            node_id BLOB NOT NULL CHECK (length(node_id) = 8),
            physical_applied BLOB NOT NULL CHECK (length(physical_applied) = 8),
            conf_state BLOB NOT NULL
        ) STRICT
        """;
    private const string KeyValueTableSql =
        """
        CREATE TABLE kv (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) WITHOUT ROWID, STRICT
        """;
    private const string RequestsTableSql =
        """
        CREATE TABLE requests (
            request_id BLOB PRIMARY KEY CHECK (length(request_id) = 16),
            command_hash BLOB NOT NULL CHECK (length(command_hash) = 32),
            result_index BLOB NOT NULL CHECK (length(result_index) = 8)
        ) WITHOUT ROWID, STRICT
        """;

    private static readonly object OwnershipGate =
        new();
    private static readonly HashSet<string> OwnedPaths =
        new(
            OperatingSystem.IsWindows()
                ? StringComparer.OrdinalIgnoreCase
                : StringComparer.Ordinal);
    private static readonly JsonSerializerOptions
        SnapshotOptions = new()
        {
            PropertyNamingPolicy =
                JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = false,
            UnmappedMemberHandling =
                JsonUnmappedMemberHandling.Disallow,
        };

    private readonly object gate = new();
    private readonly SqliteConnection connection;
    private ConfState confState = new();
    private bool disposed;
    private bool ownsPath;
    private ulong physicalApplied;

    public SqliteKeyValueStateMachine(
        string databasePath,
        ulong nodeId,
        bool createIfMissing = true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            databasePath);
        ArgumentOutOfRangeException.ThrowIfZero(
            nodeId);

        DatabasePath = Path.GetFullPath(
            databasePath);
        NodeId = nodeId;
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
                Mode = createIfMissing
                    ? SqliteOpenMode.ReadWriteCreate
                    : SqliteOpenMode.ReadWrite,
                Cache = SqliteCacheMode.Private,
                Pooling = false,
                DefaultTimeout = 30,
            };
        connection = new SqliteConnection(
            builder.ToString());
        try
        {
            ClaimPath(DatabasePath);
            ownsPath = true;
            connection.Open();
            ConfigureConnection();
            WasCreated = InitializeOrRead();
            AcquireExclusiveLock();
        }
        catch
        {
            connection.Dispose();
            ReleasePath();
            throw;
        }
    }

    public string DatabasePath { get; }

    public ulong NodeId { get; }

    public bool WasCreated { get; }

    public ulong PhysicalApplied
    {
        get
        {
            lock (gate)
            {
                ThrowIfDisposed();
                return physicalApplied;
            }
        }
    }

    public ConfState ConfState
    {
        get
        {
            lock (gate)
            {
                ThrowIfDisposed();
                return confState.Clone();
            }
        }
    }

    public KvApplyResult ApplyCommand(
        ulong index,
        ByteString data)
    {
        ArgumentNullException.ThrowIfNull(data);
        KvCommand command =
            DurableKvCommandCodec.Decode(
                data.Span);
        ByteString fingerprint =
            DurableKvCommandCodec.Fingerprint(
                command);

        lock (gate)
        {
            ThrowIfDisposed();
            EnsureNext(index);
            using SqliteTransaction transaction =
                connection.BeginTransaction(
                    deferred: false);
            RequestRecord? existing =
                ReadRequest(
                    command.RequestId,
                    transaction);
            bool duplicate = false;
            bool conflict = false;
            ulong resultIndex;
            if (existing is null)
            {
                ApplyMutation(
                    command,
                    transaction);
                InsertRequest(
                    command.RequestId,
                    fingerprint,
                    index,
                    transaction);
                resultIndex = index;
            }
            else
            {
                resultIndex = existing.ResultIndex;
                if (existing.Fingerprint.Equals(
                        fingerprint))
                {
                    duplicate = true;
                }
                else
                {
                    conflict = true;
                }
            }

            UpdateMetadata(
                index,
                confState,
                transaction);
            transaction.Commit();
            physicalApplied = index;
            return new KvApplyResult(
                command.RequestId,
                fingerprint,
                resultIndex,
                index,
                duplicate,
                conflict);
        }
    }

    public void AdvanceNoOp(ulong index)
    {
        lock (gate)
        {
            ThrowIfDisposed();
            EnsureNext(index);
            using SqliteTransaction transaction =
                connection.BeginTransaction(
                    deferred: false);
            UpdateMetadata(
                index,
                confState,
                transaction);
            transaction.Commit();
            physicalApplied = index;
        }
    }

    public void ApplyConfiguration(
        ulong index,
        ConfState configuration)
    {
        ArgumentNullException.ThrowIfNull(
            configuration);
        ConfState owned =
            NormalizeConfState(configuration);
        lock (gate)
        {
            ThrowIfDisposed();
            EnsureNext(index);
            using SqliteTransaction transaction =
                connection.BeginTransaction(
                    deferred: false);
            UpdateMetadata(
                index,
                owned,
                transaction);
            transaction.Commit();
            confState = owned;
            physicalApplied = index;
        }
    }

    public KvRequestResolution? ResolveRequest(
        KvCommand command)
    {
        ByteString fingerprint =
            DurableKvCommandCodec.Fingerprint(
                command);
        KvApplyResult? applied =
            ResolveAppliedRequest(
                command.RequestId,
                fingerprint);
        return applied is null
            ? null
            : new KvRequestResolution(
                applied.ResultIndex,
                applied.Conflict);
    }

    public KvApplyResult? ResolveAppliedRequest(
        Guid requestId,
        ByteString expectedFingerprint)
    {
        ArgumentNullException.ThrowIfNull(
            expectedFingerprint);
        lock (gate)
        {
            ThrowIfDisposed();
            RequestRecord? existing =
                ReadRequest(
                    requestId,
                    transaction: null);
            if (existing is null)
            {
                return null;
            }

            return new KvApplyResult(
                requestId,
                existing.Fingerprint,
                existing.ResultIndex,
                physicalApplied,
                Duplicate: true,
                Conflict:
                    !existing.Fingerprint.Equals(
                        expectedFingerprint));
        }
    }

    public ByteString CreateSnapshotData()
    {
        lock (gate)
        {
            ThrowIfDisposed();
            var values =
                new List<SnapshotValue>();
            using (SqliteCommand command =
                   CreateCommand(
                       """
                       SELECT key, value
                       FROM kv;
                       """))
            using (SqliteDataReader reader =
                   command.ExecuteReader())
            {
                while (reader.Read())
                {
                    values.Add(
                        new SnapshotValue(
                            reader.GetString(0),
                            reader.GetString(1)));
                }
            }

            values.Sort(
                static (left, right) =>
                    StringComparer.Ordinal.Compare(
                        left.Key,
                        right.Key));

            var requests =
                new List<SnapshotRequest>();
            using (SqliteCommand command =
                   CreateCommand(
                       """
                       SELECT
                           request_id,
                           command_hash,
                           result_index
                       FROM requests
                       ORDER BY request_id;
                       """))
            using (SqliteDataReader reader =
                   command.ExecuteReader())
            {
                while (reader.Read())
                {
                    requests.Add(
                        new SnapshotRequest(
                            reader.GetFieldValue<
                                byte[]>(0),
                            reader.GetFieldValue<
                                byte[]>(1),
                            DecodeUInt64(
                                reader.GetFieldValue<
                                    byte[]>(2),
                                "Request result index")));
                }
            }

            var snapshot =
                new ApplicationSnapshot(
                    Version: 1,
                    PhysicalApplied:
                        physicalApplied,
                    ConfState:
                        confState.ToByteArray(),
                    Values: values,
                    Requests: requests);
            return ByteString.CopyFrom(
                JsonSerializer
                    .SerializeToUtf8Bytes(
                        snapshot,
                        SnapshotOptions));
        }
    }

    public void Restore(
        ulong index,
        ByteString data,
        ConfState configuration)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(
            configuration);
        ConfState expectedConfiguration =
            NormalizeConfState(configuration);
        ApplicationSnapshot snapshot =
            DecodeSnapshot(data);
        if (snapshot.PhysicalApplied != index)
        {
            throw new InvalidDataException(
                $"Application snapshot index {snapshot.PhysicalApplied} does not match Raft snapshot index {index}.");
        }

        var payloadConfiguration =
            new ConfState();
        try
        {
            payloadConfiguration =
                NormalizeConfState(
                    ConfState.Parser.ParseFrom(
                        snapshot.ConfState));
        }
        catch (InvalidProtocolBufferException exception)
        {
            throw new InvalidDataException(
                "Application snapshot configuration is malformed.",
                exception);
        }

        if (!payloadConfiguration.Equals(
                expectedConfiguration))
        {
            throw new InvalidDataException(
                "Application snapshot configuration does not match Raft snapshot metadata.");
        }

        ValidateSnapshot(snapshot);
        lock (gate)
        {
            ThrowIfDisposed();
            if (index < physicalApplied)
            {
                throw new InvalidOperationException(
                    $"Application snapshot index {index} is behind physical application {physicalApplied}.");
            }

            using SqliteTransaction transaction =
                connection.BeginTransaction(
                    deferred: false);
            ExecuteNonQuery(
                "DELETE FROM kv;",
                transaction);
            ExecuteNonQuery(
                "DELETE FROM requests;",
                transaction);
            foreach (SnapshotValue value in
                     snapshot.Values)
            {
                using SqliteCommand insert =
                    CreateCommand(
                        """
                        INSERT INTO kv (key, value)
                        VALUES ($key, $value);
                        """,
                        transaction);
                insert.Parameters.AddWithValue(
                    "$key",
                    value.Key);
                insert.Parameters.AddWithValue(
                    "$value",
                    value.Value);
                insert.ExecuteNonQuery();
            }

            foreach (SnapshotRequest request in
                     snapshot.Requests)
            {
                using SqliteCommand insert =
                    CreateCommand(
                        """
                        INSERT INTO requests (
                            request_id,
                            command_hash,
                            result_index)
                        VALUES (
                            $requestId,
                            $commandHash,
                            $resultIndex);
                        """,
                        transaction);
                AddBlob(
                    insert,
                    "$requestId",
                    request.RequestId);
                AddBlob(
                    insert,
                    "$commandHash",
                    request.CommandHash);
                AddBlob(
                    insert,
                    "$resultIndex",
                    EncodeUInt64(
                        request.ResultIndex));
                insert.ExecuteNonQuery();
            }

            UpdateMetadata(
                index,
                expectedConfiguration,
                transaction);
            transaction.Commit();
            physicalApplied = index;
            confState = expectedConfiguration;
        }
    }

    public KeyValueReadResult ReadAtLeast(
        string key,
        ulong requiredIndex)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            key);
        lock (gate)
        {
            ThrowIfDisposed();
            if (physicalApplied < requiredIndex)
            {
                throw new InvalidOperationException(
                    $"Physical application {physicalApplied} has not reached read index {requiredIndex}.");
            }

            return ReadCore(key);
        }
    }

    public void EnsureApplied(ulong requiredIndex)
    {
        lock (gate)
        {
            ThrowIfDisposed();
            if (physicalApplied < requiredIndex)
            {
                throw new InvalidOperationException(
                    $"Physical application {physicalApplied} has not reached read index {requiredIndex}.");
            }
        }
    }

    public KeyValueReadResult ReadLocal(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            key);
        lock (gate)
        {
            ThrowIfDisposed();
            return ReadCore(key);
        }
    }

    public void Dispose()
    {
        lock (gate)
        {
            if (disposed)
            {
                return;
            }

            disposed = true;
            connection.Dispose();
            ReleasePath();
        }
    }

    private bool InitializeOrRead()
    {
        long version =
            Scalar<long>(
                "PRAGMA user_version;");
        long userObjects =
            Scalar<long>(
                """
                SELECT COUNT(*)
                FROM sqlite_schema
                WHERE name NOT LIKE 'sqlite_%';
                """);
        if (version == 0)
        {
            if (userObjects != 0)
            {
                throw new InvalidDataException(
                    "Application database is not empty.");
            }

            InitializeSchema();
            ValidateSchema();
            physicalApplied = 0;
            confState =
                NormalizeConfState(
                    new ConfState());
            return true;
        }

        if (version != SchemaVersion)
        {
            throw new InvalidDataException(
                $"Application schema version {version} is unsupported.");
        }

        ValidateSchema();
        ReadMetadata();
        return false;
    }

    private void InitializeSchema()
    {
        using SqliteTransaction transaction =
            connection.BeginTransaction(
                deferred: false);
        ExecuteNonQuery(
            MetadataTableSql,
            transaction);
        ExecuteNonQuery(
            KeyValueTableSql,
            transaction);
        ExecuteNonQuery(
            RequestsTableSql,
            transaction);

        ConfState empty =
            NormalizeConfState(
                new ConfState());
        using (SqliteCommand insert =
               CreateCommand(
                   """
                   INSERT INTO app_metadata (
                       singleton,
                       format,
                       schema_version,
                       node_id,
                       physical_applied,
                       conf_state)
                   VALUES (
                       1,
                       $format,
                       $schemaVersion,
                       $nodeId,
                       $physicalApplied,
                       $confState);
                   """,
                   transaction))
        {
            insert.Parameters.AddWithValue(
                "$format",
                Format);
            insert.Parameters.AddWithValue(
                "$schemaVersion",
                SchemaVersion);
            AddBlob(
                insert,
                "$nodeId",
                EncodeUInt64(NodeId));
            AddBlob(
                insert,
                "$physicalApplied",
                EncodeUInt64(0));
            AddBlob(
                insert,
                "$confState",
                empty.ToByteArray());
            insert.ExecuteNonQuery();
        }

        ExecuteNonQuery(
            $"PRAGMA user_version = {SchemaVersion};",
            transaction);
        transaction.Commit();
    }

    private void ValidateSchema()
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
        SchemaObject[] expected =
        [
            new(
                "table",
                "app_metadata",
                "app_metadata",
                MetadataTableSql),
            new(
                "table",
                "kv",
                "kv",
                KeyValueTableSql),
            new(
                "table",
                "requests",
                "requests",
                RequestsTableSql),
        ];
        foreach (SchemaObject wanted in expected)
        {
            if (!reader.Read())
            {
                throw new InvalidDataException(
                    $"Application schema object '{wanted.Name}' is missing.");
            }

            var actual = new SchemaObject(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3)
                    ? string.Empty
                    : reader.GetString(3));
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
                    NormalizeSql(actual.Sql),
                    NormalizeSql(wanted.Sql),
                    StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidDataException(
                    $"Application schema object '{actual.Name}' does not match version {SchemaVersion}.");
            }
        }

        if (reader.Read())
        {
            throw new InvalidDataException(
                "Application database contains unexpected schema objects.");
        }
    }

    private void ReadMetadata()
    {
        using SqliteCommand command =
            CreateCommand(
                """
                SELECT
                    format,
                    schema_version,
                    node_id,
                    physical_applied,
                    conf_state
                FROM app_metadata
                WHERE singleton = 1;
                """);
        using SqliteDataReader reader =
            command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidDataException(
                "Application metadata is missing.");
        }

        if (!string.Equals(
                reader.GetString(0),
                Format,
                StringComparison.Ordinal)
            || reader.GetInt64(1)
                != SchemaVersion)
        {
            throw new InvalidDataException(
                "Application metadata format is invalid.");
        }

        ulong storedNodeId =
            DecodeUInt64(
                reader.GetFieldValue<byte[]>(2),
                "Application node ID");
        if (storedNodeId != NodeId)
        {
            throw new InvalidDataException(
                $"Application database belongs to node {storedNodeId}, not node {NodeId}.");
        }

        physicalApplied =
            DecodeUInt64(
                reader.GetFieldValue<byte[]>(3),
                "Physical applied index");
        try
        {
            confState = NormalizeConfState(
                ConfState.Parser.ParseFrom(
                    reader.GetFieldValue<byte[]>(4)));
        }
        catch (InvalidProtocolBufferException exception)
        {
            throw new InvalidDataException(
                "Application configuration is malformed.",
                exception);
        }

        if (reader.Read())
        {
            throw new InvalidDataException(
                "Application metadata contains multiple singleton rows.");
        }

        using SqliteCommand requests =
            CreateCommand(
                "SELECT MAX(result_index) FROM requests;");
        object? maximum = requests.ExecuteScalar();
        if (maximum is byte[] encoded
            && DecodeUInt64(
                    encoded,
                    "Maximum request result index")
                > physicalApplied)
        {
            throw new InvalidDataException(
                "Application request result exceeds physical application.");
        }
    }

    private void ConfigureConnection()
    {
        RequireText(
            "PRAGMA locking_mode = EXCLUSIVE;",
            "exclusive");
        RequireText(
            "PRAGMA journal_mode = WAL;",
            "wal");
        ExecuteNonQuery(
            "PRAGMA synchronous = FULL;");
        RequireLong(
            "PRAGMA synchronous;",
            2);
        ExecuteNonQuery(
            "PRAGMA fullfsync = ON;");
        ExecuteNonQuery(
            "PRAGMA checkpoint_fullfsync = ON;");
        ExecuteNonQuery(
            "PRAGMA busy_timeout = 30000;");
        ExecuteNonQuery(
            "PRAGMA trusted_schema = OFF;");
    }

    private void AcquireExclusiveLock()
    {
        using SqliteTransaction transaction =
            connection.BeginTransaction(
                deferred: false);
        using SqliteCommand command =
            CreateCommand(
                """
                UPDATE app_metadata
                SET format = format
                WHERE singleton = 1;
                """,
                transaction);
        command.ExecuteNonQuery();
        transaction.Commit();
    }

    private void ApplyMutation(
        KvCommand command,
        SqliteTransaction transaction)
    {
        switch (command.Type)
        {
            case KvCommandType.Set:
                using (SqliteCommand upsert =
                       CreateCommand(
                           """
                           INSERT INTO kv (key, value)
                           VALUES ($key, $value)
                           ON CONFLICT(key)
                           DO UPDATE SET value = excluded.value;
                           """,
                           transaction))
                {
                    upsert.Parameters.AddWithValue(
                        "$key",
                        command.Key);
                    upsert.Parameters.AddWithValue(
                        "$value",
                        command.Value!);
                    upsert.ExecuteNonQuery();
                }

                break;
            case KvCommandType.Delete:
                using (SqliteCommand delete =
                       CreateCommand(
                           """
                           DELETE FROM kv
                           WHERE key = $key;
                           """,
                           transaction))
                {
                    delete.Parameters.AddWithValue(
                        "$key",
                        command.Key);
                    delete.ExecuteNonQuery();
                }

                break;
            default:
                throw new InvalidDataException(
                    $"Unknown KV command type {command.Type}.");
        }
    }

    private RequestRecord? ReadRequest(
        Guid requestId,
        SqliteTransaction? transaction)
    {
        using SqliteCommand command =
            CreateCommand(
                """
                SELECT command_hash, result_index
                FROM requests
                WHERE request_id = $requestId;
                """,
                transaction);
        AddBlob(
            command,
            "$requestId",
            requestId.ToByteArray());
        using SqliteDataReader reader =
            command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        return new RequestRecord(
            ByteString.CopyFrom(
                reader.GetFieldValue<byte[]>(0)),
            DecodeUInt64(
                reader.GetFieldValue<byte[]>(1),
                "Request result index"));
    }

    private void InsertRequest(
        Guid requestId,
        ByteString fingerprint,
        ulong resultIndex,
        SqliteTransaction transaction)
    {
        using SqliteCommand command =
            CreateCommand(
                """
                INSERT INTO requests (
                    request_id,
                    command_hash,
                    result_index)
                VALUES (
                    $requestId,
                    $commandHash,
                    $resultIndex);
                """,
                transaction);
        AddBlob(
            command,
            "$requestId",
            requestId.ToByteArray());
        AddBlob(
            command,
            "$commandHash",
            fingerprint.ToByteArray());
        AddBlob(
            command,
            "$resultIndex",
            EncodeUInt64(resultIndex));
        command.ExecuteNonQuery();
    }

    private void UpdateMetadata(
        ulong index,
        ConfState configuration,
        SqliteTransaction transaction)
    {
        using SqliteCommand command =
            CreateCommand(
                """
                UPDATE app_metadata
                SET physical_applied = $physicalApplied,
                    conf_state = $confState
                WHERE singleton = 1;
                """,
                transaction);
        AddBlob(
            command,
            "$physicalApplied",
            EncodeUInt64(index));
        AddBlob(
            command,
            "$confState",
            configuration.ToByteArray());
        if (command.ExecuteNonQuery() != 1)
        {
            throw new InvalidDataException(
                "Application metadata update failed.");
        }
    }

    private KeyValueReadResult ReadCore(string key)
    {
        using SqliteCommand command =
            CreateCommand(
                """
                SELECT value
                FROM kv
                WHERE key = $key;
                """);
        command.Parameters.AddWithValue(
            "$key",
            key);
        object? value = command.ExecuteScalar();
        return value is null
            ? new KeyValueReadResult(
                false,
                null,
                physicalApplied)
            : new KeyValueReadResult(
                true,
                (string)value,
                physicalApplied);
    }

    private void EnsureNext(ulong index)
    {
        if (physicalApplied == ulong.MaxValue
            || index != physicalApplied + 1)
        {
            throw new InvalidOperationException(
                $"Committed entry index {index} does not follow physical application index {physicalApplied}.");
        }
    }

    private static ApplicationSnapshot DecodeSnapshot(
        ByteString data)
    {
        ApplicationSnapshot snapshot = null!;
        try
        {
            snapshot =
                JsonSerializer.Deserialize<
                    ApplicationSnapshot>(
                    data.Span,
                    SnapshotOptions)
                ?? throw new InvalidDataException(
                    "Application snapshot is null.");
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException(
                $"Application snapshot is malformed: {exception.Message}",
                exception);
        }

        if (snapshot.Version != 1)
        {
            throw new InvalidDataException(
                $"Application snapshot version {snapshot.Version} is unsupported.");
        }

        return snapshot;
    }

    private static void ValidateSnapshot(
        ApplicationSnapshot snapshot)
    {
        string? previousKey = null;
        foreach (SnapshotValue value in
                 snapshot.Values)
        {
            if (string.IsNullOrWhiteSpace(
                    value.Key)
                || value.Value is null
                || (previousKey is not null
                    && StringComparer.Ordinal.Compare(
                        previousKey,
                        value.Key) >= 0))
            {
                throw new InvalidDataException(
                    "Application snapshot KV rows are not unique and sorted.");
            }

            previousKey = value.Key;
        }

        byte[]? previousRequest = null;
        foreach (SnapshotRequest request in
                 snapshot.Requests)
        {
            if (request.RequestId.Length != 16
                || request.CommandHash.Length != 32
                || request.ResultIndex
                    > snapshot.PhysicalApplied
                || (previousRequest is not null
                    && CompareBytes(
                        previousRequest,
                        request.RequestId) >= 0))
            {
                throw new InvalidDataException(
                    "Application snapshot request rows are invalid.");
            }

            previousRequest = request.RequestId;
        }
    }

    private static int CompareBytes(
        byte[] left,
        byte[] right)
    {
        return left.AsSpan()
            .SequenceCompareTo(right);
    }

    private static string NormalizeSql(string sql)
    {
        return string.Join(
            ' ',
            sql.Split(
                (char[]?)null,
                StringSplitOptions
                    .RemoveEmptyEntries));
    }

    private static ConfState NormalizeConfState(
        ConfState state)
    {
        ConfState result = state.Clone();
        if (!result.HasAutoLeave)
        {
            result.AutoLeave = false;
        }

        return result;
    }

    private static byte[] EncodeUInt64(
        ulong value)
    {
        var result = new byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64BigEndian(
            result,
            value);
        return result;
    }

    private static ulong DecodeUInt64(
        byte[] value,
        string description)
    {
        if (value.Length != sizeof(ulong))
        {
            throw new InvalidDataException(
                $"{description} is not an eight-byte unsigned value.");
        }

        return BinaryPrimitives.ReadUInt64BigEndian(
            value);
    }

    private static void ClaimPath(string path)
    {
        lock (OwnershipGate)
        {
            if (!OwnedPaths.Add(path))
            {
                throw new InvalidOperationException(
                    $"Application database '{path}' is already open.");
            }
        }
    }

    private void ReleasePath()
    {
        if (!ownsPath)
        {
            return;
        }

        lock (OwnershipGate)
        {
            OwnedPaths.Remove(DatabasePath);
            ownsPath = false;
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(
            disposed,
            this);
    }

    private SqliteCommand CreateCommand(
        string sql,
        SqliteTransaction? transaction = null)
    {
        SqliteCommand command =
            connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = 30;
        command.Transaction = transaction;
        return command;
    }

    private void ExecuteNonQuery(
        string sql,
        SqliteTransaction? transaction = null)
    {
        using SqliteCommand command =
            CreateCommand(sql, transaction);
        command.ExecuteNonQuery();
    }

    private T Scalar<T>(string sql)
    {
        using SqliteCommand command =
            CreateCommand(sql);
        return (T)command.ExecuteScalar()!;
    }

    private void RequireText(
        string sql,
        string expected)
    {
        string actual = Scalar<string>(sql);
        if (!string.Equals(
                actual,
                expected,
                StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException(
                $"SQLite pragma returned '{actual}', expected '{expected}'.");
        }
    }

    private void RequireLong(
        string sql,
        long expected)
    {
        long actual = Scalar<long>(sql);
        if (actual != expected)
        {
            throw new InvalidDataException(
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

    private sealed record RequestRecord(
        ByteString Fingerprint,
        ulong ResultIndex);

    private sealed record ApplicationSnapshot(
        int Version,
        ulong PhysicalApplied,
        byte[] ConfState,
        IReadOnlyList<SnapshotValue> Values,
        IReadOnlyList<SnapshotRequest> Requests);

    private sealed record SnapshotValue(
        string Key,
        string Value);

    private sealed record SnapshotRequest(
        byte[] RequestId,
        byte[] CommandHash,
        ulong ResultIndex);

    private sealed record SchemaObject(
        string Type,
        string Name,
        string TableName,
        string Sql);
}
