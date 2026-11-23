using System.Buffers.Binary;

using DotnetRaft.Protocol;

using Google.Protobuf;

using Microsoft.Data.Sqlite;

namespace DotnetRaft.Examples.KvCluster;

public sealed partial class SqliteKeyValueStateMachine
    : IDisposable
{
    private static readonly object OwnershipGate =
        new();
    private static readonly HashSet<string> OwnedPaths =
        new(
            OperatingSystem.IsWindows()
                ? StringComparer.OrdinalIgnoreCase
                : StringComparer.Ordinal);
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
        EnsureDatabaseDirectory();
        connection = CreateConnection(createIfMissing);
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

    private void EnsureDatabaseDirectory()
    {
        string? directory =
            Path.GetDirectoryName(DatabasePath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }
    }

    private SqliteConnection CreateConnection(bool createIfMissing)
    {
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
        return new SqliteConnection(builder.ToString());
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

    public KvRequestResolution? ResolveRequest(
        KvCommand command)
    {
        KvCommandFingerprint fingerprint =
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
        KvCommandFingerprint expectedFingerprint)
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
                Conflict: false).ForRequest(expectedFingerprint);
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
            EnsureAppliedCore(requiredIndex);
            return ReadCore(key);
        }
    }

    public void EnsureApplied(ulong requiredIndex)
    {
        lock (gate)
        {
            ThrowIfDisposed();
            EnsureAppliedCore(requiredIndex);
        }
    }

    private void EnsureAppliedCore(ulong requiredIndex)
    {
        if (physicalApplied < requiredIndex)
        {
            throw new InvalidOperationException(
                $"Physical application {physicalApplied} has not reached read index {requiredIndex}.");
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
            KvCommandFingerprint.Read(
                reader.GetFieldValue<byte[]>(0)),
            DecodeUInt64(
                reader.GetFieldValue<byte[]>(1),
                "Request result index"));
    }

    private void InsertRequest(
        Guid requestId,
        KvCommandFingerprint fingerprint,
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

    private static void EnsureNext(ulong previous, ulong index)
    {
        if (previous == ulong.MaxValue || index != previous + 1)
        {
            throw new InvalidOperationException(
                $"Committed entry index {index} does not follow physical application index {previous}.");
        }
    }

    private static ConfState NormalizeConfState(
        ConfState state)
    {
        return ProtocolDefaults.EnsureConfState(state.Clone());
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
        KvCommandFingerprint Fingerprint,
        ulong ResultIndex);

}
