using System.Text.Json;
using System.Text.Json.Serialization;

using DotnetRaft.Protobuf;
using DotnetRaft.Protocol;

using Microsoft.Data.Sqlite;

namespace DotnetRaft.Examples.KvCluster;

public sealed partial class SqliteKeyValueStateMachine
{
    private static readonly JsonSerializerOptions SnapshotOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = false,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
    };

    public RaftBytes CreateSnapshotData()
    {
        lock (gate)
        {
            ThrowIfDisposed();
            List<SnapshotValue> values = ReadSnapshotValues();
            List<SnapshotRequest> requests = ReadSnapshotRequests();
            var snapshot = new ApplicationSnapshot(1, physicalApplied, ProtobufRaftCodec.EncodeMembership(confState), values, requests);
            return RaftBytes.CopyFrom(JsonSerializer.SerializeToUtf8Bytes(snapshot, SnapshotOptions));
        }
    }

    private List<SnapshotValue> ReadSnapshotValues()
    {
        var values = new List<SnapshotValue>();
        using SqliteCommand command = CreateCommand("SELECT key, value FROM kv;");
        using SqliteDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            values.Add(new SnapshotValue(reader.GetString(0), reader.GetString(1)));
        }

        // SQLite's UTF-8 BINARY order is not .NET ordinal order for all keys.
        values.Sort(static (left, right) => StringComparer.Ordinal.Compare(left.Key, right.Key));
        return values;
    }

    private List<SnapshotRequest> ReadSnapshotRequests()
    {
        var requests = new List<SnapshotRequest>();
        using SqliteCommand command = CreateCommand(
            "SELECT request_id, command_hash, result_index FROM requests ORDER BY request_id;");
        using SqliteDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            requests.Add(new SnapshotRequest(reader.GetFieldValue<byte[]>(0), reader.GetFieldValue<byte[]>(1),
                DecodeUInt64(reader.GetFieldValue<byte[]>(2), "Request result index")));
        }

        return requests;
    }

    public void Restore(ulong index, RaftBytes data, ConfState configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ConfState expectedConfiguration = NormalizeConfState(configuration);
        ApplicationSnapshot snapshot = ReadValidatedSnapshot(index, data, expectedConfiguration);
        lock (gate)
        {
            ThrowIfDisposed();
            EnsureSnapshotDoesNotMoveBackward(index);
            ReplaceApplicationFromSnapshot(index, snapshot, expectedConfiguration);
        }
    }

    private static ApplicationSnapshot ReadValidatedSnapshot(ulong index, RaftBytes data, ConfState configuration)
    {
        ApplicationSnapshot snapshot = DecodeSnapshot(data);
        if (snapshot.PhysicalApplied != index)
        {
            throw new InvalidDataException(
                $"Application snapshot index {snapshot.PhysicalApplied} does not match Raft snapshot index {index}.");
        }

        ConfState payloadConfiguration = ReadSnapshotConfiguration(snapshot);
        if (!payloadConfiguration.Equals(configuration))
        {
            throw new InvalidDataException("Application snapshot configuration does not match Raft snapshot metadata.");
        }

        ValidateSnapshotValues(snapshot.Values);
        ValidateSnapshotRequests(snapshot);
        return snapshot;
    }

    private static ConfState ReadSnapshotConfiguration(ApplicationSnapshot snapshot)
    {
        try
        {
            return NormalizeConfState(ProtobufRaftCodec.DecodeMembership(snapshot.ConfState));
        }
        catch (InvalidDataException exception)
        {
            throw new InvalidDataException("Application snapshot configuration is malformed.", exception);
        }
    }

    private void EnsureSnapshotDoesNotMoveBackward(ulong index)
    {
        if (index < physicalApplied)
        {
            throw new InvalidOperationException(
                $"Application snapshot index {index} is behind physical application {physicalApplied}.");
        }
    }

    private void ReplaceApplicationFromSnapshot(ulong index, ApplicationSnapshot snapshot, ConfState configuration)
    {
        using SqliteTransaction transaction = connection.BeginTransaction(deferred: false);
        ExecuteNonQuery("DELETE FROM kv;", transaction);
        ExecuteNonQuery("DELETE FROM requests;", transaction);
        RestoreSnapshotValues(snapshot.Values, transaction);
        RestoreSnapshotRequests(snapshot.Requests, transaction);
        UpdateMetadata(index, configuration, transaction);
        transaction.Commit();
        physicalApplied = index;
        confState = configuration;
    }

    private void RestoreSnapshotValues(IEnumerable<SnapshotValue> values, SqliteTransaction transaction)
    {
        foreach (SnapshotValue value in values)
        {
            using SqliteCommand insert = CreateCommand(
                "INSERT INTO kv (key, value) VALUES ($key, $value);", transaction);
            insert.Parameters.AddWithValue("$key", value.Key);
            insert.Parameters.AddWithValue("$value", value.Value);
            insert.ExecuteNonQuery();
        }
    }

    private void RestoreSnapshotRequests(IEnumerable<SnapshotRequest> requests, SqliteTransaction transaction)
    {
        foreach (SnapshotRequest request in requests)
        {
            using SqliteCommand insert = CreateCommand(
                """
                INSERT INTO requests (request_id, command_hash, result_index)
                VALUES ($requestId, $commandHash, $resultIndex);
                """, transaction);
            AddBlob(insert, "$requestId", request.RequestId);
            AddBlob(insert, "$commandHash", request.CommandHash);
            AddBlob(insert, "$resultIndex", EncodeUInt64(request.ResultIndex));
            insert.ExecuteNonQuery();
        }
    }

    private static ApplicationSnapshot DecodeSnapshot(RaftBytes data)
    {
        ApplicationSnapshot snapshot;
        try
        {
            snapshot = JsonSerializer.Deserialize<ApplicationSnapshot>(data.Span, SnapshotOptions)
                ?? throw new InvalidDataException("Application snapshot is null.");
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException($"Application snapshot is malformed: {exception.Message}", exception);
        }

        if (snapshot.Version != 1)
        {
            throw new InvalidDataException($"Application snapshot version {snapshot.Version} is unsupported.");
        }

        return snapshot;
    }

    private static void ValidateSnapshotValues(IEnumerable<SnapshotValue> values)
    {
        string? previousKey = null;
        foreach (SnapshotValue value in values)
        {
            if (string.IsNullOrWhiteSpace(value.Key) || value.Value is null
                || (previousKey is not null && StringComparer.Ordinal.Compare(previousKey, value.Key) >= 0))
            {
                throw new InvalidDataException("Application snapshot KV rows are not unique and sorted.");
            }

            previousKey = value.Key;
        }
    }

    private static void ValidateSnapshotRequests(ApplicationSnapshot snapshot)
    {
        byte[]? previousRequest = null;
        foreach (SnapshotRequest request in snapshot.Requests)
        {
            if (request.RequestId.Length != 16 || request.CommandHash.Length != 32
                || request.ResultIndex > snapshot.PhysicalApplied
                || (previousRequest is not null && previousRequest.AsSpan().SequenceCompareTo(request.RequestId) >= 0))
            {
                throw new InvalidDataException("Application snapshot request rows are invalid.");
            }

            previousRequest = request.RequestId;
        }
    }

    private sealed record ApplicationSnapshot(
        int Version, ulong PhysicalApplied, byte[] ConfState,
        IReadOnlyList<SnapshotValue> Values, IReadOnlyList<SnapshotRequest> Requests);

    private sealed record SnapshotValue(string Key, string Value);

    private sealed record SnapshotRequest(byte[] RequestId, byte[] CommandHash, ulong ResultIndex);
}
