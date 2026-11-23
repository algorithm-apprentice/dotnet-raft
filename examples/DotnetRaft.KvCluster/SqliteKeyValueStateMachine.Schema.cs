using DotnetRaft.Protobuf;
using DotnetRaft.Protocol;

using Microsoft.Data.Sqlite;

namespace DotnetRaft.Examples.KvCluster;

public sealed partial class SqliteKeyValueStateMachine
{
    private const int SchemaVersion = 1;
    private const string Format = "dotnet-raft-durable-kv";
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

    private bool InitializeOrRead()
    {
        long version = Scalar<long>("PRAGMA user_version;");
        long userObjects = Scalar<long>("SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%';");
        if (version == 0)
        {
            InitializeEmptyDatabase(userObjects);
            return true;
        }

        if (version != SchemaVersion)
        {
            throw new InvalidDataException($"Application schema version {version} is unsupported.");
        }

        ValidateSchema();
        ReadMetadata();
        return false;
    }

    private void InitializeEmptyDatabase(long userObjects)
    {
        if (userObjects != 0)
        {
            throw new InvalidDataException("Application database is not empty.");
        }

        var initial = new AppliedPrefix(0, NormalizeConfState(new ConfState()));
        InitializeSchema(initial);
        ValidateSchema();
        appliedPrefix = initial;
    }

    private void InitializeSchema(AppliedPrefix initial)
    {
        using SqliteTransaction transaction = connection.BeginTransaction(deferred: false);
        ExecuteNonQuery(MetadataTableSql, transaction);
        ExecuteNonQuery(KeyValueTableSql, transaction);
        ExecuteNonQuery(RequestsTableSql, transaction);
        InsertInitialMetadata(initial, transaction);
        ExecuteNonQuery($"PRAGMA user_version = {SchemaVersion};", transaction);
        transaction.Commit();
    }

    private void InsertInitialMetadata(AppliedPrefix initial, SqliteTransaction transaction)
    {
        using SqliteCommand insert = CreateCommand(
            """
            INSERT INTO app_metadata (
                singleton, format, schema_version, node_id, physical_applied, conf_state)
            VALUES (1, $format, $schemaVersion, $nodeId, $physicalApplied, $confState);
            """, transaction);
        insert.Parameters.AddWithValue("$format", Format);
        insert.Parameters.AddWithValue("$schemaVersion", SchemaVersion);
        AddBlob(insert, "$nodeId", EncodeUInt64(NodeId));
        AddBlob(insert, "$physicalApplied", EncodeUInt64(initial.Index));
        AddBlob(insert, "$confState", ProtobufRaftCodec.EncodeMembership(initial.Configuration));
        insert.ExecuteNonQuery();
    }

    private void ValidateSchema()
    {
        using SqliteCommand command = CreateCommand(
            "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name;");
        using SqliteDataReader reader = command.ExecuteReader();
        SchemaObject[] expected =
        [
            new("table", "app_metadata", "app_metadata", MetadataTableSql),
            new("table", "kv", "kv", KeyValueTableSql),
            new("table", "requests", "requests", RequestsTableSql),
        ];
        foreach (SchemaObject wanted in expected)
        {
            ValidateSchemaObject(reader, wanted);
        }

        if (reader.Read())
        {
            throw new InvalidDataException("Application database contains unexpected schema objects.");
        }
    }

    private static void ValidateSchemaObject(SqliteDataReader reader, SchemaObject wanted)
    {
        if (!reader.Read())
        {
            throw new InvalidDataException($"Application schema object '{wanted.Name}' is missing.");
        }

        var actual = new SchemaObject(reader.GetString(0), reader.GetString(1), reader.GetString(2),
            reader.IsDBNull(3) ? string.Empty : reader.GetString(3));
        if (!SchemaMatches(actual, wanted))
        {
            throw new InvalidDataException($"Application schema object '{actual.Name}' does not match version {SchemaVersion}.");
        }
    }

    private static bool SchemaMatches(SchemaObject actual, SchemaObject wanted) =>
        string.Equals(actual.Type, wanted.Type, StringComparison.Ordinal)
        && string.Equals(actual.Name, wanted.Name, StringComparison.Ordinal)
        && string.Equals(actual.TableName, wanted.TableName, StringComparison.Ordinal)
        && string.Equals(NormalizeSql(actual.Sql), NormalizeSql(wanted.Sql), StringComparison.OrdinalIgnoreCase);

    private void ReadMetadata()
    {
        using SqliteCommand command = CreateCommand(
            "SELECT format, schema_version, node_id, physical_applied, conf_state FROM app_metadata WHERE singleton = 1;");
        using SqliteDataReader reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidDataException("Application metadata is missing.");
        }

        ValidateMetadataFormat(reader);
        ValidateNodeBinding(reader);
        var recovered = new AppliedPrefix(DecodeUInt64(reader.GetFieldValue<byte[]>(3), "Physical applied index"),
            ReadStoredConfiguration(reader));
        if (reader.Read())
        {
            throw new InvalidDataException("Application metadata contains multiple singleton rows.");
        }

        ValidateRequestResultBounds(recovered.Index);
        appliedPrefix = recovered;
    }

    private static void ValidateMetadataFormat(SqliteDataReader reader)
    {
        if (!string.Equals(reader.GetString(0), Format, StringComparison.Ordinal) || reader.GetInt64(1) != SchemaVersion)
        {
            throw new InvalidDataException("Application metadata format is invalid.");
        }
    }

    private void ValidateNodeBinding(SqliteDataReader reader)
    {
        ulong storedNodeId = DecodeUInt64(reader.GetFieldValue<byte[]>(2), "Application node ID");
        if (storedNodeId != NodeId)
        {
            throw new InvalidDataException($"Application database belongs to node {storedNodeId}, not node {NodeId}.");
        }
    }

    private static ConfState ReadStoredConfiguration(SqliteDataReader reader)
    {
        try
        {
            return NormalizeConfState(ProtobufRaftCodec.DecodeMembership(reader.GetFieldValue<byte[]>(4)));
        }
        catch (InvalidDataException exception)
        {
            throw new InvalidDataException("Application configuration is malformed.", exception);
        }
    }

    private void ValidateRequestResultBounds(ulong appliedIndex)
    {
        using SqliteCommand requests = CreateCommand("SELECT MAX(result_index) FROM requests;");
        object? maximum = requests.ExecuteScalar();
        if (maximum is byte[] encoded && DecodeUInt64(encoded, "Maximum request result index") > appliedIndex)
        {
            throw new InvalidDataException("Application request result exceeds physical application.");
        }
    }

    private static string NormalizeSql(string sql) =>
        string.Join(' ', sql.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

    private sealed record SchemaObject(string Type, string Name, string TableName, string Sql);
}
