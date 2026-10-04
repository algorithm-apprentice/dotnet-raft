namespace DotnetRaft.Storage.Sqlite;

internal static class SqliteStorageSchema
{
    internal const int Version = 1;
    internal const string Format =
        "dotnet-raft-sqlite";

    internal const string MetadataTableSql =
        """
        CREATE TABLE raft_metadata (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            format TEXT NOT NULL,
            schema_version INTEGER NOT NULL,
            hard_state BLOB NULL,
            snapshot BLOB NOT NULL,
            compacted_index BLOB NOT NULL CHECK (length(compacted_index) = 8),
            last_index BLOB NOT NULL CHECK (length(last_index) = 8),
            pending_snapshot_index BLOB NULL CHECK (
                pending_snapshot_index IS NULL
                OR length(pending_snapshot_index) = 8)
        ) STRICT
        """;

    internal const string EntriesTableSql =
        """
        CREATE TABLE raft_entries (
            entry_index BLOB PRIMARY KEY CHECK (length(entry_index) = 8),
            entry_term BLOB NOT NULL CHECK (length(entry_term) = 8),
            payload BLOB NOT NULL
        ) WITHOUT ROWID, STRICT
        """;

    internal static string NormalizeSql(string sql)
    {
        return string.Join(
            ' ',
            sql.Split(
                (char[]?)null,
                StringSplitOptions
                    .RemoveEmptyEntries));
    }
}
