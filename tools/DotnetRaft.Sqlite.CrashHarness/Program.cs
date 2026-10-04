using DotnetRaft.Protocol;
using DotnetRaft.Storage.Sqlite;

using Google.Protobuf;

using Microsoft.Data.Sqlite;

namespace DotnetRaft.Sqlite.CrashHarness;

public static class CrashHarnessMarker
{
}

internal static class Program
{
    public static int Main(string[] args)
    {
        if (args.Length != 2)
        {
            Console.Error.WriteLine(
                "usage: <committed|uncommitted> <database-path>");
            return 2;
        }

        switch (args[0])
        {
            case "committed":
                CommitThenCrash(args[1]);
                break;
            case "uncommitted":
                MutateWithoutCommitThenCrash(
                    args[1]);
                break;
            default:
                Console.Error.WriteLine(
                    "unknown crash mode");
                return 2;
        }

        return 1;
    }

    private static void CommitThenCrash(
        string databasePath)
    {
        using var storage =
            new SqliteStorage(databasePath);
        var request = new Message
        {
            Type = MessageType.MsgStorageAppend,
            From = 1,
            To =
                RaftLocalMessageTargets.AppendThread,
            Term = 7,
            Vote = 1,
            Commit = 1,
        };
        request.Entries.Add(
            new Entry
            {
                Index = 1,
                Term = 7,
                Data =
                    ByteString.CopyFromUtf8(
                        "durable"),
            });
        storage.PersistStorageAppend(request);
        Console.Out.WriteLine("COMMIT_RETURNED");
        Console.Out.Flush();
        Environment.FailFast(
            "Committed crash fixture.");
    }

    private static void MutateWithoutCommitThenCrash(
        string databasePath)
    {
        var builder =
            new SqliteConnectionStringBuilder
            {
                DataSource = databasePath,
                Mode =
                    SqliteOpenMode.ReadWrite,
                Cache = SqliteCacheMode.Private,
                Pooling = false,
            };
        using var connection =
            new SqliteConnection(
                builder.ToString());
        connection.Open();
        using SqliteTransaction transaction =
            connection.BeginTransaction(
                deferred: false);
        using SqliteCommand command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            UPDATE raft_metadata
            SET hard_state = $hardState
            WHERE singleton = 1;
            """;
        command.Parameters.AddWithValue(
            "$hardState",
            new HardState
            {
                Term = 99,
                Vote = 99,
                Commit = 0,
            }.ToByteArray());
        command.ExecuteNonQuery();
        Console.Out.WriteLine(
            "UNCOMMITTED_MUTATION_EXECUTED");
        Console.Out.Flush();
        Environment.FailFast(
            "Uncommitted crash fixture.");
    }
}
