using System.Diagnostics;

using DotnetRaft.Protobuf;
using DotnetRaft.Protocol;
using DotnetRaft.Storage.Sqlite;

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

        try
        {
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
        finally
        {
            // A normal return or exception must not masquerade as an abrupt exit.
            Console.Error.WriteLine(
                "CRASH_FIXTURE_UNWOUND");
            Console.Error.Flush();
        }
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
            Vote = 2,
            Commit = 3,
            Snapshot = new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = 2,
                    Term = 6,
                    ConfState = new ConfState { Voters = { 1, 2 } },
                },
                Data = RaftBytes.CopyFromUtf8("new application snapshot"),
            },
        };
        request.Entries.Add(
            new Entry
            {
                Index = 3,
                Term = 7,
                Data =
                    RaftBytes.CopyFromUtf8(
                        "durable"),
            });
        storage.PersistStorageAppend(request);
        CrashAfterMarker("COMMIT_RETURNED");
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
            ProtobufRaftCodec.EncodeHardState(new RaftHardState(99, 99, 0)));
        if (command.ExecuteNonQuery() != 1)
        {
            throw new InvalidOperationException("The crash fixture did not update its metadata row.");
        }
        CrashAfterMarker(
            "UNCOMMITTED_MUTATION_EXECUTED");
    }

    private static void CrashAfterMarker(
        string marker)
    {
        Console.Out.WriteLine(marker);
        Console.Out.Flush();

        // Bypass Windows Error Reporting and dumps without unwinding the
        // live SQLite scopes. FailFast can delay exit beyond the test deadline.
        using Process process =
            Process.GetCurrentProcess();
        process.Kill();
        // Keep the caller's using scopes live if the kill returns asynchronously.
        process.WaitForExit();
        throw new InvalidOperationException(
            "The crash fixture was not terminated.");
    }
}
