using DotnetRaft.Storage.Sqlite;

using Google.Protobuf;

using Microsoft.Data.Sqlite;

using static DotnetRaft.Sqlite.Tests.SqliteStorageTestSupport;

using ProtocolHardState = DotnetRaft.Protocol.HardState;

namespace DotnetRaft.Sqlite.Tests;

public sealed class SqliteHardStateCompatibilityTests
{
    [Fact]
    public void ReopensStateWrittenWithTheOldProtobufModel()
    {
        using TemporaryStorageDirectory directory = CreateDirectory();
        InitializeWithLegacyState(directory, new ProtocolHardState { Term = 5 });

        using (SqliteStorage storage = CreateStorage(directory))
        {
            Assert.Equal(new RaftHardState(5, 0, 0), storage.GetInitialState().HardState);
            storage.Append(Entries((1, 5)));
        }

        // Appending entries without a hard-state update must not rewrite
        // the old blob and change the presence of its zero-valued fields.
        ProtocolHardState stored = ReadProtocolState(directory);
        Assert.Equal(5UL, stored.Term);
        Assert.False(stored.HasVote);
        Assert.False(stored.HasCommit);
    }

    [Fact]
    public void NewStateCanStillBeReadByTheProtobufParser()
    {
        using TemporaryStorageDirectory directory = CreateDirectory();
        var state = new RaftHardState(Term: ulong.MaxValue, Vote: 0, Commit: 0);
        using (SqliteStorage storage = CreateStorage(directory))
        {
            storage.SetHardState(state);
        }

        ProtocolHardState stored = ReadProtocolState(directory);
        Assert.True(stored.HasTerm);
        Assert.True(stored.HasVote);
        Assert.True(stored.HasCommit);
        Assert.Equal(state, new RaftHardState(stored.Term, stored.Vote, stored.Commit));

        using SqliteStorage reopened = CreateStorage(directory);
        Assert.Equal(state, reopened.GetHardState());
    }

    private static void InitializeWithLegacyState(TemporaryStorageDirectory directory, ProtocolHardState state)
    {
        using (SqliteStorage initialize = CreateStorage(directory))
        {
            Assert.Null(initialize.GetHardState());
        }

        using SqliteConnection connection = OpenRaw(directory.DatabasePath);
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "UPDATE raft_metadata SET hard_state = $state WHERE singleton = 1;";
        command.Parameters.AddWithValue("$state", state.ToByteArray());
        Assert.Equal(1, command.ExecuteNonQuery());
    }

    private static ProtocolHardState ReadProtocolState(TemporaryStorageDirectory directory)
    {
        using SqliteConnection connection = OpenRaw(directory.DatabasePath);
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT hard_state FROM raft_metadata WHERE singleton = 1;";
        return ProtocolHardState.Parser.ParseFrom(Assert.IsType<byte[]>(command.ExecuteScalar()));
    }
}
