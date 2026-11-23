using DotnetRaft.Storage.Sqlite;

using Microsoft.Data.Sqlite;

using static DotnetRaft.Sqlite.Tests.SqliteStorageTestSupport;

namespace DotnetRaft.Sqlite.Tests;

public sealed class SqliteHardStateCompatibilityTests
{
    [Fact]
    public void ReopensLegacyHardStateWithoutRewritingAbsentFields()
    {
        using TemporaryStorageDirectory directory = CreateDirectory();
        // The proto2 writer at 56a4a4d emitted only term for this partial state.
        byte[] legacy = [0x08, 0x05];
        InitializeWithLegacyState(directory, legacy);

        using (SqliteStorage storage = CreateStorage(directory))
        {
            Assert.Equal(new RaftHardState(5, 0, 0), storage.GetInitialState().HardState);
            storage.Append(Entries((1, 5)));
        }

        // Appending entries without a hard-state update must not rewrite
        // the old blob and change the presence of its zero-valued fields.
        Assert.Equal(legacy, ReadStoredHardState(directory));
    }

    [Fact]
    public void HardStateWithZeroVoteAndCommitRetainsLegacyEncoding()
    {
        using TemporaryStorageDirectory directory = CreateDirectory();
        var state = new RaftHardState(Term: ulong.MaxValue, Vote: 0, Commit: 0);
        using (SqliteStorage storage = CreateStorage(directory))
        {
            storage.SetHardState(state);
        }

        byte[] legacy = Convert.FromHexString("08FFFFFFFFFFFFFFFFFF0110001800");
        Assert.Equal(legacy, ReadStoredHardState(directory));

        using SqliteStorage reopened = CreateStorage(directory);
        Assert.Equal(state, reopened.GetHardState());
    }

    private static void InitializeWithLegacyState(TemporaryStorageDirectory directory, byte[] payload)
    {
        using (SqliteStorage initialize = CreateStorage(directory))
        {
            Assert.Null(initialize.GetHardState());
        }

        using SqliteConnection connection = OpenRaw(directory.DatabasePath);
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "UPDATE raft_metadata SET hard_state = $state WHERE singleton = 1;";
        command.Parameters.AddWithValue("$state", payload);
        Assert.Equal(1, command.ExecuteNonQuery());
    }

    private static byte[] ReadStoredHardState(TemporaryStorageDirectory directory)
    {
        using SqliteConnection connection = OpenRaw(directory.DatabasePath);
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT hard_state FROM raft_metadata WHERE singleton = 1;";
        return Assert.IsType<byte[]>(command.ExecuteScalar());
    }
}
