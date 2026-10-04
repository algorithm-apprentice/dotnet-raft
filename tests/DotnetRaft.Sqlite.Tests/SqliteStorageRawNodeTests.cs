using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Storage.Sqlite;

using Google.Protobuf;

using static DotnetRaft.Sqlite.Tests.SqliteStorageTestSupport;

using ProtocolConfChange =
    DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Sqlite.Tests;

public sealed class SqliteStorageRawNodeTests
{
    [Fact]
    public void SynchronousReadyRestartsFromDurableState()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using (SqliteStorage storage =
               CreateStorage(directory))
        {
            var raw = RawNode.Start(
                new RaftConfig
                {
                    Id = 1,
                    ElectionTick = 10,
                    HeartbeatTick = 1,
                    Storage = storage,
                },
                [new Peer(1)]);
            Ready ready = raw.Ready();
            storage.PersistReady(ready);
            Entry configuration =
                Assert.Single(
                    ready.CommittedEntries);
            ConfState confState =
                raw.ApplyConfChange(
                    ProtocolConfChange.Parser
                        .ParseFrom(
                            configuration.Data));
            storage.CreateSnapshot(
                configuration.Index,
                confState,
                ByteString.CopyFromUtf8(
                    "application"));
            storage.Compact(
                configuration.Index);
            raw.Advance(ready);
        }

        using SqliteStorage reopened =
            CreateStorage(directory);
        var restarted = RawNode.Restart(
            new RaftConfig
            {
                Id = 1,
                ElectionTick = 10,
                HeartbeatTick = 1,
                Storage = reopened,
                Applied = 1,
            });

        Status status = restarted.GetStatus();
        Assert.Equal(1UL, status.Basic.Term);
        Assert.Equal(1UL, status.Basic.Commit);
        Assert.Equal(1UL, status.Basic.Applied);
        Assert.Equal(
            [1UL],
            status.Configuration.Voters);
        Assert.False(restarted.HasReady());
    }

    [Fact]
    public void AsyncAppendRequestRestartsWithPersistedVote()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using (SqliteStorage storage =
               CreateStorage(directory))
        {
            var confState = new ConfState();
            confState.Voters.Add(1);
            storage.SetHardState(
                new HardState
                {
                    Term = 1,
                });
            storage.ApplySnapshot(
                SnapshotAt(
                    2,
                    1,
                    confState));
            storage.SetHardState(
                new HardState
                {
                    Term = 1,
                    Commit = 2,
                });
            storage.AcknowledgeApplicationSnapshot(
                2);

            var raw = RawNode.Restart(
                new RaftConfig
                {
                    Id = 1,
                    ElectionTick = 10,
                    HeartbeatTick = 1,
                    Storage = storage,
                    Applied = 2,
                    AsyncStorageWrites = true,
                });
            raw.Campaign();
            Ready ready = raw.Ready();
            Message append = Assert.Single(
                ready.Messages,
                message =>
                    message.Type
                    == MessageType.MsgStorageAppend);

            storage.PersistStorageAppend(append);
        }

        using SqliteStorage reopened =
            CreateStorage(directory);
        HardState hardState =
            Assert.IsType<HardState>(
                reopened.GetHardState());
        Assert.Equal(2UL, hardState.Term);
        Assert.Equal(1UL, hardState.Vote);
        Assert.Equal(2UL, hardState.Commit);
        Assert.Equal(2UL, reopened.GetLastIndex());
    }
}
