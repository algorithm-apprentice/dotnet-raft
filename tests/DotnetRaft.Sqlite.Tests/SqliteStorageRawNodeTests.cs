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
                    ProtobufFixtureEncoding.DecodeLegacy(
                            configuration.Data));
            storage.CreateSnapshot(
                configuration.Index,
                confState,
                RaftBytes.CopyFromUtf8(
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
                new RaftHardState
                {
                    Term = 1,
                });
            storage.ApplySnapshot(
                SnapshotAt(
                    2,
                    1,
                    confState));
            storage.SetHardState(
                new RaftHardState
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
            StorageAppendWork append = Assert.IsType<StorageAppendWork>(ready.AppendWork);

            storage.PersistStorageAppend(append);
        }

        using SqliteStorage reopened =
            CreateStorage(directory);
        RaftHardState hardState =
            Assert.IsType<RaftHardState>(
                reopened.GetHardState());
        Assert.Equal(2UL, hardState.Term);
        Assert.Equal(1UL, hardState.Vote);
        Assert.Equal(2UL, hardState.Commit);
        Assert.Equal(2UL, reopened.GetLastIndex());
    }

    [Fact]
    public async Task DurableAppendResponsesGoToTheLocalNodeAndTheRemotePeer()
    {
        using TemporaryStorageDirectory directory = CreateDirectory();
        using SqliteStorage storage = CreateStorage(directory);
        const ulong localNodeId = 1;
        const ulong leaderId = 2;
        var configuration = new ConfState { Voters = { localNodeId, leaderId, 3 } };
        storage.ApplySnapshot(SnapshotAt(0, 0, configuration));
        await using var node = RaftNode.Restart(new RaftConfig
        {
            Id = localNodeId,
            ElectionTick = 10,
            HeartbeatTick = 1,
            Storage = storage,
            AsyncStorageWrites = true,
        });
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var incoming = new Message
        {
            Type = MessageType.MsgApp,
            From = leaderId,
            To = localNodeId,
            Term = 1,
            Commit = 1,
        };
        incoming.Entries.Add(new Entry { Index = 1, Term = 1 });
        await node.StepAsync(incoming, guard.Token);
        Ready ready = await node.WaitForReadyAsync(guard.Token);
        Assert.Empty(ready.Messages);
        StorageAppendWork append = Assert.IsType<StorageAppendWork>(ready.AppendWork);
        Assert.Equal(leaderId, Assert.Single(append.MessagesAfterPersist).To);

        storage.PersistStorageAppend(append);
        Assert.Equal(1UL, storage.GetTerm(1));
        Assert.Equal(1UL, storage.GetHardState()?.Commit);

        Message outgoing = Assert.Single(append.MessagesAfterPersist);
        Assert.Equal(MessageType.MsgAppResp, outgoing.Type);
        Assert.Equal(1UL, outgoing.Index);
        await node.CompleteStorageWorkAsync(Assert.IsAssignableFrom<StorageCompletion>(append.Completion), guard.Token);
        Ready afterAppend = await node.WaitForReadyAsync(guard.Token);
        Assert.Empty(afterAppend.Messages);
        StorageApplyWork apply = Assert.IsType<StorageApplyWork>(afterAppend.ApplyWork);
        Assert.Equal(1UL, Assert.Single(apply.Entries).Index);
        await node.CompleteStorageWorkAsync(apply.Completion, guard.Token);
        Assert.Equal(1UL, (await node.GetStatusAsync(guard.Token)).Basic.Applied);
    }
}
