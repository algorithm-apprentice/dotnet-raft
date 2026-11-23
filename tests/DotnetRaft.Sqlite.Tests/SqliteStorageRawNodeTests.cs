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
            Message append = Assert.Single(
                ready.Messages,
                message =>
                    message.Type
                    == MessageType.MsgStorageAppend);

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
        Message append = Assert.Single(ready.Messages);
        Assert.Equal(MessageType.MsgStorageAppend, append.Type);
        Assert.Collection(append.Responses,
            response => Assert.Equal(leaderId, response.To),
            response => Assert.Equal(localNodeId, response.To));

        storage.PersistStorageAppend(append);
        Assert.Equal(1UL, storage.GetTerm(1));
        Assert.Equal(1UL, storage.GetHardState()?.Commit);

        var sent = new List<Message>();
        foreach (Message response in append.Responses)
        {
            if (response.To == localNodeId)
            {
                await node.StepAsync(response, guard.Token);
            }
            else
            {
                sent.Add(response);
            }
        }

        Message outgoing = Assert.Single(sent);
        Assert.Equal(MessageType.MsgAppResp, outgoing.Type);
        Assert.Equal(1UL, outgoing.Index);
        Ready afterAppend = await node.WaitForReadyAsync(guard.Token);
        Message apply = Assert.Single(afterAppend.Messages);
        Assert.Equal(MessageType.MsgStorageApply, apply.Type);
        Assert.Equal(1UL, Assert.Single(apply.Entries).Index);
    }
}
