using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using DotnetRawNode = DotnetRaft.RawNode;
using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.AsyncStorage;

internal static class AsyncStorageTestSupport
{
    internal static (
        DotnetRawNode Node,
        MemoryStorage Storage) CreateNode(
        IEnumerable<ulong>? voters = null,
        ulong applied = 2,
        ulong maxCommittedSizePerReady = 0,
        IRaftTraceSink? traceSink = null)
    {
        var state = new ConfState();
        state.Voters.Add(voters ?? [1UL]);
        var storage = new MemoryStorage();
        storage.ApplySnapshot(
            new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = 2,
                    Term = 1,
                    ConfState = state,
                },
            });
        var node = new DotnetRawNode(
            new RaftConfig
            {
                Id = 1,
                ElectionTick = 10,
                HeartbeatTick = 1,
                Storage = storage,
                Applied = applied,
                AsyncStorageWrites = true,
                MaxCommittedSizePerReady =
                    maxCommittedSizePerReady,
                TraceSink = traceSink,
            });
        return (node, storage);
    }

    internal static Message StorageMessage(
        Ready ready,
        MessageType type)
    {
        return Assert.Single(
            ready.Messages,
            message => message.Type == type);
    }

    internal static Message[] PersistAppend(
        MemoryStorage storage,
        Message request)
    {
        Assert.Equal(
            MessageType.MsgStorageAppend,
            request.Type);
        if (request.Snapshot is not null
            && request.Snapshot.Metadata.Index != 0)
        {
            storage.ApplySnapshot(request.Snapshot);
        }

        storage.Append(request.Entries);
        if (request.HasTerm
            || request.HasVote
            || request.HasCommit)
        {
            Assert.True(request.HasTerm);
            Assert.True(request.HasVote);
            Assert.True(request.HasCommit);
            storage.SetHardState(
                new HardState
                {
                    Term = request.Term,
                    Vote = request.Vote,
                    Commit = request.Commit,
                });
        }

        return
        [
            .. request.Responses.Select(
                response => response.Clone()),
        ];
    }

    internal static async Task ProcessReadyAsync(
        DotnetRawNode node,
        MemoryStorage storage,
        Ready ready)
    {
        foreach (Message message in ready.Messages)
        {
            switch (message.Type)
            {
                case MessageType.MsgStorageAppend:
                    foreach (Message response in
                             PersistAppend(
                                 storage,
                                 message))
                    {
                        node.Step(response);
                    }

                    break;
                case MessageType.MsgStorageApply:
                    foreach (Entry entry in message.Entries)
                    {
                        switch (entry.Type)
                        {
                            case EntryType.EntryConfChange:
                                node.ApplyConfChange(
                                    ProtocolConfChange.Parser
                                        .ParseFrom(
                                            entry.Data));
                                break;
                            case EntryType.EntryConfChangeV2:
                                node.ApplyConfChange(
                                    ConfChangeV2.Parser
                                        .ParseFrom(
                                            entry.Data));
                                break;
                        }
                    }

                    foreach (Message response in
                             message.Responses)
                    {
                        node.Step(response);
                    }

                    break;
            }
        }

        await Task.CompletedTask;
    }

    internal static async Task BecomeLeaderAsync(
        DotnetRawNode node,
        MemoryStorage storage)
    {
        node.Campaign();

        Ready election = node.Ready();
        await ProcessReadyAsync(
            node,
            storage,
            election);

        Ready leadership = node.Ready();
        await ProcessReadyAsync(
            node,
            storage,
            leadership);

        Ready commit = node.Ready();
        await ProcessReadyAsync(
            node,
            storage,
            commit);

        Assert.False(node.HasReady());
    }
}
