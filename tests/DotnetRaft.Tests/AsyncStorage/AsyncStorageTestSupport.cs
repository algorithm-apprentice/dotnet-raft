using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

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
        state.Voters.AddRange(voters ?? [1UL]);
        var storage = new MemoryStorage(ProtobufRaftCodec.Instance);
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

    internal static void PersistAppend(MemoryStorage storage, StorageAppendWork append)
    {
        if (append.Snapshot is { } snapshot)
        {
            storage.ApplySnapshot(snapshot);
        }

        storage.Append(append.Entries);
        if (append.HardState is { } state)
        {
            storage.SetHardState(state);
        }
    }

    internal static Task ProcessReadyAsync(
        DotnetRawNode node,
        MemoryStorage storage,
        Ready ready)
    {
        // This helper has no network; only use it for singleton batches.
        if (ready.AppendWork is { } append)
        {
            PersistAppend(storage, append);
            foreach (Message response in append.MessagesAfterPersist)
            {
                Assert.Equal(node.GetBasicStatus().Id, response.To);
                node.Step(response);
            }

            if (append.Completion is { } complete)
            {
                node.CompleteStorageWork(complete);
            }
        }

        if (ready.ApplyWork is { } apply)
        {
            foreach (Entry entry in apply.Entries)
            {
                if (entry.Type == EntryType.EntryConfChange)
                {
                    node.ApplyConfChange(ProtobufFixtureEncoding.DecodeLegacy(entry.Data));
                }
                else if (entry.Type == EntryType.EntryConfChangeV2)
                {
                    node.ApplyConfChange(ProtobufFixtureEncoding.DecodeV2(entry.Data));
                }
            }

            node.CompleteStorageWork(apply.Completion);
        }

        return Task.CompletedTask;
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
