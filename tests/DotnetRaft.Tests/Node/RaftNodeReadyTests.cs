using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using static DotnetRaft.Tests.Node.RaftNodeTestSupport;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Node;

public sealed class RaftNodeReadyTests
{
    [Fact]
    public async Task StartDeliversBootstrapReady()
    {
        var storage = new MemoryStorage();
        var node = RaftNode.Start(
            new RaftConfig
            {
                Id = 1,
                Storage = storage,
                ElectionTick = 10,
                HeartbeatTick = 1,
            },
            [new Peer(1)]);
        try
        {
            Ready ready = await WaitReadyAsync(node);

            Assert.Single(ready.Entries);
            Entry committed =
                Assert.Single(ready.CommittedEntries);
            Assert.Equal(
                EntryType.EntryConfChange,
                committed.Type);
            Assert.Equal(1UL, ready.HardState?.Term);
            storage.Append(ready.Entries);
            storage.SetHardState(ready.HardState!);
            await node.ApplyConfChangeAsync(
                ProtocolConfChange.Parser.ParseFrom(
                    committed.Data));
            await node.AdvanceAsync();
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task RestartDeliversCommittedWork()
    {
        MemoryStorage storage = CreateStorage([1]);
        storage.Append(
        [
            new Entry
            {
                Index = 3,
                Term = 1,
                Data = ByteString.CopyFromUtf8("value"),
            },
        ]);
        storage.SetHardState(
            new HardState
            {
                Term = 1,
                Commit = 3,
            });
        var node = RaftNode.Restart(
            new RaftConfig
            {
                Id = 1,
                Storage = storage,
                Applied = 2,
                ElectionTick = 10,
                HeartbeatTick = 1,
            });
        try
        {
            Ready ready = await WaitReadyAsync(node);

            Assert.Equal(
                3UL,
                Assert.Single(
                    ready.CommittedEntries).Index);
            Assert.Null(ready.HardState);
            Assert.Empty(ready.Entries);
            await node.AdvanceAsync();
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task OnlyOneReadyWaitOrBatchIsAllowed()
    {
        (RaftNode node, _) = RestartNode();
        try
        {
            using var cancellation =
                new CancellationTokenSource();
            Task<Ready> first = node.WaitForReadyAsync(
                    cancellation.Token)
                .AsTask();

            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await node.WaitForReadyAsync()
                    .AsTask()
                    .WaitAsync(TimeSpan.FromSeconds(2)));

            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await first);

            await node.CampaignAsync();
            Ready ready = await WaitReadyAsync(node);
            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await node.WaitForReadyAsync()
                    .AsTask()
                    .WaitAsync(TimeSpan.FromSeconds(2)));
            await node.AdvanceAsync();
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task CanceledReadyWaitDoesNotAcceptBatch()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode();
        try
        {
            using var cancellation =
                new CancellationTokenSource();
            Task<Ready> canceled = node.WaitForReadyAsync(
                    cancellation.Token)
                .AsTask();
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await canceled);

            await node.CampaignAsync();
            Ready ready = await WaitReadyAsync(node);
            Assert.Equal(
                RaftRole.Candidate,
                ready.SoftState?.Role);
            await PersistAndAdvanceAsync(
                node,
                storage,
                ready);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task MissingAndDuplicateAdvanceFail()
    {
        (RaftNode node, _) = RestartNode();
        try
        {
            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await node.AdvanceAsync()
                    .AsTask());

            await node.CampaignAsync();
            _ = await WaitReadyAsync(node);
            await node.AdvanceAsync();

            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await node.AdvanceAsync()
                    .AsTask());
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task CommittedPaginationRemainsGapFree()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode(maxCommittedSizePerReady: 1);
        try
        {
            await BecomeSingletonLeaderAsync(
                node,
                storage);
            await node.ProposeAsync("a"u8.ToArray());
            await node.ProposeAsync("b"u8.ToArray());
            await node.ProposeAsync("c"u8.ToArray());

            Ready append = await WaitReadyAsync(node);
            Assert.Equal(3, append.Entries.Count);
            await PersistAndAdvanceAsync(
                node,
                storage,
                append);

            var indexes = new List<ulong>();
            for (var page = 0; page < 3; page++)
            {
                Ready ready = await WaitReadyAsync(node);
                Entry entry =
                    Assert.Single(
                        ready.CommittedEntries);
                indexes.Add(entry.Index);
                await PersistAndAdvanceAsync(
                    node,
                    storage,
                    ready);
            }

            Assert.Equal(
                [4UL, 5UL, 6UL],
                indexes);
        }
        finally
        {
            await node.StopAsync();
        }
    }
}
