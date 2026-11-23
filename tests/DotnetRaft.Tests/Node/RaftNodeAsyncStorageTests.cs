using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using static DotnetRaft.Tests.AsyncStorage.AsyncStorageTestSupport;
using static DotnetRaft.Tests.Node.RaftNodeTestSupport;

namespace DotnetRaft.Tests.Node;

public sealed class RaftNodeAsyncStorageTests
{
    [Fact]
    public async Task AsyncNodeDoesNotUseAdvance()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartAsyncNode();
        try
        {
            await node.CampaignAsync();
            Ready election = await WaitReadyAsync(node);

            NotSupportedException exception =
                await Assert.ThrowsAsync<NotSupportedException>(
                async () => await node.AdvanceAsync()
                    .AsTask());
            Assert.Equal(
                "Advance is replaced by storage response messages when asynchronous storage writes are enabled.",
                exception.Message);
            await ProcessReadyAsync(
                node,
                storage,
                election);

            Ready leadership = await WaitReadyAsync(node);
            Assert.Single(leadership.Entries);
            await ProcessReadyAsync(
                node,
                storage,
                leadership);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task SuccessiveReadyWaitsPipelineWithoutAdvance()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartAsyncNode();
        try
        {
            await BecomeLeaderAsync(
                node,
                storage);

            await node.ProposeAsync("one"u8.ToArray());
            Ready first = await WaitReadyAsync(node);
            await node.ProposeAsync("two"u8.ToArray());
            Ready second = await WaitReadyAsync(node);

            Assert.Equal(
                "one",
                first.Entries[0].Data.ToStringUtf8());
            Assert.Equal(
                "two",
                second.Entries[0].Data.ToStringUtf8());
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task ValidStorageWorkIsSerialized()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartAsyncNode();
        try
        {
            await node.CampaignAsync();
            Ready ready = await WaitReadyAsync(node);
            await ProcessReadyAsync(node, storage, ready);

            Status status =
                await node.GetStatusAsync();
            Assert.Equal(
                RaftRole.Leader,
                status.Basic.Role);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task FixtureStorageResponseWithWrongSenderIsNonterminal()
    {
        (RaftNode node, _) =
            RestartAsyncNode();
        try
        {
            await Assert.ThrowsAnyAsync<InvalidOperationException>(
                async () => await node.StepFixtureAsync(
                    new Message
                    {
                        From = 2,
                        To = 1,
                        Type =
                            MessageType.MsgStorageAppendResp,
                        Term = 1,
                    }).AsTask());

            Assert.False(node.Completion.IsCompleted);
            _ = await node.GetStatusAsync();
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task FixtureStorageResponseWithWrongTypeIsNonterminal()
    {
        (RaftNode node, _) =
            RestartAsyncNode();
        try
        {
            await Assert.ThrowsAsync<StorageResponseValidationException>(
                async () => await node.StepFixtureAsync(
                    new Message
                    {
                        From =
                            RaftLocalMessageTargets
                                .ApplyThread,
                        To = 1,
                        Type = MessageType.MsgAppResp,
                    }).AsTask());

            Assert.False(node.Completion.IsCompleted);
            _ = await node.GetStatusAsync();
        }
        finally
        {
            await node.StopAsync();
        }
    }

    private static (
        RaftNode Node,
        MemoryStorage Storage) RestartAsyncNode()
    {
        MemoryStorage storage =
            CreateStorage([1]);
        var node = RaftNode.Restart(
            new RaftConfig
            {
                Id = 1,
                ElectionTick = 10,
                HeartbeatTick = 1,
                Storage = storage,
                Applied = 2,
                AsyncStorageWrites = true,
            });
        return (node, storage);
    }

    private static async Task ProcessReadyAsync(
        RaftNode node,
        MemoryStorage storage,
        Ready ready)
    {
        // This helper has no network; only use it for singleton batches.
        Assert.Empty(ready.Messages);
        if (ready.AppendWork is { } append)
        {
            PersistAppend(storage, append);
            foreach (Message response in append.MessagesAfterPersist)
            {
                Assert.Equal(1UL, response.To);
                await node.StepAsync(response);
            }

            if (append.Completion is { } completion)
            {
                await node.CompleteStorageWorkAsync(completion);
            }
        }

        if (ready.ApplyWork is { } apply)
        {
            Assert.All(apply.Entries, entry => Assert.Equal(EntryType.EntryNormal, entry.Type));
            await node.CompleteStorageWorkAsync(apply.Completion);
        }
    }

    private static async Task BecomeLeaderAsync(
        RaftNode node,
        MemoryStorage storage)
    {
        await node.CampaignAsync();
        await ProcessReadyAsync(
            node,
            storage,
            await WaitReadyAsync(node));
        await ProcessReadyAsync(
            node,
            storage,
            await WaitReadyAsync(node));
        await ProcessReadyAsync(
            node,
            storage,
            await WaitReadyAsync(node));
    }
}
