using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using static DotnetRaft.Tests.Node.RaftNodeTestSupport;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Node;

public sealed class RaftNodeCommandTests
{
    [Fact]
    public async Task ProposalBlocksUntilLeaderAndOwnsBytes()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode();
        try
        {
            byte[] payload = "original"u8.ToArray();
            Task proposal = node.ProposeAsync(payload)
                .AsTask();
            payload[0] = (byte)'X';
            await Task.Yield();
            Assert.False(proposal.IsCompleted);

            await node.CampaignAsync();
            Ready election = await WaitReadyAsync(node);
            await PersistAndAdvanceAsync(
                node,
                storage,
                election);
            await proposal.WaitAsync(
                TimeSpan.FromSeconds(2));

            Ready leadership = await WaitReadyAsync(node);
            Assert.Equal(2, leadership.Entries.Count);
            Assert.Equal(
                ByteString.CopyFromUtf8("original"),
                leadership.Entries[^1].Data);
            await PersistAndAdvanceAsync(
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
    public async Task CanceledBlockedProposalNeverDispatches()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode();
        try
        {
            using var cancellation =
                new CancellationTokenSource();
            Task proposal = node.ProposeAsync(
                    "value"u8.ToArray(),
                    cancellation.Token)
                .AsTask();
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await proposal);

            await node.CampaignAsync();
            Ready election = await WaitReadyAsync(node);
            await PersistAndAdvanceAsync(
                node,
                storage,
                election);

            Ready leadership = await WaitReadyAsync(node);
            Assert.Single(leadership.Entries);
            await PersistAndAdvanceAsync(
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
    public async Task BlockedProposalBacklogDoesNotDelayControlOrTicks()
    {
        (RaftNode node, _) = RestartNode(
            electionTick: 2);
        using var cancellation =
            new CancellationTokenSource();
        try
        {
            Task[] proposals =
                Enumerable.Range(0, 500)
                    .Select(index =>
                        node.ProposeAsync(
                                BitConverter.GetBytes(index),
                                cancellation.Token)
                            .AsTask())
                    .ToArray();

            Status status = await node.GetStatusAsync()
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(2));
            Assert.Equal(RaftRole.Follower, status.Basic.Role);

            for (var tick = 0; tick < 4; tick++)
            {
                node.Tick();
            }

            Ready ready = await WaitReadyAsync(node);
            Assert.Equal(
                RaftRole.Candidate,
                ready.SoftState?.Role);

            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await Task.WhenAll(proposals));
            Assert.All(
                proposals,
                proposal => Assert.True(proposal.IsCanceled));
        }
        finally
        {
            cancellation.Cancel();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task ClaimWinsOverLaterCancellation()
    {
        var trace = new BlockingTraceSink();
        (RaftNode node, _) = RestartNode(
            traceSink: trace);
        try
        {
            using var cancellation =
                new CancellationTokenSource();
            Task campaign = node.CampaignAsync(
                    cancellation.Token)
                .AsTask();
            Assert.True(
                trace.Entered.Wait(
                    TimeSpan.FromSeconds(2)));

            cancellation.Cancel();
            trace.Release.Set();

            await campaign.WaitAsync(
                TimeSpan.FromSeconds(2));
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task LocalMessagesAndUnknownResponsesAreIgnored()
    {
        (RaftNode node, _) = RestartNode(
            voters: [1, 2]);
        try
        {
            await node.StepAsync(
                new Message
                {
                    From = 2,
                    To = 1,
                    Type = MessageType.MsgHup,
                });
            await node.StepAsync(
                new Message
                {
                    From = 99,
                    To = 1,
                    Type = MessageType.MsgAppResp,
                    Term = 10,
                });

            Status status =
                await node.GetStatusAsync();
            Assert.Equal(0UL, status.Basic.Term);
            Assert.Equal(RaftRole.Follower, status.Basic.Role);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task ReservedStorageSenderFailsUntilD25()
    {
        (RaftNode node, _) = RestartNode();
        try
        {
            await Assert.ThrowsAsync<NotSupportedException>(
                async () => await node.StepAsync(
                    new Message
                    {
                        From = ulong.MaxValue,
                        To = 1,
                        Type =
                            MessageType.MsgStorageAppendResp,
                    }).AsTask());
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task StepOwnsMessageBeforeDispatch()
    {
        (RaftNode node, _) = RestartNode(
            voters: [1, 2]);
        try
        {
            var message = new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgHeartbeat,
                Term = 1,
            };
            ValueTask step = node.StepAsync(message);
            message.From = 1;
            message.Type = MessageType.MsgHup;
            message.Term = 0;
            await step;

            Status status =
                await node.GetStatusAsync();
            Assert.Equal(1UL, status.Basic.Term);
            Assert.Equal(2UL, status.Basic.LeaderId);
            Assert.Equal(RaftRole.Follower, status.Basic.Role);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task MsgPropNormalizesSenderAndWaitsForDispatch()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode(voters: [1, 2]);
        try
        {
            await node.StepAsync(
                new Message
                {
                    From = 2,
                    To = 1,
                    Type = MessageType.MsgHeartbeat,
                    Term = 1,
                });
            Ready heartbeat = await WaitReadyAsync(node);
            await PersistAndAdvanceAsync(
                node,
                storage,
                heartbeat);

            var proposal = new Message
            {
                From = 99,
                To = 1,
                Type = MessageType.MsgProp,
            };
            proposal.Entries.Add(
                new Entry
                {
                    Data =
                        ByteString.CopyFromUtf8("value"),
                });
            await node.StepAsync(proposal);

            Ready ready = await WaitReadyAsync(node);
            Message forwarded = Assert.Single(
                ready.Messages,
                message =>
                    message.Type
                    == MessageType.MsgProp);
            Assert.Equal(1UL, forwarded.From);
            Assert.Equal(2UL, forwarded.To);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task LearnerProgressDoesNotDisableProposals()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode(
                voters: [2],
                learners: [1]);
        try
        {
            await node.StepAsync(
                new Message
                {
                    From = 2,
                    To = 1,
                    Type = MessageType.MsgHeartbeat,
                    Term = 1,
                });
            Ready heartbeat = await WaitReadyAsync(node);
            await PersistAndAdvanceAsync(
                node,
                storage,
                heartbeat);

            await node.ProposeAsync("value"u8.ToArray());
            Ready ready = await WaitReadyAsync(node);
            Assert.Contains(
                ready.Messages,
                message =>
                    message.Type
                    == MessageType.MsgProp
                    && message.To == 2);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task RemovalBlocksAndReadditionEnablesProposals()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode(voters: [1, 2]);
        try
        {
            await node.StepAsync(
                new Message
                {
                    From = 2,
                    To = 1,
                    Type = MessageType.MsgHeartbeat,
                    Term = 1,
                });
            Ready heartbeat = await WaitReadyAsync(node);
            await PersistAndAdvanceAsync(
                node,
                storage,
                heartbeat);
            await node.ApplyConfChangeAsync(
                new ProtocolConfChange
                {
                    Type =
                        ConfChangeType.ConfChangeRemoveNode,
                    NodeId = 1,
                });

            using var cancellation =
                new CancellationTokenSource();
            Task blocked = node.ProposeAsync(
                    "blocked"u8.ToArray(),
                    cancellation.Token)
                .AsTask();
            await Task.Yield();
            Assert.False(blocked.IsCompleted);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await blocked);

            await node.ApplyConfChangeAsync(
                new ProtocolConfChange
                {
                    Type =
                        ConfChangeType.ConfChangeAddNode,
                    NodeId = 1,
                });
            await node.ProposeAsync(
                "accepted"u8.ToArray());
            Ready forwarded = await WaitReadyAsync(node);
            Assert.Contains(
                forwarded.Messages,
                message =>
                    message.Type
                    == MessageType.MsgProp
                    && message.To == 2);
        }
        finally
        {
            await node.StopAsync();
        }
    }
}
