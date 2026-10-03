using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Interaction;

public sealed class InteractionEnvironmentTests
{
    [Fact]
    public void NodesUseDeterministicElectionTimeouts()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                Voters = [1],
                SnapshotIndex = 2,
            });

        InteractionNode node =
            Assert.Single(environment.Nodes);
        Assert.Equal(
            node.Config.ElectionTick,
            node.RawNode.Core.RandomizedElectionTimeout);

        node.RawNode
            .SetRandomizedElectionTimeoutForTesting(7);

        Assert.Equal(
            7,
            node.RawNode.Core.RandomizedElectionTimeout);
    }

    [Fact]
    public void ReadyProcessingPersistsAndAppliesInOrder()
    {
        var environment = CreateSingleton(
            ByteString.CopyFromUtf8("seed"));
        environment.Campaign(0);
        environment.Stabilize();

        environment.Propose(
            0,
            "value"u8);
        environment.Stabilize();

        InteractionNode node = environment.Nodes[0];
        Snapshot application =
            node.GetApplicationSnapshot();
        Assert.Equal(4UL, application.Metadata.Index);
        Assert.Equal(1UL, application.Metadata.Term);
        Assert.Equal(
            ByteString.CopyFromUtf8("seedvalue"),
            application.Data);
        Assert.Equal(
            [1UL],
            application.Metadata.ConfState.Voters);

        StorageState state =
            node.Storage.GetInitialState();
        Assert.Equal(4UL, state.HardState?.Commit);
        Assert.Equal(
            [3UL, 4UL],
            node.Storage.GetEntries(
                    3,
                    5,
                    ulong.MaxValue)
                .Select(entry => entry.Index));
    }

    [Fact]
    public void ReadyProcessingHandlesCommittedPagination()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                Voters = [1],
                SnapshotIndex = 2,
                MaxCommittedSizePerReady = 1,
            });
        environment.Campaign(0);
        environment.Stabilize();

        environment.Propose(0, "a"u8);
        environment.Propose(0, "b"u8);
        environment.Propose(0, "c"u8);
        environment.Stabilize();

        Snapshot application =
            environment.Nodes[0]
                .GetApplicationSnapshot();
        Assert.Equal(6UL, application.Metadata.Index);
        Assert.Equal(
            ByteString.CopyFromUtf8("abc"),
            application.Data);
    }

    [Fact]
    public void MessageDropPreservesUnselectedOrder()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            3,
            new InteractionNodeOptions());
        var first = new Message
        {
            From = 1,
            To = 2,
            Type = MessageType.MsgHeartbeat,
        };
        var middle = new Message
        {
            From = 1,
            To = 3,
            Type = MessageType.MsgHeartbeat,
        };
        var last = new Message
        {
            From = 1,
            To = 2,
            Type = MessageType.MsgHeartbeat,
            Commit = 1,
        };
        environment.QueueMessageForTesting(first);
        environment.QueueMessageForTesting(middle);
        environment.QueueMessageForTesting(last);
        first.To = 99;

        int handled = environment.DeliverMessages(
            MessageType.MsgHeartbeat,
            new InteractionRecipient(
                2,
                Drop: true));

        Assert.Equal(2, handled);
        Message remaining =
            Assert.Single(
                environment.QueuedMessages);
        Assert.Equal(3UL, remaining.To);
    }

    [Fact]
    public void LocalMessagesCannotBeDropped()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            1,
            new InteractionNodeOptions());
        environment.QueueMessageForTesting(
            new Message
            {
                From = 1,
                To = 1,
                Type = MessageType.MsgHeartbeat,
            });

        int handled = environment.DeliverMessages(
            type: null,
            new InteractionRecipient(
                1,
                Drop: true));

        Assert.Equal(0, handled);
        Assert.Single(environment.QueuedMessages);
    }

    [Fact]
    public void UnknownResponseIsReportedAndDeliveryContinues()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                Voters = [1],
                SnapshotIndex = 2,
            });
        environment.QueueMessageForTesting(
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgAppResp,
                Term = 1,
                Index = 2,
            });

        int handled = environment.DeliverMessages(
            type: null,
            new InteractionRecipient(1));

        Assert.Equal(1, handled);
        Assert.Empty(environment.QueuedMessages);
        Assert.Contains(
            "Response sender 2 is not a known peer.",
            environment.CurrentOutput,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ReservedSenderRemainsFatal()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                Voters = [1],
                SnapshotIndex = 2,
            });
        environment.QueueMessageForTesting(
            new Message
            {
                From = ulong.MaxValue,
                To = 1,
                Type = MessageType.MsgAppResp,
            });

        Assert.ThrowsAny<InvalidOperationException>(
            () => environment.DeliverMessages(
                type: null,
                new InteractionRecipient(1)));
    }

    [Fact]
    public void FaultedDestinationDoesNotUseSafeResponsePath()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                Voters = [1],
                SnapshotIndex = 2,
            });
        InteractionNode node = environment.Nodes[0];
        Assert.ThrowsAny<Exception>(
            () => node.RawNode.ApplyConfChange(
                new ProtocolConfChange
                {
                    Type =
                        ConfChangeType.ConfChangeAddLearnerNode,
                    NodeId = 1,
                }));
        Assert.True(
            node.RawNode.IsFaultedForTesting);
        environment.QueueMessageForTesting(
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgAppResp,
            });

        InvalidOperationException exception =
            Assert.ThrowsAny<InvalidOperationException>(
                () => environment.DeliverMessages(
                    type: null,
                    new InteractionRecipient(1)));

        Assert.Contains(
            "faulted",
            exception.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SafeResponseRejectionsConsumeStabilizationBudget()
    {
        var environment =
            new InteractionEnvironment(
                stabilizationLimit: 1);
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                Voters = [1],
                SnapshotIndex = 2,
            });
        environment.QueueMessageForTesting(
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgAppResp,
            });
        environment.QueueMessageForTesting(
            new Message
            {
                From = 3,
                To = 1,
                Type = MessageType.MsgAppResp,
            });

        string result = environment.Handle(
            "stabilize",
            string.Empty);

        Assert.Contains(
            "2->1 MsgAppResp",
            result,
            StringComparison.Ordinal);
        Assert.Contains(
            "Response sender 2 is not a known peer.",
            result,
            StringComparison.Ordinal);
        Assert.Contains(
            "limit 1",
            result,
            StringComparison.Ordinal);
        Message remaining =
            Assert.Single(
                environment.QueuedMessages);
        Assert.Equal(3UL, remaining.From);
    }

    [Fact]
    public void FatalDeliveryPreservesCurrentMessageAndSuffix()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                Voters = [1],
                SnapshotIndex = 2,
            });
        environment.QueueMessageForTesting(
            new Message
            {
                From = ulong.MaxValue,
                To = 1,
                Type = MessageType.MsgAppResp,
            });
        environment.QueueMessageForTesting(
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgHeartbeat,
            });

        Assert.ThrowsAny<InvalidOperationException>(
            () => environment.DeliverMessages(
                type: null,
                new InteractionRecipient(1)));

        Assert.Equal(
            [ulong.MaxValue, 2UL],
            environment.QueuedMessages.Select(
                message => message.From));
    }

    [Fact]
    public void CompactionCannotPassPhysicalApplication()
    {
        var environment = CreateSingleton();
        environment.Campaign(0);
        environment.Stabilize();
        environment.Propose(0, "value"u8);
        environment.Stabilize();
        InteractionNode node = environment.Nodes[0];
        ulong firstBefore =
            node.Storage.GetFirstIndex();

        Assert.Throws<InvalidOperationException>(
            () => environment.Compact(0, 5));

        Assert.Equal(
            firstBefore,
            node.Storage.GetFirstIndex());

        environment.Compact(0, 3);
        Assert.Equal(
            4UL,
            node.Storage.GetFirstIndex());
    }

    [Fact]
    public void SnapshotMessagesUsePhysicalStateAndAreDetached()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            2,
            new InteractionNodeOptions
            {
                Voters = [1, 2],
                SnapshotIndex = 2,
            });
        environment.Campaign(0);
        environment.Stabilize();
        environment.Propose(0, "value"u8);
        environment.Stabilize();

        environment.SendSnapshot(0, 1);

        Message queued =
            Assert.Single(
                environment.QueuedMessages);
        Assert.Equal(MessageType.MsgSnap, queued.Type);
        Assert.Equal(1UL, queued.From);
        Assert.Equal(2UL, queued.To);
        Assert.Equal(
            environment.Nodes[0]
                .RawNode.GetBasicStatus().Term,
            queued.Term);
        Assert.Equal(
            environment.Nodes[0]
                .GetApplicationSnapshot(),
            queued.Snapshot);

        queued.Snapshot!.Metadata.Index = 99;
        Assert.NotEqual(
            99UL,
            Assert.Single(
                    environment.QueuedMessages)
                .Snapshot!.Metadata.Index);
    }

    [Fact]
    public void SelectedStabilizationLeavesOtherMessagesQueued()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            3,
            new InteractionNodeOptions
            {
                Voters = [1, 2, 3],
                SnapshotIndex = 2,
            });
        environment.Campaign(0);

        environment.Stabilize(0, 1);

        Assert.Contains(
            environment.QueuedMessages,
            message => message.To == 3);
    }

    [Fact]
    public void StabilizationLimitFailsDeterministically()
    {
        var environment =
            new InteractionEnvironment(
                stabilizationLimit: 1);
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                Voters = [1],
                SnapshotIndex = 2,
            });
        environment.Campaign(0);

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(
                () => environment.Stabilize());

        Assert.Contains(
            "limit 1",
            exception.Message,
            StringComparison.Ordinal);
        Assert.Contains(
            "selected IDs: 1",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void TickCommandsUseConfiguredIntervals()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                Voters = [1],
                SnapshotIndex = 2,
            });

        environment.TickHeartbeat(0);
        Assert.False(
            environment.Nodes[0].RawNode.HasReady());

        environment.TickElection(0);
        Assert.True(
            environment.Nodes[0].RawNode.HasReady());
    }

    [Fact]
    public void StatusUsesPinnedSuffixOrder()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            2,
            new InteractionNodeOptions
            {
                Voters = [1, 2],
                SnapshotIndex = 2,
                MaxInflightMessages = 2,
            });
        DotnetRaft.RawNode node =
            environment.Nodes[0].RawNode;
        node.Core.BecomeCandidate();
        node.Core.BecomeLeader();
        DotnetRaft.Tracker.Progress peer =
            node.Core.Tracker.Progress[2];
        peer.BecomeReplicate();
        peer.SentEntries(1, 1);
        peer.SentEntries(1, 1);

        environment.Status(0);

        Assert.Contains(
            "2: Replicate match=0 next=3 paused inactive inflight=2[full]\n",
            environment.CurrentOutput,
            StringComparison.Ordinal);
    }

    private static InteractionEnvironment CreateSingleton(
        ByteString? data = null)
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                Voters = [1],
                SnapshotIndex = 2,
                SnapshotData = data ?? ByteString.Empty,
            });
        return environment;
    }
}
