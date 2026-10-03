using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using static DotnetRaft.Tests.AsyncStorage.AsyncStorageTestSupport;

using DotnetRawNode = DotnetRaft.RawNode;
using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.AsyncStorage;

public sealed class AsyncStorageResponseTests
{
    [Fact]
    public void SynchronousModeRejectsReservedStorageSender()
    {
        DotnetRawNode node =
            DotnetRaft.Tests.RawNode.RawNodeTestSupport
                .CreateNode(
                    DotnetRaft.Tests.RawNode.RawNodeTestSupport
                        .CreateStorage());

        Assert.ThrowsAny<InvalidOperationException>(
            () => node.Step(
                new Message
                {
                    From =
                        RaftLocalMessageTargets.AppendThread,
                    To = 1,
                    Type =
                        MessageType.MsgStorageAppendResp,
                    Term = 1,
                }));
    }

    [Fact]
    public void MalformedLocalResponsesFailBeforeTermHandling()
    {
        (DotnetRawNode node, _) = CreateNode();
        Message[] invalid =
        [
            new Message
            {
                From = 2,
                To = 1,
                Type =
                    MessageType.MsgStorageAppendResp,
                Term = 5,
            },
            new Message
            {
                From =
                    RaftLocalMessageTargets.ApplyThread,
                To = 1,
                Type =
                    MessageType.MsgStorageAppendResp,
                Term = 5,
            },
            new Message
            {
                From =
                    RaftLocalMessageTargets.AppendThread,
                To = 2,
                Type =
                    MessageType.MsgStorageAppendResp,
                Term = 5,
            },
            new Message
            {
                From =
                    RaftLocalMessageTargets.AppendThread,
                To = 1,
                Type =
                    MessageType.MsgStorageAppendResp,
            },
            new Message
            {
                From =
                    RaftLocalMessageTargets.AppendThread,
                To = 1,
                Type =
                    MessageType.MsgStorageAppendResp,
                Term = 5,
                Index = 3,
            },
            new Message
            {
                From =
                    RaftLocalMessageTargets.ApplyThread,
                To = 1,
                Type =
                    MessageType.MsgStorageApplyResp,
                Term = 1,
            },
            new Message
            {
                From =
                    RaftLocalMessageTargets.ApplyThread,
                To = 1,
                Type =
                    MessageType.MsgStorageApplyResp,
                Term = 0,
            },
        ];

        foreach (Message response in invalid)
        {
            Assert.ThrowsAny<InvalidOperationException>(
                () => node.Step(response));
        }

        Assert.Equal(0UL, node.GetBasicStatus().Term);
        Assert.False(node.IsFaultedForTesting);
    }

    [Fact]
    public void AppendResponseTermPreventsAbaTruncation()
    {
        (DotnetRawNode node, _) = CreateNode();
        node.Core.BecomeFollower(1, 0);
        node.Core.Log.Append(
        [
            EntryAt(3, 1),
        ]);
        Message first = StorageResponse(
            node.Ready(),
            MessageType.MsgStorageAppendResp);

        node.Core.BecomeFollower(2, 0);
        node.Core.Log.Append(
        [
            EntryAt(3, 2),
        ]);
        Message second = StorageResponse(
            node.Ready(),
            MessageType.MsgStorageAppendResp);

        node.Core.BecomeFollower(3, 0);
        node.Core.Log.Append(
        [
            EntryAt(3, 1),
        ]);
        _ = node.Ready();

        node.Core.BecomeFollower(4, 0);
        Ready currentReady = node.Ready();
        Message current = StorageResponse(
            currentReady,
            MessageType.MsgStorageAppendResp);
        Assert.Empty(
            StorageMessage(
                currentReady,
                MessageType.MsgStorageAppend).Entries);

        node.Step(first);
        node.Step(second);
        Assert.Equal(3UL, node.Core.Log.Unstable.Offset);

        node.Step(current);
        Assert.Equal(4UL, node.Core.Log.Unstable.Offset);
    }

    [Fact]
    public void SnapshotCompletionReassertsMatchingConfiguration()
    {
        var trace = new RecordingTraceSink();
        (DotnetRawNode node, _) = CreateNode(
            voters: [1, 2],
            traceSink: trace);
        Snapshot snapshot = SnapshotAt(
            5,
            [1UL, 2UL]);
        node.Step(
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgSnap,
                Term = 1,
                Snapshot = snapshot,
            });
        Message response = StorageResponse(
            node.Ready(),
            MessageType.MsgStorageAppendResp);

        node.Core.BecomeFollower(2, 0);
        node.ApplyConfChange(
            new ProtocolConfChange
            {
                Type =
                    ConfChangeType.ConfChangeRemoveNode,
                NodeId = 2,
            });
        Assert.Equal(
            [1UL],
            node.GetStatus().Configuration.Voters);
        trace.Events.Clear();

        node.Step(response);

        Assert.Equal(
            [1UL, 2UL],
            node.GetStatus().Configuration.Voters);
        Assert.Equal(5UL, node.GetBasicStatus().Applied);
        Assert.False(node.Core.Log.HasUnstableSnapshot);
        Assert.Contains(
            trace.Events,
            traceEvent =>
                traceEvent.Type
                == RaftTraceEventType.ConfigurationApplied);
    }

    [Fact]
    public void SnapshotCompletionReassertsLearnerStateAndClearsVotes()
    {
        (DotnetRawNode node, _) = CreateNode(voters: [1, 2]);
        var state = new ConfState();
        state.Voters.Add(2);
        state.Learners.Add(1);
        var snapshot = new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = 5,
                Term = 1,
                ConfState = state,
            },
        };
        node.Step(
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgSnap,
                Term = 1,
                Snapshot = snapshot,
            });
        Message response = StorageResponse(
            node.Ready(),
            MessageType.MsgStorageAppendResp);
        Assert.True(node.Core.IsLearner);

        node.ApplyConfChange(
            new ProtocolConfChange
            {
                Type =
                    ConfChangeType.ConfChangeAddNode,
                NodeId = 1,
            });
        node.Core.Tracker.RecordVote(2, granted: true);
        Assert.False(node.Core.IsLearner);
        Assert.NotEmpty(node.Core.Tracker.Votes);

        node.Step(response);

        Assert.True(node.Core.IsLearner);
        Assert.Empty(node.Core.Tracker.Votes);
        Assert.Equal(
            [1UL],
            node.GetStatus().Configuration.Learners);
    }

    [Fact]
    public void OldSnapshotResponseDoesNotOverwriteNewConfiguration()
    {
        (DotnetRawNode node, _) = CreateNode(voters: [1, 2, 3]);
        node.Step(
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgSnap,
                Term = 1,
                Snapshot = SnapshotAt(
                    5,
                    [1UL, 2UL]),
            });
        Message oldResponse = StorageResponse(
            node.Ready(),
            MessageType.MsgStorageAppendResp);

        node.Step(
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgSnap,
                Term = 1,
                Snapshot = SnapshotAt(
                    6,
                    [1UL, 2UL, 3UL]),
            });
        Message currentResponse = StorageResponse(
            node.Ready(),
            MessageType.MsgStorageAppendResp);
        node.ApplyConfChange(
            new ProtocolConfChange
            {
                Type =
                    ConfChangeType.ConfChangeRemoveNode,
                NodeId = 3,
            });

        node.Step(oldResponse);
        Assert.Equal(
            [1UL, 2UL],
            node.GetStatus().Configuration.Voters);
        Assert.True(node.Core.Log.HasUnstableSnapshot);

        node.Step(currentResponse);
        Assert.Equal(
            [1UL, 2UL, 3UL],
            node.GetStatus().Configuration.Voters);
        Assert.Equal(6UL, node.GetBasicStatus().Applied);
    }

    [Fact]
    public void OlderApplyResponseAfterSnapshotReleasesAccounting()
    {
        var state = new ConfState();
        state.Voters.Add(1);
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
        storage.Append([EntryAt(3, 1)]);
        storage.SetHardState(
            new HardState
            {
                Term = 1,
                Commit = 3,
            });
        var node = new DotnetRawNode(
            new RaftConfig
            {
                Id = 1,
                Storage = storage,
                Applied = 2,
                AsyncStorageWrites = true,
            });
        Ready applyReady = node.Ready();
        Message applyResponse =
            StorageMessage(
                applyReady,
                MessageType.MsgStorageApply)
            .Responses[0]
            .Clone();
        Assert.True(
            node.Core.Log.ApplyingEntriesSize > 0);

        node.Step(
            new Message
            {
                From = 1,
                To = 1,
                Type = MessageType.MsgSnap,
                Term = 1,
                Snapshot = SnapshotAt(
                    5,
                    [1UL]),
            });
        Message snapshotResponse = StorageResponse(
            node.Ready(),
            MessageType.MsgStorageAppendResp);
        node.Step(snapshotResponse);
        Assert.Equal(5UL, node.GetBasicStatus().Applied);

        node.Step(applyResponse);
        Assert.Equal(5UL, node.GetBasicStatus().Applied);
        Assert.Equal(
            0UL,
            node.Core.Log.ApplyingEntriesSize);
    }

    private static Entry EntryAt(
        ulong index,
        ulong term)
    {
        return new Entry
        {
            Index = index,
            Term = term,
        };
    }

    private static Snapshot SnapshotAt(
        ulong index,
        IEnumerable<ulong> voters)
    {
        var state = new ConfState();
        state.Voters.Add(voters);
        return new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = index,
                Term = 1,
                ConfState = state,
            },
        };
    }

    private sealed class RecordingTraceSink : IRaftTraceSink
    {
        internal List<RaftTraceEvent> Events { get; } = [];

        public void Trace(RaftTraceEvent traceEvent)
        {
            Events.Add(traceEvent);
        }
    }

    private static Message StorageResponse(
        Ready ready,
        MessageType type)
    {
        return Assert.Single(
                StorageMessage(
                    ready,
                    MessageType.MsgStorageAppend)
                .Responses,
                message => message.Type == type)
            .Clone();
    }
}
