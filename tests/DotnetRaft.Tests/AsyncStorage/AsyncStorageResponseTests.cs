using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

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
            () => node.StepFixture(
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
                () => node.StepFixture(response));
        }

        Assert.Equal(0UL, node.GetBasicStatus().Term);
        Assert.False(node.IsFaultedForTesting);
    }

    [Fact]
    public void AppendReceiptTermPreventsAbaTruncation()
    {
        (DotnetRawNode node, MemoryStorage storage) = CreateNode();
        node.Core.BecomeFollower(1, 0);
        node.Core.Log.Append(
        [
            EntryAt(3, 1),
        ]);
        StorageAppendWork first = Assert.IsType<StorageAppendWork>(node.Ready().AppendWork);

        node.Core.BecomeFollower(2, 0);
        node.Core.Log.Append(
        [
            EntryAt(3, 2),
        ]);
        StorageAppendWork second = Assert.IsType<StorageAppendWork>(node.Ready().AppendWork);

        node.Core.BecomeFollower(3, 0);
        node.Core.Log.Append(
        [
            EntryAt(3, 1),
        ]);
        StorageAppendWork restored = Assert.IsType<StorageAppendWork>(node.Ready().AppendWork);

        node.Core.BecomeFollower(4, 0);
        StorageAppendWork current = Assert.IsType<StorageAppendWork>(node.Ready().AppendWork);
        Assert.Empty(current.Entries);

        CompleteAppend(first);
        Assert.Equal(3UL, node.Core.Log.Unstable.Offset);
        CompleteAppend(second);
        Assert.Equal(3UL, node.Core.Log.Unstable.Offset);
        CompleteAppend(restored);
        Assert.Equal(3UL, node.Core.Log.Unstable.Offset);

        CompleteAppend(current);
        Assert.Equal(4UL, node.Core.Log.Unstable.Offset);
        Assert.Equal(1UL, storage.GetTerm(3));

        void CompleteAppend(StorageAppendWork append)
        {
            Assert.Empty(append.MessagesAfterPersist);
            PersistAppend(storage, append);
            node.CompleteStorageWork(Assert.IsAssignableFrom<StorageCompletion>(append.Completion));
        }
    }

    [Fact]
    public void SnapshotCompletionReassertsMatchingConfiguration()
    {
        var trace = new RecordingTraceSink();
        (DotnetRawNode node, MemoryStorage storage) = CreateNode(
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
        StorageAppendWork append = Assert.IsType<StorageAppendWork>(node.Ready().AppendWork);

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

        PersistAppend(storage, append);
        node.CompleteStorageWork(Assert.IsAssignableFrom<StorageCompletion>(append.Completion));

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
        (DotnetRawNode node, MemoryStorage storage) = CreateNode(voters: [1, 2]);
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
        StorageAppendWork append = Assert.IsType<StorageAppendWork>(node.Ready().AppendWork);
        Assert.True(node.Core.Tracker.IsLearner(node.Core.Id));

        node.ApplyConfChange(
            new ProtocolConfChange
            {
                Type =
                    ConfChangeType.ConfChangeAddNode,
                NodeId = 1,
            });
        node.Core.Tracker.RecordVote(2, granted: true);
        Assert.False(node.Core.Tracker.IsLearner(node.Core.Id));
        Assert.NotEmpty(node.Core.Tracker.Votes);

        PersistAppend(storage, append);
        node.CompleteStorageWork(Assert.IsAssignableFrom<StorageCompletion>(append.Completion));

        Assert.True(node.Core.Tracker.IsLearner(node.Core.Id));
        Assert.Empty(node.Core.Tracker.Votes);
        Assert.Equal(
            [1UL],
            node.GetStatus().Configuration.Learners);
    }

    [Fact]
    public void OldSnapshotReceiptDoesNotOverwriteNewConfiguration()
    {
        (DotnetRawNode node, MemoryStorage storage) = CreateNode(voters: [1, 2, 3]);
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
        StorageAppendWork older = Assert.IsType<StorageAppendWork>(node.Ready().AppendWork);

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
        StorageAppendWork current = Assert.IsType<StorageAppendWork>(node.Ready().AppendWork);
        node.ApplyConfChange(
            new ProtocolConfChange
            {
                Type =
                    ConfChangeType.ConfChangeRemoveNode,
                NodeId = 3,
            });

        PersistAppend(storage, older);
        node.CompleteStorageWork(Assert.IsAssignableFrom<StorageCompletion>(older.Completion));
        Assert.Equal(
            [1UL, 2UL],
            node.GetStatus().Configuration.Voters);
        Assert.True(node.Core.Log.HasUnstableSnapshot);

        PersistAppend(storage, current);
        node.CompleteStorageWork(Assert.IsAssignableFrom<StorageCompletion>(current.Completion));
        Assert.Equal(
            [1UL, 2UL, 3UL],
            node.GetStatus().Configuration.Voters);
        Assert.Equal(6UL, node.GetBasicStatus().Applied);
    }

    [Fact]
    public void OlderApplyReceiptAfterSnapshotReleasesAccounting()
    {
        var state = new ConfState();
        state.Voters.Add(1);
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
        storage.Append([EntryAt(3, 1)]);
        storage.SetHardState(
            new RaftHardState
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
        StorageApplyWork apply = Assert.IsType<StorageApplyWork>(node.Ready().ApplyWork);
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
        StorageAppendWork append = Assert.IsType<StorageAppendWork>(node.Ready().AppendWork);
        PersistAppend(storage, append);
        node.CompleteStorageWork(Assert.IsAssignableFrom<StorageCompletion>(append.Completion));
        Assert.Equal(5UL, node.GetBasicStatus().Applied);

        node.CompleteStorageWork(apply.Completion);
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
        state.Voters.AddRange(voters);
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
}
