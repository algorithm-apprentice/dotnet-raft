using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Tests.AsyncStorage;

using Google.Protobuf;

using static DotnetRaft.Tests.RawNode.RawNodeTestSupport;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.RawNode;

public sealed class RawNodeReadyTests
{
    [Fact]
    public void SingletonSelfAppendWaitsForAdvance()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(storage);

        node.Campaign();
        Ready election = node.Ready();
        PersistAndAdvance(node, storage, election);
        Ready ready = node.Ready();

        Assert.True(ready.MustSync);
        Assert.Single(ready.Entries);
        Assert.Empty(ready.Messages);
        Assert.Equal(0UL, node.Core.Log.Committed);

        Persist(storage, ready);
        Assert.Equal(0UL, node.Core.Log.Committed);

        node.Advance(ready);

        Assert.Equal(1UL, node.Core.Log.Committed);
        Assert.True(node.HasReady());
    }

    [Fact]
    public void ReadyRequiresExactOutstandingInstance()
    {
        MemoryStorage firstStorage = CreateStorage();
        var first = CreateNode(firstStorage);
        first.Campaign();
        Ready outstanding = first.Ready();

        Assert.False(first.HasReady());
        Assert.Throws<InvalidOperationException>(
            () => first.Ready());

        var second = CreateNode(CreateStorage());
        second.Campaign();
        Ready wrong = second.Ready();

        Assert.Throws<InvalidOperationException>(
            () => first.Advance(wrong));

        Persist(firstStorage, outstanding);
        first.Advance(outstanding);
        Assert.Throws<InvalidOperationException>(
            () => first.Advance(outstanding));
    }

    [Fact]
    public void WorkAddedAfterAcceptanceSurvivesOlderAdvance()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(storage);
        node.Campaign();
        Ready vote = node.Ready();
        PersistAndAdvance(node, storage, vote);
        Ready election = node.Ready();

        node.Propose("later"u8);
        Assert.False(node.HasReady());

        PersistAndAdvance(node, storage, election);

        Ready later = node.Ready();
        Assert.Contains(
            later.Entries,
            entry =>
                entry.Index == 2
                && entry.Data ==
                    ByteString.CopyFromUtf8("later"));
        Assert.Contains(
            later.CommittedEntries,
            entry => entry.Index == 1);
    }

    [Fact]
    public void ReturnedMutationCannotChangeCoreOrAcknowledgement()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(storage);
        node.Campaign();
        Ready election = node.Ready();
        HardState persistedState =
            election.HardState!.Clone();
        election.HardState.Term = 99;
        storage.SetHardState(persistedState);
        node.Advance(election);
        Ready ready = node.Ready();
        Entry persistedEntry = ready.Entries[0].Clone();

        ready.Entries[0].Index = 99;
        ready.Entries[0].Term = 99;
        storage.Append([persistedEntry]);

        node.Advance(ready);

        Ready committed = node.Ready();
        Entry applied = Assert.Single(
            committed.CommittedEntries);
        Assert.Equal(1UL, applied.Index);
        Assert.Equal(1UL, applied.Term);
        Assert.Equal(1UL, node.Core.Term);
    }

    [Fact]
    public void ImmediateMessagesPrecedeAfterAppendResponses()
    {
        MemoryStorage storage = CreateStorage(voters: [1, 2]);
        var node = CreateNode(storage);
        node.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 1,
            Type = MessageType.MsgHeartbeat,
        });
        Ready heartbeat = node.Ready();
        PersistAndAdvance(node, storage, heartbeat);

        node.Propose("forward"u8);
        var append = new Message
        {
            From = 2,
            To = 1,
            Term = 1,
            Type = MessageType.MsgApp,
            Index = 0,
            LogTerm = 0,
            Commit = 1,
        };
        append.Entries.Add(EntryAt(1, 1, "committed"));
        node.Step(append);

        Ready ready = node.Ready();

        Assert.Collection(
            ready.Messages,
            message =>
                Assert.Equal(
                    MessageType.MsgProp,
                    message.Type),
            message =>
                Assert.Equal(
                    MessageType.MsgAppResp,
                    message.Type));
    }

    [Fact]
    public void SnapshotRemainsInProgressUntilAdvance()
    {
        MemoryStorage storage = CreateStorage(voters: [1, 2]);
        var node = CreateNode(storage);
        node.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 1,
            Type = MessageType.MsgSnap,
            Snapshot = new Snapshot
            {
                Data = ByteString.CopyFromUtf8("state"),
                Metadata = new SnapshotMetadata
                {
                    Index = 5,
                    Term = 1,
                    ConfState = new ConfState
                    {
                        Voters = { 1, 2 },
                    },
                },
            },
        });

        Ready ready = node.Ready();

        Assert.Equal(5UL, ready.Snapshot?.Metadata.Index);
        Assert.Contains(
            ready.Messages,
            message =>
                message.Type == MessageType.MsgAppResp
                && message.To == 2
                && message.Index == 5);
        Assert.True(node.Core.Log.HasUnstableSnapshot);

        PersistAndAdvance(node, storage, ready);

        Assert.False(node.Core.Log.HasUnstableSnapshot);
        Assert.Equal(5UL, node.Core.Log.Applied);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void SnapshotAndEarlierApplicationKeepTheirOwnConfigurations(
        bool asyncStorageWrites,
        bool earlierEntryLeavesJoint)
    {
        var storage = new MemoryStorage();
        var initial = new ConfState { Voters = { 1, 2 } };
        if (earlierEntryLeavesJoint)
        {
            initial.VotersOutgoing.Add([1UL, 2UL, 3UL]);
        }
        else
        {
            initial.Voters.Add(3);
        }

        storage.ApplySnapshot(new Snapshot
        {
            Metadata = new SnapshotMetadata { ConfState = initial },
        });
        var change = new Entry { Index = 1, Term = 1 };
        if (earlierEntryLeavesJoint)
        {
            change.Type = EntryType.EntryConfChangeV2;
        }
        else
        {
            change.Type = EntryType.EntryConfChange;
            change.Data = new ProtocolConfChange
            {
                Type = ConfChangeType.ConfChangeAddNode,
                NodeId = 4,
            }.ToByteString();
        }

        storage.Append([change]);
        storage.SetHardState(new HardState { Term = 1, Commit = 1 });
        var node = CreateNode(storage, asyncStorageWrites: asyncStorageWrites);
        Ready earlier = node.Ready();
        Message? apply = asyncStorageWrites
            ? Assert.Single(earlier.Messages, message =>
                message.Type == MessageType.MsgStorageApply)
            : null;
        Entry entry = Assert.Single(apply is null
            ? earlier.CommittedEntries
            : apply.Entries);

        // The newer snapshot includes the earlier change and a later change.
        // It is already non-joint in the joint-exit case.
        ulong[] snapshotVoters = earlierEntryLeavesJoint ? [1, 2, 4] : [1, 2, 3];
        ReceiveSnapshot(node, 2, snapshotVoters);
        ConfState applied = earlierEntryLeavesJoint
            ? node.ApplyConfChange(ConfChangeV2.Parser.ParseFrom(entry.Data))
            : node.ApplyConfChange(ProtocolConfChange.Parser.ParseFrom(entry.Data));
        ulong[] earlierVoters = earlierEntryLeavesJoint ? [1, 2] : [1, 2, 3, 4];
        Assert.Equal(earlierVoters, applied.Voters);
        Assert.Empty(applied.VotersOutgoing);
        Assert.Equal(snapshotVoters, node.GetStatus().Configuration.Voters);
        if (apply is null)
        {
            node.Advance(earlier);
        }
        else
        {
            node.Step(Assert.Single(apply.Responses));
        }

        Ready snapshot = node.Ready();
        CompleteSnapshot(node, storage, snapshot);

        Assert.Equal(2UL, node.GetBasicStatus().Applied);
        Assert.Equal(snapshotVoters, node.GetStatus().Configuration.Voters);
        Assert.False(node.Core.Log.HasUnstableSnapshot);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void CompletingOlderSnapshotDoesNotReplaceNewerMembership(
        bool asyncStorageWrites)
    {
        MemoryStorage storage = CreateStorage(voters: [1, 2]);
        var node = CreateNode(storage, asyncStorageWrites: asyncStorageWrites);
        ReceiveSnapshot(node, 4, [1, 2]);
        Ready older = node.Ready();
        ReceiveSnapshot(node, 5, [1, 2, 3]);

        CompleteSnapshot(node, storage, older);

        Assert.Equal(4UL, node.GetBasicStatus().Applied);
        Assert.Equal([1UL, 2UL, 3UL], node.GetStatus().Configuration.Voters);
        Assert.Equal(5UL, node.Ready().Snapshot?.Metadata.Index);
    }

    [Fact]
    public void AppliedCommittedEntriesReleaseUncommittedPayload()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(
            storage,
            maxUncommittedEntriesSize: 5);
        BecomeSingletonLeader(node, storage);

        node.Propose("12345"u8);
        Ready append = node.Ready();
        PersistAndAdvance(node, storage, append);
        Assert.Equal(5UL, node.Core.UncommittedSize);

        Ready apply = node.Ready();
        Assert.Single(apply.CommittedEntries);
        Persist(storage, apply);
        node.Advance(apply);

        Assert.Equal(0UL, node.Core.UncommittedSize);
    }

    private static void ReceiveSnapshot(
        DotnetRaft.RawNode node,
        ulong index,
        ulong[] voters)
    {
        var configuration = new ConfState();
        configuration.Voters.Add(voters);
        node.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 1,
            Type = MessageType.MsgSnap,
            Snapshot = new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = index,
                    Term = 1,
                    ConfState = configuration,
                },
            },
        });
    }

    private static void CompleteSnapshot(
        DotnetRaft.RawNode node,
        MemoryStorage storage,
        Ready ready)
    {
        if (!node.AsyncStorageWrites)
        {
            PersistAndAdvance(node, storage, ready);
            return;
        }

        Message append = AsyncStorageTestSupport.StorageMessage(
            ready, MessageType.MsgStorageAppend);
        Message[] responses = AsyncStorageTestSupport.PersistAppend(storage, append);
        node.Step(Assert.Single(responses, response => response.To == 1));
    }
}
