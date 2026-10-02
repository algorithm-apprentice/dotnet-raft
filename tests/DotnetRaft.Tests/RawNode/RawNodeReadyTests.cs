using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using static DotnetRaft.Tests.RawNode.RawNodeTestSupport;

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
}
