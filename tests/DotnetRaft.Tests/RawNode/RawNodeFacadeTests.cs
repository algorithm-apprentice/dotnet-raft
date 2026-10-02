using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Tracker;

using Google.Protobuf;

using static DotnetRaft.Tests.RawNode.RawNodeTestSupport;

namespace DotnetRaft.Tests.RawNode;

public sealed class RawNodeFacadeTests
{
    [Fact]
    public void ConstructorRejectsAsyncStorageWrites()
    {
        MemoryStorage storage = CreateStorage();

        Assert.Throws<NotSupportedException>(
            () => CreateNode(
                storage,
                asyncStorageWrites: true));
    }

    [Fact]
    public void ConstructorBaselinesPersistedState()
    {
        MemoryStorage storage = CreateStorage();
        storage.Append([EntryAt(1, 1)]);
        storage.SetHardState(new HardState
        {
            Term = 1,
            Vote = 1,
            Commit = 1,
        });

        var node = CreateNode(storage, applied: 1);

        Assert.False(node.HasReady());
    }

    [Theory]
    [InlineData(1UL, 1UL, 0UL, 1UL, 1UL, 0UL, 0, false)]
    [InlineData(1UL, 1UL, 1UL, 1UL, 1UL, 0UL, 0, false)]
    [InlineData(1UL, 1UL, 2UL, 1UL, 1UL, 1UL, 0, false)]
    [InlineData(2UL, 1UL, 1UL, 1UL, 1UL, 1UL, 0, true)]
    [InlineData(1UL, 2UL, 1UL, 1UL, 1UL, 1UL, 0, true)]
    [InlineData(1UL, 1UL, 1UL, 1UL, 1UL, 1UL, 1, true)]
    public void MustSyncMatchesPinnedDurabilityRule(
        ulong term,
        ulong vote,
        ulong commit,
        ulong previousTerm,
        ulong previousVote,
        ulong previousCommit,
        int entryCount,
        bool expected)
    {
        bool actual = DotnetRaft.RawNode.MustSync(
            new HardState
            {
                Term = term,
                Vote = vote,
                Commit = commit,
            },
            new HardState
            {
                Term = previousTerm,
                Vote = previousVote,
                Commit = previousCommit,
            },
            entryCount);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void MustSyncRejectsNegativeEntryCount()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => DotnetRaft.RawNode.MustSync(
                new HardState(),
                new HardState(),
                -1));
    }

    [Fact]
    public void TickUsesElectionClock()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(
            storage,
            electionTick: 2,
            heartbeatTick: 1);

        for (int tick = 0; tick < 4 && !node.HasReady(); tick++)
        {
            node.Tick();
        }

        Assert.True(node.HasReady());
        Ready election = node.Ready();
        Assert.Equal(
            RaftRole.Candidate,
            election.SoftState?.Role);
        Assert.NotNull(election.HardState);
        PersistAndAdvance(node, storage, election);

        Ready leadership = node.Ready();
        Assert.Equal(
            RaftRole.Leader,
            leadership.SoftState?.Role);
    }

    [Fact]
    public void ProposalOwnsCallerBytes()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(storage);
        BecomeSingletonLeader(node, storage);
        byte[] data = "original"u8.ToArray();

        node.Propose(data);
        data[0] = (byte)'X';

        Ready ready = node.Ready();
        Entry entry = Assert.Single(ready.Entries);
        Assert.Equal(
            ByteString.CopyFromUtf8("original"),
            entry.Data);
    }

    [Fact]
    public void ProposalDropIsPublicAndExplicit()
    {
        var node = CreateNode(CreateStorage());

        Assert.Throws<ProposalDroppedException>(
            () => node.Propose("value"u8));
    }

    [Theory]
    [InlineData(MessageType.MsgHup)]
    [InlineData(MessageType.MsgBeat)]
    [InlineData(MessageType.MsgUnreachable)]
    [InlineData(MessageType.MsgSnapStatus)]
    [InlineData(MessageType.MsgCheckQuorum)]
    [InlineData(MessageType.MsgStorageAppend)]
    [InlineData(MessageType.MsgStorageAppendResp)]
    [InlineData(MessageType.MsgStorageApply)]
    [InlineData(MessageType.MsgStorageApplyResp)]
    public void StepRejectsClassifierLocalMessages(
        MessageType type)
    {
        var node = CreateNode(CreateStorage(voters: [1, 2]));

        Assert.Throws<InvalidOperationException>(
            () => node.Step(new Message
            {
                From = 2,
                To = 1,
                Type = type,
            }));
    }

    [Theory]
    [InlineData(ulong.MaxValue)]
    [InlineData(ulong.MaxValue - 1)]
    public void StepRejectsReservedStorageSenders(
        ulong sender)
    {
        var node = CreateNode(CreateStorage(voters: [1, 2]));

        Assert.Throws<InvalidOperationException>(
            () => node.Step(new Message
            {
                From = sender,
                To = 1,
                Type = MessageType.MsgHeartbeat,
            }));
    }

    [Theory]
    [InlineData(MessageType.MsgAppResp, 0UL)]
    [InlineData(MessageType.MsgAppResp, 99UL)]
    [InlineData(MessageType.MsgVoteResp, 0UL)]
    [InlineData(MessageType.MsgVoteResp, 99UL)]
    [InlineData(MessageType.MsgHeartbeatResp, 0UL)]
    [InlineData(MessageType.MsgHeartbeatResp, 99UL)]
    [InlineData(MessageType.MsgReadIndexResp, 0UL)]
    [InlineData(MessageType.MsgReadIndexResp, 99UL)]
    [InlineData(MessageType.MsgPreVoteResp, 0UL)]
    [InlineData(MessageType.MsgPreVoteResp, 99UL)]
    public void StepRejectsUnknownResponsesBeforeTermHandling(
        MessageType type,
        ulong sender)
    {
        MemoryStorage storage = CreateStorage(voters: [1, 2]);
        storage.SetHardState(new HardState
        {
            Term = 1,
        });
        var node = CreateNode(storage);

        Assert.Throws<InvalidOperationException>(
            () => node.Step(new Message
            {
                From = sender,
                To = 1,
                Term = 5,
                Type = type,
            }));
        Assert.False(node.HasReady());
        Assert.Equal(1UL, node.Core.Term);
    }

    [Fact]
    public void StepPreservesAcceptedInput()
    {
        MemoryStorage storage = CreateStorage(voters: [1, 2]);
        var node = CreateNode(storage);
        var message = new Message
        {
            From = 2,
            To = 1,
            Term = 1,
            Type = MessageType.MsgHeartbeat,
            Commit = 0,
            Context = ByteString.CopyFromUtf8("context"),
        };
        Message original = message.Clone();

        node.Step(message);

        Assert.Equal(original, message);
    }

    [Fact]
    public void NetworkForgetLeaderRemainsAccepted()
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
        Assert.Equal(2UL, node.Core.LeaderId);

        node.Step(new Message
        {
            From = 2,
            To = 1,
            Type = MessageType.MsgForgetLeader,
        });

        Ready forgotten = node.Ready();
        Assert.Equal(0UL, forgotten.SoftState?.LeaderId);
    }

    [Fact]
    public void ReadIndexOwnsContextAndDeliversOneResult()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(storage);
        BecomeSingletonLeader(node, storage);
        byte[] context = "read"u8.ToArray();

        node.ReadIndex(context);
        context[0] = (byte)'X';

        Ready ready = node.Ready();
        ReadState result = Assert.Single(ready.ReadStates);
        Assert.Equal(1UL, result.Index);
        Assert.Equal(
            ByteString.CopyFromUtf8("read"),
            result.RequestContext);
        node.Advance(ready);
        Assert.False(node.HasReady());
    }

    [Fact]
    public void ReportingAndTransferWrappersReachCore()
    {
        MemoryStorage storage = CreateStorage(voters: [1, 2]);
        var node = CreateNode(storage);
        BecomeTwoVoterLeader(node, storage);
        Progress peer = node.Core.Tracker.Progress[2];
        Assert.Equal(ProgressState.Replicate, peer.State);

        node.ReportUnreachable(2);
        Assert.Equal(ProgressState.Probe, peer.State);

        peer.BecomeSnapshot(node.Core.Log.LastIndex);
        node.ReportSnapshot(2, SnapshotStatus.Failure);
        Assert.Equal(ProgressState.Probe, peer.State);

        peer.MaybeUpdate(node.Core.Log.LastIndex);
        node.TransferLeader(2);
        Ready transfer = node.Ready();
        Assert.Contains(
            transfer.Messages,
            message =>
                message.Type == MessageType.MsgTimeoutNow
                && message.To == 2);
    }

    [Fact]
    public void SnapshotReportRejectsUnknownStatus()
    {
        var node = CreateNode(CreateStorage());

        Assert.Throws<ArgumentOutOfRangeException>(
            () => node.ReportSnapshot(
                2,
                (SnapshotStatus)99));
    }
}
