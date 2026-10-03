using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Tracker;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;
using static DotnetRaft.Tests.Core.RaftCoreLeadershipTransferTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreLeadershipTransferTests
{
    [Fact]
    public void UpToDateVoterReceivesTimeoutNowImmediately()
    {
        RaftCore core = CreateLeader().Core;
        MakeUpToDate(core, 2);
        Message request = Transfer(2);
        Message original = request.Clone();

        core.Step(request);

        Assert.Equal(original, request);
        Assert.Equal(2UL, core.LeaderTransferee);
        Assert.Equal(0, core.GetClockStateForTesting().ElectionElapsed);
        Message timeout = Assert.Single(core.TakeMessages());
        Assert.Equal(MessageType.MsgTimeoutNow, timeout.Type);
        Assert.Equal(1UL, timeout.From);
        Assert.Equal(2UL, timeout.To);
        Assert.Equal(core.Term, timeout.Term);
    }

    [Fact]
    public void SlowVoterReceivesAppendBeforeTimeoutNow()
    {
        RaftCore core = CreateLeader().Core;

        core.Step(Transfer(2));

        Assert.Equal(2UL, core.LeaderTransferee);
        Message append = Assert.Single(core.TakeMessages());
        Assert.Equal(MessageType.MsgApp, append.Type);
        Assert.Equal(2UL, append.To);
        Assert.DoesNotContain(
            core.TakeMessages(),
            message =>
                message.Type ==
                MessageType.MsgTimeoutNow);
    }

    [Fact]
    public void OptimisticallySentVoterReceivesEmptyTransferProbe()
    {
        RaftCore core = CreateLeader(voters: [1, 2]).Core;
        Progress progress = core.Tracker.Progress[2];
        progress.BecomeReplicate();
        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgHeartbeatResp,
        });
        Message optimistic = Assert.Single(
            core.TakeMessages());
        Assert.NotEmpty(optimistic.Entries);
        Assert.Equal(
            core.Log.LastIndex + 1,
            progress.Next);
        Assert.True(progress.Match < core.Log.LastIndex);

        core.Step(Transfer(2));

        Message probe = Assert.Single(core.TakeMessages());
        Assert.Equal(MessageType.MsgApp, probe.Type);
        Assert.Equal(core.Log.LastIndex, probe.Index);
        Assert.Empty(probe.Entries);
    }

    [Fact]
    public void CatchUpAcknowledgementEmitsTimeoutNow()
    {
        RaftCore core = CreateLeader().Core;
        core.Step(Transfer(2));
        core.TakeMessages();

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = core.Log.LastIndex,
        });

        Message timeout = Assert.Single(
            core.TakeMessages(),
            message =>
                message.Type ==
                MessageType.MsgTimeoutNow);
        Assert.Equal(2UL, timeout.To);
        Assert.Equal(
            core.Log.LastIndex,
            core.Tracker.Progress[2].Match);
    }

    [Fact]
    public void SnapshotCatchUpWaitsForAcknowledgements()
    {
        RaftCore core = CreateCompactedLeader().Core;
        core.Step(Transfer(2));
        Message probe = Assert.Single(core.TakeMessages());
        Assert.Equal(MessageType.MsgApp, probe.Type);
        Assert.Equal(7UL, probe.Index);

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = probe.Index,
            Reject = true,
            RejectHint = 0,
        });

        Message snapshot = Assert.Single(core.TakeMessages());
        Assert.Equal(MessageType.MsgSnap, snapshot.Type);
        Assert.Equal(5UL, snapshot.Snapshot.Metadata.Index);
        Assert.Equal(
            ProgressState.Snapshot,
            core.Tracker.Progress[2].State);

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = 5,
        });

        Message[] catchUp = core.TakeMessages();
        Assert.Contains(
            catchUp,
            message =>
                message.Type == MessageType.MsgApp
                && message.To == 2);
        Assert.DoesNotContain(
            catchUp,
            message =>
                message.Type ==
                MessageType.MsgTimeoutNow);

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = core.Log.LastIndex,
        });

        Assert.Contains(
            core.TakeMessages(),
            message =>
                message.Type ==
                MessageType.MsgTimeoutNow
                && message.To == 2);
    }

    [Fact]
    public void FollowerForwardsTargetToKnownLeader()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2, 3],
            term: 5,
            disableProposalForwarding: true).Core;
        core.BecomeFollower(5, leaderId: 1);
        Message request = Transfer(
            transferee: 3,
            recipient: 2);
        Message original = request.Clone();

        core.Step(request);

        Assert.Equal(original, request);
        Message forwarded = Assert.Single(
            core.TakeMessages());
        Assert.Equal(MessageType.MsgTransferLeader, forwarded.Type);
        Assert.Equal(3UL, forwarded.From);
        Assert.Equal(1UL, forwarded.To);
        Assert.Equal(5UL, forwarded.Term);
    }

    [Fact]
    public void FollowerWithoutLeaderDropsTransfer()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2, 3],
            term: 5).Core;

        core.Step(Transfer(
            transferee: 3,
            recipient: 2));

        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Theory]
    [InlineData(ElectionTestRole.PreCandidate)]
    [InlineData(ElectionTestRole.Candidate)]
    public void CampaigningNodeIgnoresLocalTransfer(
        ElectionTestRole role)
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 5).Core;
        EnterRole(core, role);
        SoftState before = core.SoftState;
        HardState hardState = core.HardState;

        core.Step(Transfer(2));

        Assert.Equal(before, core.SoftState);
        Assert.Equal(hardState, core.HardState);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void TransferToSelfIsNoOp()
    {
        RaftCore core = CreateLeader().Core;
        SoftState before = core.SoftState;

        core.Step(Transfer(1));

        Assert.Equal(before, core.SoftState);
        Assert.Equal(0UL, core.LeaderTransferee);
        Assert.Empty(core.TakeMessages());
    }

    [Fact]
    public void TransferToSelfCancelsPendingTarget()
    {
        RaftCore core = CreateLeader().Core;
        core.Step(Transfer(2));
        core.TakeMessages();

        core.Step(Transfer(1));

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.Equal(0UL, core.LeaderTransferee);
        Assert.Empty(core.TakeMessages());
    }

    [Fact]
    public void UnknownAndLearnerTargetsDoNotCancelPendingTransfer()
    {
        RaftCore core = CreateLeader(
            voters: [1, 2],
            learners: [3]).Core;
        core.Step(Transfer(2));
        core.TakeMessages();
        core.SetClockElapsedForTesting(4);

        core.Step(Transfer(4));
        core.Step(Transfer(3));

        Assert.Equal(2UL, core.LeaderTransferee);
        Assert.Equal(4, core.GetClockStateForTesting().ElectionElapsed);
        Assert.Empty(core.TakeMessages());
    }

    [Fact]
    public void DuplicateTargetDoesNotExtendTimeout()
    {
        RaftCore core = CreateLeader(
            electionTick: 5).Core;
        core.Step(Transfer(2));
        core.TakeMessages();
        core.SetClockElapsedForTesting(4);

        core.Step(Transfer(2));

        Assert.Equal(4, core.GetClockStateForTesting().ElectionElapsed);
        core.TickLeader();
        Assert.Equal(0UL, core.LeaderTransferee);
    }

    [Fact]
    public void DifferentTargetReplacesTransferAndResetsTimeout()
    {
        RaftCore core = CreateLeader(
            electionTick: 5).Core;
        core.Step(Transfer(2));
        core.TakeMessages();
        core.SetClockElapsedForTesting(4);

        core.Step(Transfer(3));

        Assert.Equal(3UL, core.LeaderTransferee);
        Assert.Equal(0, core.GetClockStateForTesting().ElectionElapsed);
    }

    [Fact]
    public void TransferExpiresAtElectionTickAndProposalsResume()
    {
        RaftCore core = CreateLeader(
            electionTick: 3).Core;
        core.Step(Transfer(2));
        core.TakeMessages();
        ulong before = core.Log.LastIndex;

        core.TickLeader();
        core.TakeMessages();
        core.TickLeader();
        core.TakeMessages();

        Assert.Equal(2UL, core.LeaderTransferee);

        core.TickLeader();
        core.TakeMessages();
        Assert.Equal(0UL, core.LeaderTransferee);

        core.Step(Proposal(
            core.Id,
            NormalEntry()));

        Assert.Equal(before + 1, core.Log.LastIndex);
    }

    [Fact]
    public void ActiveQuorumExpiresTransferBeforeDueHeartbeat()
    {
        RaftCore core = CreateLeader(
            electionTick: 2,
            heartbeatTick: 1,
            checkQuorum: true).Core;
        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgHeartbeatResp,
        });
        core.TakeMessages();
        core.Step(Transfer(3));
        core.TakeMessages();
        core.TickLeader();
        core.TakeMessages();

        core.TickLeader();

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.Equal(0UL, core.LeaderTransferee);
        Message[] heartbeats = core.TakeMessages();
        Assert.Equal(2, heartbeats.Length);
        Assert.All(
            heartbeats,
            message =>
                Assert.Equal(
                    MessageType.MsgHeartbeat,
                    message.Type));
    }

    [Fact]
    public void QuorumLossStepDownClearsTransferWithoutHeartbeat()
    {
        RaftCore core = CreateLeader(
            electionTick: 2,
            heartbeatTick: 1,
            checkQuorum: true).Core;
        core.Step(Transfer(3));
        core.TakeMessages();
        core.TickLeader();
        core.TakeMessages();

        core.TickLeader();

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(0UL, core.LeaderTransferee);
        Assert.Empty(core.TakeMessages());
    }

    [Fact]
    public void HigherTermResetClearsTransfer()
    {
        RaftCore core = CreateLeader().Core;
        core.Step(Transfer(2));
        core.TakeMessages();

        core.Step(new Message
        {
            From = 3,
            To = 1,
            Term = core.Term + 1,
            Type = MessageType.MsgAppResp,
        });

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(0UL, core.LeaderTransferee);
    }

    [Fact]
    public void ValidProposalIsDroppedWithoutMutation()
    {
        RaftCore core = CreateLeader().Core;
        core.Step(Transfer(2));
        core.TakeMessages();
        ulong lastIndex = core.Log.LastIndex;
        ulong uncommitted = core.UncommittedSize;
        ulong pending = core.PendingConfigurationIndex;

        Assert.Throws<ProposalDroppedException>(
            () => core.Step(Proposal(
                core.Id,
                NormalEntry())));

        Assert.Equal(lastIndex, core.Log.LastIndex);
        Assert.Equal(uncommitted, core.UncommittedSize);
        Assert.Equal(
            pending,
            core.PendingConfigurationIndex);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void ForwardedProposalIsDroppedByTransferringLeader()
    {
        RaftCore leader = CreateLeader().Core;
        leader.Step(Transfer(2));
        leader.TakeMessages();
        RaftCore follower = Create(
            id: 3,
            voters: [1, 2, 3],
            term: leader.Term).Core;
        follower.BecomeFollower(
            leader.Term,
            leaderId: 1);
        ulong lastIndex = leader.Log.LastIndex;

        follower.Step(Proposal(
            follower.Id,
            NormalEntry()));
        Message forwarded = Assert.Single(
            follower.TakeMessages());

        Assert.Equal(1UL, forwarded.To);
        Assert.Throws<ProposalDroppedException>(
            () => leader.Step(forwarded));
        Assert.Equal(lastIndex, leader.Log.LastIndex);
        Assert.Empty(leader.TakeMessages());
        Assert.Empty(leader.TakeMessagesAfterAppend());
    }

    [Fact]
    public void EmptyProposalKeepsInvariantPrecedence()
    {
        RaftCore core = CreateLeader().Core;
        core.Step(Transfer(2));
        core.TakeMessages();

        Assert.Throws<RaftInvariantException>(
            () => core.Step(Proposal(core.Id)));
    }

    [Theory]
    [InlineData(EntryType.EntryConfChange)]
    [InlineData(EntryType.EntryConfChangeV2)]
    public void TransferDropPrecedesConfigurationDecoding(
        EntryType type)
    {
        RaftCore core = CreateLeader().Core;
        core.Step(Transfer(2));
        core.TakeMessages();
        ulong lastIndex = core.Log.LastIndex;

        Assert.Throws<ProposalDroppedException>(
            () => core.Step(Proposal(
                core.Id,
                new Entry
                {
                    Type = type,
                    Data = ByteString.CopyFrom(0xff),
                })));

        Assert.Equal(lastIndex, core.Log.LastIndex);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }
}
