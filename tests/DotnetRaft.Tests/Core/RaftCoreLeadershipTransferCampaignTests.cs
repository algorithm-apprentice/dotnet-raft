using DotnetRaft.Core;
using DotnetRaft.Protocol;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;
using static DotnetRaft.Tests.Core.RaftCoreLeadershipTransferTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreLeadershipTransferCampaignTests
{
    [Fact]
    public void TimeoutNowBypassesPreVoteWithTransferCampaign()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2, 3],
            term: 5,
            preVote: true).Core;

        core.Step(TimeoutNow(core, from: 1));

        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Equal(6UL, core.Term);
        Assert.Equal(2UL, core.Vote);
        Message[] requests = core.TakeMessages();
        Assert.Equal(2, requests.Length);
        Assert.All(requests, request =>
        {
            Assert.Equal(MessageType.MsgVote, request.Type);
            Assert.Equal(6UL, request.Term);
            Assert.Equal(
                ByteString.CopyFromUtf8("CampaignTransfer"),
                request.Context);
        });
        Message selfVote = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgVoteResp, selfVote.Type);
    }

    [Theory]
    [InlineData(ElectionTestRole.PreCandidate)]
    [InlineData(ElectionTestRole.Candidate)]
    [InlineData(ElectionTestRole.Leader)]
    public void SameTermTimeoutNowIsIgnoredOutsideFollower(
        ElectionTestRole role)
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2, 3],
            term: 5).Core;
        EnterRole(core, role);
        core.TakeMessages();
        core.TakeMessagesAfterAppend();
        SoftState softState = core.SoftState;
        HardState hardState = core.HardState;

        core.Step(TimeoutNow(core, from: 2));

        Assert.Equal(softState, core.SoftState);
        Assert.Equal(hardState, core.HardState);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Theory]
    [InlineData(ElectionTestRole.Follower)]
    [InlineData(ElectionTestRole.PreCandidate)]
    [InlineData(ElectionTestRole.Candidate)]
    [InlineData(ElectionTestRole.Leader)]
    public void HigherTermTimeoutNowResetsThenCampaigns(
        ElectionTestRole role)
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2, 3],
            term: 5,
            preVote: true).Core;
        EnterRole(core, role);
        core.TakeMessages();
        core.TakeMessagesAfterAppend();
        ulong messageTerm = core.Term + 1;

        core.Step(TimeoutNow(
            core,
            from: 2,
            term: messageTerm));

        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Equal(messageTerm + 1, core.Term);
        Assert.Equal(1UL, core.Vote);
        Assert.All(
            core.TakeMessages(),
            request =>
            {
                Assert.Equal(
                    MessageType.MsgVote,
                    request.Type);
                Assert.Equal(
                    ByteString.CopyFromUtf8(
                        "CampaignTransfer"),
                    request.Context);
            });
        Assert.Single(core.TakeMessagesAfterAppend());
    }

    [Theory]
    [InlineData(ElectionTestRole.Follower)]
    [InlineData(ElectionTestRole.PreCandidate)]
    [InlineData(ElectionTestRole.Candidate)]
    [InlineData(ElectionTestRole.Leader)]
    public void HigherTermTransferResetsThenDropsWithoutLeader(
        ElectionTestRole role)
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2, 3],
            term: 5).Core;
        EnterRole(core, role);
        core.TakeMessages();
        core.TakeMessagesAfterAppend();
        ulong messageTerm = core.Term + 1;

        core.Step(Transfer(
            transferee: 2,
            recipient: 1,
            term: messageTerm));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(messageTerm, core.Term);
        Assert.Equal(0UL, core.LeaderId);
        Assert.Equal(0UL, core.LeaderTransferee);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void LearnerAndRemovedNodeIgnoreTimeoutNow(
        bool learner)
    {
        RaftCore core = learner
            ? Create(
                id: 1,
                voters: [2, 3],
                learners: [1],
                term: 5,
                preVote: true).Core
            : Create(
                id: 1,
                voters: [2, 3],
                term: 5,
                preVote: true).Core;

        core.Step(TimeoutNow(core, from: 2));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(5UL, core.Term);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void RestoringSnapshotBlocksTimeoutNowCampaign()
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2],
            term: 1,
            preVote: true).Core;
        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 2,
            Type = MessageType.MsgSnap,
            Snapshot = new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = 5,
                    Term = 2,
                    ConfState = new ConfState
                    {
                        Voters = { 1, 2 },
                    },
                },
            },
        });
        Assert.True(core.Log.HasUnstableSnapshot);
        core.TakeMessagesAfterAppend();

        core.Step(TimeoutNow(core, from: 2));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(2UL, core.Term);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void UnappliedConfigurationBlocksThenAllowsTransferCampaign()
    {
        var change = new ConfChangeV2();
        change.Changes.Add(new ConfChangeSingle
        {
            Type = ConfChangeType.ConfChangeAddLearnerNode,
            NodeId = 3,
        });
        RaftCore core = Create(
            id: 1,
            voters: [1, 2],
            entries:
            [
                new Entry
                {
                    Index = 1,
                    Term = 1,
                    Type = EntryType.EntryConfChangeV2,
                    Data = change.ToByteString(),
                },
            ],
            term: 1,
            commit: 1,
            preVote: true).Core;

        core.Step(TimeoutNow(core, from: 2));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());

        core.ApplyConfigurationChange(change);
        core.AppliedTo(1, 0);
        core.Step(TimeoutNow(core, from: 2));

        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Equal(2UL, core.Term);
        Assert.NotEmpty(core.TakeMessages());
        Assert.Single(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void TimeoutNowDoesNotRequireSenderToBeKnownLeader()
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2, 3],
            term: 5).Core;
        core.BecomeFollower(5, leaderId: 2);

        core.Step(TimeoutNow(core, from: 3));

        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Equal(6UL, core.Term);
    }
}
