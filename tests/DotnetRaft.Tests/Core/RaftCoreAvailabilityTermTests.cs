using DotnetRaft.Core;
using DotnetRaft.Protocol;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreAvailabilityTermTests
{
    [Theory]
    [InlineData(MessageType.MsgVote)]
    [InlineData(MessageType.MsgPreVote)]
    public void RecentFollowerIgnoresHigherTermElectionRequest(
        MessageType type)
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2, 3],
            term: 5,
            checkQuorum: true).Core;
        core.BecomeFollower(5, leaderId: 2);
        SoftState softState = core.SoftState;
        HardState hardState = core.HardState;

        core.Step(ElectionRequest(
            core,
            type,
            from: 3,
            term: 6));

        Assert.Equal(softState, core.SoftState);
        Assert.Equal(hardState, core.HardState);
        Assert.Equal(0, core.GetClockStateForTesting().ElectionElapsed);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Theory]
    [InlineData(MessageType.MsgVote)]
    [InlineData(MessageType.MsgPreVote)]
    public void RecentLeaderIgnoresHigherTermElectionRequest(
        MessageType type)
    {
        RaftCore core = NewLeader();
        SoftState softState = core.SoftState;
        HardState hardState = core.HardState;

        core.Step(ElectionRequest(
            core,
            type,
            from: 2,
            term: core.Term + 1));

        Assert.Equal(softState, core.SoftState);
        Assert.Equal(hardState, core.HardState);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void ExpiredLeaseAllowsHigherTermRealVote()
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2, 3],
            term: 5,
            checkQuorum: true).Core;
        core.BecomeFollower(5, leaderId: 2);
        core.SetClockElapsedForTesting(core.GetClockStateForTesting().ElectionTick);

        core.Step(ElectionRequest(
            core,
            MessageType.MsgVote,
            from: 3,
            term: 6));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(6UL, core.Term);
        Assert.Equal(3UL, core.Vote);
        Assert.Equal(0UL, core.LeaderId);
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgVoteResp, response.Type);
        Assert.False(response.Reject);
        Assert.Equal(6UL, response.Term);
    }

    [Fact]
    public void TransferCampaignContextBypassesActiveLease()
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2, 3],
            term: 5,
            checkQuorum: true).Core;
        core.BecomeFollower(5, leaderId: 2);
        Message request = ElectionRequest(
            core,
            MessageType.MsgVote,
            from: 3,
            term: 6);
        request.Context =
            ByteString.CopyFromUtf8("CampaignTransfer");

        core.Step(request);

        Assert.Equal(6UL, core.Term);
        Assert.Equal(3UL, core.Vote);
        Assert.Single(core.TakeMessagesAfterAppend());
    }

    [Theory]
    [InlineData(true, false, MessageType.MsgApp)]
    [InlineData(true, false, MessageType.MsgHeartbeat)]
    [InlineData(false, true, MessageType.MsgApp)]
    [InlineData(false, true, MessageType.MsgHeartbeat)]
    public void AvailabilityFeatureRespondsToLowerTermLeaderMessage(
        bool checkQuorum,
        bool preVote,
        MessageType type)
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2, 3],
            term: 5,
            checkQuorum: checkQuorum,
            preVote: preVote).Core;

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 4,
            Type = type,
        });

        Assert.Equal(5UL, core.Term);
        Assert.Equal(RaftRole.Follower, core.Role);
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgAppResp, response.Type);
        Assert.Equal(2UL, response.To);
        Assert.Equal(5UL, response.Term);
    }

    [Theory]
    [InlineData(MessageType.MsgApp)]
    [InlineData(MessageType.MsgHeartbeat)]
    public void LowerTermLeaderMessageStaysSilentWithoutFeature(
        MessageType type)
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2, 3],
            term: 5).Core;

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 4,
            Type = type,
        });

        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void LowerTermPreVoteIsRejectedWithCurrentTerm()
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2, 3],
            term: 5).Core;

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 4,
            Type = MessageType.MsgPreVote,
        });

        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgPreVoteResp, response.Type);
        Assert.True(response.Reject);
        Assert.Equal(5UL, response.Term);
        Assert.Equal(5UL, core.Term);
    }

    [Theory]
    [InlineData(MessageType.MsgVote)]
    [InlineData(MessageType.MsgVoteResp)]
    [InlineData(MessageType.MsgPreVote)]
    [InlineData(MessageType.MsgPreVoteResp)]
    public void ElectionMessagesRequireNonzeroTerm(
        MessageType type)
    {
        RaftCore core = Create(voters: [1, 2, 3]).Core;

        Assert.Throws<RaftInvariantException>(
            () => core.Step(new Message
            {
                From = 2,
                To = 1,
                Type = type,
            }));
    }

    [Fact]
    public void TransferCampaignEmitsPinnedContext()
    {
        RaftCore core = Create(voters: [1, 2, 3]).Core;

        core.Campaign(CampaignType.Transfer);

        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Equal(1UL, core.Term);
        Message[] requests = core.TakeMessages();
        Assert.Equal(2, requests.Length);
        Assert.All(
            requests,
            request =>
            {
                Assert.Equal(MessageType.MsgVote, request.Type);
                Assert.Equal(
                    ByteString.CopyFromUtf8("CampaignTransfer"),
                    request.Context);
            });
        Message selfVote = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgVoteResp, selfVote.Type);
        Assert.True(selfVote.Context.IsEmpty);
    }

    private static RaftCore NewLeader()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            checkQuorum: true).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        Message selfAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(selfAck);
        core.TakeMessages();
        return core;
    }

    private static Message ElectionRequest(
        RaftCore core,
        MessageType type,
        ulong from,
        ulong term)
    {
        EntryId last = core.Log.LastEntryId;
        return new Message
        {
            From = from,
            To = core.Id,
            Term = term,
            Type = type,
            Index = last.Index,
            LogTerm = last.Term,
        };
    }
}
