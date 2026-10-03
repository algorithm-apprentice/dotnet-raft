using DotnetRaft.Core;
using DotnetRaft.Protocol;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCorePreVoteTests
{
    [Fact]
    public void HupStartsPreElectionWithoutChangingHardState()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 5,
            vote: 2,
            preVote: true).Core;
        HardState hardState = core.HardState;

        core.Step(Hup(1));

        Assert.Equal(RaftRole.PreCandidate, core.Role);
        Assert.Equal(hardState, core.HardState);
        Message[] requests = core.TakeMessages();
        Assert.Equal([2UL, 3UL], requests.Select(
            request => request.To));
        Assert.All(requests, request =>
        {
            Assert.Equal(MessageType.MsgPreVote, request.Type);
            Assert.Equal(6UL, request.Term);
        });
        Message selfVote = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgPreVoteResp, selfVote.Type);
        Assert.Equal(6UL, selfVote.Term);
    }

    [Fact]
    public void PendingPreElectionKeepsTermAndRole()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 5,
            preVote: true).Core;
        core.Step(Hup(1));
        Message selfVote = Assert.Single(
            core.TakeMessagesAfterAppend());

        core.Step(selfVote);

        Assert.Equal(RaftRole.PreCandidate, core.Role);
        Assert.Equal(5UL, core.Term);
        Assert.Equal(0UL, core.Vote);
    }

    [Fact]
    public void PreVoteQuorumStartsRealElectionExactlyOnce()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 5,
            preVote: true).Core;
        core.Step(Hup(1));
        core.TakeMessages();
        core.Step(Assert.Single(
            core.TakeMessagesAfterAppend()));

        core.Step(PreVoteResponse(
            core,
            from: 2,
            term: 6,
            reject: false));

        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Equal(6UL, core.Term);
        Assert.Equal(1UL, core.Vote);
        Message[] requests = core.TakeMessages();
        Assert.Equal(2, requests.Length);
        Assert.All(
            requests,
            request =>
            {
                Assert.Equal(MessageType.MsgVote, request.Type);
                Assert.Equal(6UL, request.Term);
            });
        Message selfVote = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgVoteResp, selfVote.Type);
        Assert.Equal(6UL, selfVote.Term);
    }

    [Fact]
    public void SingletonPreVoteUsesThreeDurabilityCycles()
    {
        RaftCore core = Create(
            voters: [1],
            preVote: true).Core;

        core.Step(Hup(1));

        Assert.Equal(RaftRole.PreCandidate, core.Role);
        Message preVote = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgPreVoteResp, preVote.Type);

        core.Step(preVote);

        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Equal(1UL, core.Term);
        Message realVote = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgVoteResp, realVote.Type);

        core.Step(realVote);

        Assert.Equal(RaftRole.Leader, core.Role);
        Message noOpAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgAppResp, noOpAck.Type);
        Assert.Equal(0UL, core.Log.Committed);

        core.Step(noOpAck);

        Assert.Equal(1UL, core.Log.Committed);
    }

    [Theory]
    [InlineData(ElectionTestRole.Follower)]
    [InlineData(ElectionTestRole.PreCandidate)]
    [InlineData(ElectionTestRole.Candidate)]
    [InlineData(ElectionTestRole.Leader)]
    public void PreVoteGrantDoesNotMutateResponderState(
        ElectionTestRole role)
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 5).Core;
        EnterRole(core, role);
        core.TakeMessages();
        core.TakeMessagesAfterAppend();
        core.SetClockElapsedForTesting(3);
        SoftState softState = core.SoftState;
        HardState hardState = core.HardState;

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term + 1,
            Type = MessageType.MsgPreVote,
            Index = ulong.MaxValue,
            LogTerm = ulong.MaxValue,
        });

        Assert.Equal(softState, core.SoftState);
        Assert.Equal(hardState, core.HardState);
        Assert.Equal(3, core.GetClockStateForTesting().ElectionElapsed);
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgPreVoteResp, response.Type);
        Assert.Equal(core.Term + 1, response.Term);
        Assert.False(response.Reject);
    }

    [Fact]
    public void StaleLogPreVoteIsRejectedWithoutStateChange()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
            ],
            term: 2,
            vote: 1).Core;
        HardState hardState = core.HardState;

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 3,
            Type = MessageType.MsgPreVote,
            Index = 99,
            LogTerm = 1,
        });

        Assert.Equal(hardState, core.HardState);
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgPreVoteResp, response.Type);
        Assert.True(response.Reject);
        Assert.Equal(2UL, response.Term);
    }

    [Fact]
    public void LearnerCanGrantPreVoteButCannotCampaign()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1],
            learners: [2],
            term: 1,
            preVote: true).Core;

        core.Step(Hup(2));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());

        core.Step(new Message
        {
            From = 1,
            To = 2,
            Term = 2,
            Type = MessageType.MsgPreVote,
        });

        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgPreVoteResp, response.Type);
        Assert.False(response.Reject);
    }

    [Fact]
    public void JointPreVoteRequiresBothMajorities()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            outgoingVoters: [1, 4, 5],
            preVote: true).Core;
        core.Step(Hup(1));
        core.TakeMessages();
        core.Step(Assert.Single(
            core.TakeMessagesAfterAppend()));

        core.Step(PreVoteResponse(
            core,
            from: 2,
            term: 1,
            reject: false));

        Assert.Equal(RaftRole.PreCandidate, core.Role);

        core.Step(PreVoteResponse(
            core,
            from: 4,
            term: 1,
            reject: false));

        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Equal(1UL, core.Term);
    }

    [Fact]
    public void RejectedHigherTermPreVoteResponseAdvancesTerm()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 5,
            preVote: true).Core;
        core.Step(Hup(1));
        core.TakeMessages();
        core.TakeMessagesAfterAppend();

        core.Step(PreVoteResponse(
            core,
            from: 2,
            term: 7,
            reject: true));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(7UL, core.Term);
        Assert.Equal(0UL, core.LeaderId);
    }

    [Fact]
    public void GrantedFuturePreVoteResponseWaitsForQuorum()
    {
        RaftCore core = Create(
            voters: [1, 2, 3, 4, 5],
            term: 5,
            preVote: true).Core;
        core.Step(Hup(1));
        core.TakeMessages();
        core.Step(Assert.Single(
            core.TakeMessagesAfterAppend()));

        core.Step(PreVoteResponse(
            core,
            from: 2,
            term: 6,
            reject: false));

        Assert.Equal(RaftRole.PreCandidate, core.Role);
        Assert.Equal(5UL, core.Term);
    }

    [Fact]
    public void DelayedPreVoteResponseIsIgnoredAfterRealCampaignStarts()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 5,
            preVote: true).Core;
        core.Step(Hup(1));
        core.TakeMessages();
        core.Step(Assert.Single(
            core.TakeMessagesAfterAppend()));
        core.Step(PreVoteResponse(
            core,
            from: 2,
            term: 6,
            reject: false));
        core.TakeMessages();
        int votesBefore = core.Tracker.Votes.Count;

        core.Step(PreVoteResponse(
            core,
            from: 3,
            term: 6,
            reject: false));

        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Equal(6UL, core.Term);
        Assert.Equal(votesBefore, core.Tracker.Votes.Count);
    }

    [Fact]
    public void PreVoteQuorumLossReturnsFollowerInSameTerm()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 5,
            vote: 2,
            preVote: true).Core;
        core.Step(Hup(1));
        core.TakeMessages();
        core.Step(Assert.Single(
            core.TakeMessagesAfterAppend()));

        core.Step(PreVoteResponse(
            core,
            from: 2,
            term: 5,
            reject: true));
        core.Step(PreVoteResponse(
            core,
            from: 3,
            term: 5,
            reject: true));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(5UL, core.Term);
        Assert.Equal(2UL, core.Vote);
        Assert.Equal(0UL, core.LeaderId);
    }

    [Fact]
    public void PreElectionTermOverflowFailsAtomically()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: ulong.MaxValue,
            preVote: true).Core;
        SoftState softState = core.SoftState;
        HardState hardState = core.HardState;

        Assert.Throws<RaftInvariantException>(
            () => core.Step(Hup(1)));

        Assert.Equal(softState, core.SoftState);
        Assert.Equal(hardState, core.HardState);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    private static Message PreVoteResponse(
        RaftCore core,
        ulong from,
        ulong term,
        bool reject)
    {
        return new Message
        {
            From = from,
            To = core.Id,
            Term = term,
            Type = MessageType.MsgPreVoteResp,
            Reject = reject,
        };
    }
}
