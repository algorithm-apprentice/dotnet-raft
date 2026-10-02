using DotnetRaft.Core;
using DotnetRaft.Protocol;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreVoteTests
{
    [Theory]
    [MemberData(nameof(RolesAndLeaderMessages))]
    public void HigherTermLeaderMessageStepsEveryRoleDownAndHandlesPayload(
        ElectionTestRole role,
        MessageType type)
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 4,
            commit: 1,
            applied: 1).Core;
        EnterRole(core, role);
        if (role == ElectionTestRole.Leader)
        {
            core.TakeMessagesAfterAppend();
        }

        core.ElectionElapsed = 4;
        ulong lastIndex = core.Log.LastIndex;
        ulong committed = core.Log.Committed;
        Message message = LeaderMessage(
            core,
            type,
            core.Term + 1);
        Message original = message.Clone();

        core.Step(message);

        Assert.Equal(original, message);
        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(original.Term, core.Term);
        Assert.Equal(2UL, core.LeaderId);
        Assert.Equal(0, core.ElectionElapsed);
        Assert.Equal(lastIndex, core.Log.LastIndex);
        Assert.Equal(committed, core.Log.Committed);
        AssertLeaderMessageResponse(core, type, lastIndex);
    }

    [Fact]
    public void HigherTermDeferredResponseStillStepsLeaderDown()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 4).Core;
        EnterRole(core, ElectionTestRole.Leader);
        core.TakeMessagesAfterAppend();
        ulong term = core.Term;
        ulong lastIndex = core.Log.LastIndex;

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = term + 1,
            Type = MessageType.MsgAppResp,
            Index = 100,
            Reject = true,
            RejectHint = 99,
            LogTerm = 9,
        });

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(term + 1, core.Term);
        Assert.Equal(0UL, core.LeaderId);
        Assert.Equal(lastIndex, core.Log.LastIndex);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Theory]
    [InlineData(MessageType.MsgApp)]
    [InlineData(MessageType.MsgHeartbeat)]
    [InlineData(MessageType.MsgSnap)]
    public void CurrentTermLeaderMessageMakesCandidateFollowerAndHandlesPayload(
        MessageType type)
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 4).Core;
        core.BecomeCandidate();
        core.ElectionElapsed = 4;
        ulong lastIndex = core.Log.LastIndex;
        ulong committed = core.Log.Committed;
        Message message = LeaderMessage(
            core,
            type,
            core.Term);

        core.Step(message);

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(5UL, core.Term);
        Assert.Equal(2UL, core.LeaderId);
        Assert.Equal(0, core.ElectionElapsed);
        Assert.Equal(lastIndex, core.Log.LastIndex);
        Assert.Equal(committed, core.Log.Committed);
        AssertLeaderMessageResponse(core, type, lastIndex);
    }

    [Theory]
    [InlineData(MessageType.MsgApp)]
    [InlineData(MessageType.MsgHeartbeat)]
    [InlineData(MessageType.MsgSnap)]
    public void CurrentTermLeaderMessageRefreshesFollowerAndHandlesPayload(
        MessageType type)
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 5).Core;
        core.BecomeFollower(5, 3);
        core.ElectionElapsed = 4;
        ulong lastIndex = core.Log.LastIndex;
        ulong committed = core.Log.Committed;

        core.Step(LeaderMessage(core, type, core.Term));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(5UL, core.Term);
        Assert.Equal(2UL, core.LeaderId);
        Assert.Equal(0, core.ElectionElapsed);
        Assert.Equal(lastIndex, core.Log.LastIndex);
        Assert.Equal(committed, core.Log.Committed);
        AssertLeaderMessageResponse(core, type, lastIndex);
    }

    [Theory]
    [InlineData(MessageType.MsgApp)]
    [InlineData(MessageType.MsgHeartbeat)]
    [InlineData(MessageType.MsgSnap)]
    public void EqualTermLeaderIgnoresCompetingLeaderPayload(
        MessageType type)
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 4).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        core.TakeMessagesAfterAppend();
        core.ElectionElapsed = 4;
        ulong lastIndex = core.Log.LastIndex;
        ulong committed = core.Log.Committed;

        core.Step(LeaderMessage(core, type, core.Term));

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.Equal(core.Id, core.LeaderId);
        Assert.Equal(4, core.ElectionElapsed);
        Assert.Equal(lastIndex, core.Log.LastIndex);
        Assert.Equal(committed, core.Log.Committed);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Theory]
    [InlineData(MessageType.MsgVote)]
    [InlineData(MessageType.MsgVoteResp)]
    [InlineData(MessageType.MsgApp)]
    [InlineData(MessageType.MsgAppResp)]
    public void LowerTermMessagesAreIgnoredWithoutSideEffects(
        MessageType type)
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 4).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        core.TakeMessagesAfterAppend();
        core.ElectionElapsed = 3;
        SoftState beforeSoftState = core.SoftState;
        HardState beforeHardState = core.HardState;
        ulong beforeLastIndex = core.Log.LastIndex;

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term - 1,
            Type = type,
            Index = 99,
            LogTerm = 99,
            Commit = 99,
            Reject = true,
        });

        Assert.Equal(beforeSoftState, core.SoftState);
        Assert.Equal(beforeHardState, core.HardState);
        Assert.Equal(beforeLastIndex, core.Log.LastIndex);
        Assert.Equal(3, core.ElectionElapsed);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Theory]
    [InlineData(ElectionTestRole.Follower)]
    [InlineData(ElectionTestRole.PreCandidate)]
    [InlineData(ElectionTestRole.Candidate)]
    [InlineData(ElectionTestRole.Leader)]
    public void HigherTermVoteFromAnyRoleIsGranted(
        ElectionTestRole role)
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 4).Core;
        EnterRole(core, role);
        if (role == ElectionTestRole.Leader)
        {
            core.TakeMessagesAfterAppend();
        }

        ulong requestTerm = core.Term + 1;
        EntryId lastEntry = core.Log.LastEntryId;

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = requestTerm,
            Type = MessageType.MsgVote,
            Index = lastEntry.Index,
            LogTerm = lastEntry.Term,
        });

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(requestTerm, core.Term);
        Assert.Equal(2UL, core.Vote);
        Assert.Equal(0UL, core.LeaderId);
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgVoteResp, response.Type);
        Assert.Equal(2UL, response.To);
        Assert.Equal(requestTerm, response.Term);
        Assert.False(response.Reject);
        Assert.Empty(core.TakeMessages());
    }

    [Fact]
    public void HigherTermVoteResponseStepsCandidateDownWithoutTallying()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 4).Core;
        core.BecomeCandidate();

        core.Step(VoteResponse(
            core,
            from: 2,
            granted: true,
            term: core.Term + 1));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(6UL, core.Term);
        Assert.Equal(0UL, core.Vote);
        Assert.Empty(core.Tracker.Votes);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void HigherTermPreVoteMessagesPreserveReservedTermSemantics()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 4).Core;
        core.BecomeCandidate();
        SoftState candidate = core.SoftState;
        HardState hardState = core.HardState;

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term + 1,
            Type = MessageType.MsgPreVote,
        });
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgPreVoteResp, response.Type);
        Assert.False(response.Reject);
        Assert.Equal(core.Term + 1, response.Term);

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term + 1,
            Type = MessageType.MsgPreVoteResp,
        });

        Assert.Equal(candidate, core.SoftState);
        Assert.Equal(hardState, core.HardState);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term + 1,
            Type = MessageType.MsgPreVoteResp,
            Reject = true,
        });

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(6UL, core.Term);
        Assert.Equal(0UL, core.LeaderId);
    }

    [Theory]
    [MemberData(nameof(VoteEligibilityCases))]
    public void VoteEligibilityUsesPersistentVoteAndKnownLeader(
        ulong existingVote,
        ulong leaderId,
        ulong candidateId,
        bool reject)
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 5,
            vote: existingVote).Core;
        core.BecomeFollower(5, leaderId);
        core.ElectionElapsed = 4;

        core.Step(new Message
        {
            From = candidateId,
            To = 1,
            Term = 5,
            Type = MessageType.MsgVote,
        });

        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(reject, response.Reject);
        Assert.Equal(candidateId, response.To);
        Assert.Equal(5UL, response.Term);
        Assert.Equal(
            reject ? existingVote : candidateId,
            core.Vote);
        Assert.Equal(reject ? 4 : 0, core.ElectionElapsed);
        Assert.Empty(core.TakeMessages());
    }

    [Theory]
    [MemberData(nameof(LogFreshnessCases))]
    public void VoteRequiresCandidateLogToBeAtLeastAsUpToDate(
        ulong candidateTerm,
        ulong candidateIndex,
        bool reject)
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
            ],
            term: 5).Core;

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 5,
            Type = MessageType.MsgVote,
            Index = candidateIndex,
            LogTerm = candidateTerm,
        });

        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(reject, response.Reject);
        Assert.Equal(reject ? 0UL : 2UL, core.Vote);
    }

    [Fact]
    public void LearnerCanVoteButCannotCampaign()
    {
        RaftCore core = Create(
            voters: [2, 3],
            learners: [1],
            term: 4).Core;

        core.Step(Hup(core.Id));
        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(4UL, core.Term);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 5,
            Type = MessageType.MsgVote,
            Index = 1,
            LogTerm = 1,
        });

        Assert.True(core.IsLearner);
        Assert.Equal(2UL, core.Vote);
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.False(response.Reject);
        Assert.Equal(2UL, response.To);
    }

    [Fact]
    public void VoteResponseWaitsForDurabilityAndInputRemainsOwnedByCaller()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 5).Core;
        var request = new Message
        {
            From = 2,
            To = 1,
            Term = 5,
            Type = MessageType.MsgVote,
        };
        Message original = request.Clone();

        core.Step(request);

        Assert.Equal(original, request);
        Assert.Empty(core.TakeMessages());
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());

        request.From = 3;
        request.Term = 9;
        request.Reject = true;

        Assert.Equal(2UL, response.To);
        Assert.Equal(5UL, response.Term);
        Assert.False(response.Reject);
        Assert.Equal(2UL, core.Vote);
    }

    [Fact]
    public void TermZeroRealVoteMessagesFailAtomically()
    {
        RaftCore follower = Create(
            voters: [1, 2, 3],
            term: 5).Core;
        SoftState followerSoftState = follower.SoftState;
        HardState followerHardState = follower.HardState;

        Assert.Throws<RaftInvariantException>(
            () => follower.Step(new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgVote,
            }));

        Assert.Equal(followerSoftState, follower.SoftState);
        Assert.Equal(followerHardState, follower.HardState);
        Assert.Empty(follower.TakeMessages());
        Assert.Empty(follower.TakeMessagesAfterAppend());

        RaftCore candidate = Create(
            voters: [1, 2, 3],
            term: 4).Core;
        candidate.BecomeCandidate();
        SoftState candidateSoftState = candidate.SoftState;
        HardState candidateHardState = candidate.HardState;

        Assert.Throws<RaftInvariantException>(
            () => candidate.Step(new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgVoteResp,
            }));

        Assert.Equal(candidateSoftState, candidate.SoftState);
        Assert.Equal(candidateHardState, candidate.HardState);
        Assert.Empty(candidate.Tracker.Votes);
        Assert.Empty(candidate.TakeMessages());
        Assert.Empty(candidate.TakeMessagesAfterAppend());
    }

    [Theory]
    [InlineData(ElectionTestRole.Follower)]
    [InlineData(ElectionTestRole.PreCandidate)]
    [InlineData(ElectionTestRole.Leader)]
    public void CurrentTermVoteResponseIsIgnoredOutsideCandidate(
        ElectionTestRole role)
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 4).Core;
        EnterRole(core, role);
        if (role == ElectionTestRole.Leader)
        {
            core.TakeMessagesAfterAppend();
        }

        SoftState before = core.SoftState;

        core.Step(VoteResponse(core, 2, granted: true));

        Assert.Equal(before, core.SoftState);
        Assert.Empty(core.Tracker.Votes);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    private static void AssertLeaderMessageResponse(
        RaftCore core,
        MessageType type,
        ulong lastIndex)
    {
        switch (type)
        {
            case MessageType.MsgApp:
                Assert.Empty(core.TakeMessages());
                Message appendResponse = Assert.Single(
                    core.TakeMessagesAfterAppend());
                Assert.Equal(
                    MessageType.MsgAppResp,
                    appendResponse.Type);
                Assert.Equal(2UL, appendResponse.To);
                Assert.Equal(lastIndex, appendResponse.Index);
                Assert.False(appendResponse.Reject);
                return;
            case MessageType.MsgHeartbeat:
                Message heartbeatResponse = Assert.Single(
                    core.TakeMessages());
                Assert.Equal(
                    MessageType.MsgHeartbeatResp,
                    heartbeatResponse.Type);
                Assert.Equal(2UL, heartbeatResponse.To);
                Assert.Empty(core.TakeMessagesAfterAppend());
                return;
            case MessageType.MsgSnap:
                Assert.Empty(core.TakeMessages());
                Message snapshotResponse = Assert.Single(
                    core.TakeMessagesAfterAppend());
                Assert.Equal(
                    MessageType.MsgAppResp,
                    snapshotResponse.Type);
                Assert.Equal(2UL, snapshotResponse.To);
                Assert.Equal(
                    core.Log.Committed,
                    snapshotResponse.Index);
                Assert.False(snapshotResponse.Reject);
                return;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(type),
                    type,
                    null);
        }
    }

    public static TheoryData<ElectionTestRole, MessageType>
        RolesAndLeaderMessages()
    {
        var data = new TheoryData<ElectionTestRole, MessageType>();
        foreach (ElectionTestRole role in Enum.GetValues<ElectionTestRole>())
        {
            data.Add(role, MessageType.MsgApp);
            data.Add(role, MessageType.MsgHeartbeat);
            data.Add(role, MessageType.MsgSnap);
        }

        return data;
    }

    public static TheoryData<ulong, ulong, ulong, bool>
        VoteEligibilityCases()
    {
        return new TheoryData<ulong, ulong, ulong, bool>
        {
            { 0, 0, 2, false },
            { 2, 0, 2, false },
            { 2, 0, 3, true },
            { 0, 3, 2, true },
            { 3, 3, 3, false },
        };
    }

    public static TheoryData<ulong, ulong, bool>
        LogFreshnessCases()
    {
        return new TheoryData<ulong, ulong, bool>
        {
            { 1, 3, true },
            { 2, 1, true },
            { 2, 2, false },
            { 2, 3, false },
            { 3, 1, false },
        };
    }
}
