using System.Globalization;

using DotnetRaft.Core;
using DotnetRaft.Protocol;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreElectionTests
{
    [Fact]
    public void HupStartsCampaignWithSortedJointRequestsAndLastLogId()
    {
        ElectionCoreFixture fixture = Create(
            voters: [1, 3, 5],
            outgoingVoters: [1, 2, 4],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
                EntryAt(3, 2),
            ],
            term: 4);
        RaftCore core = fixture.Core;
        Message hup = Hup(core.Id);
        Message original = hup.Clone();

        core.Step(hup);

        Assert.Equal(original, hup);
        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Equal(5UL, core.Term);
        Assert.Equal(1UL, core.Vote);
        Assert.Empty(core.Tracker.Votes);

        Message selfVote = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgVoteResp, selfVote.Type);
        Assert.Equal(1UL, selfVote.From);
        Assert.Equal(1UL, selfVote.To);
        Assert.Equal(5UL, selfVote.Term);
        Assert.False(selfVote.Reject);

        Message[] requests = core.TakeMessages();
        Assert.Equal(
            [2UL, 3UL, 4UL, 5UL],
            requests.Select(message => message.To));
        Assert.All(requests, message =>
        {
            Assert.Equal(1UL, message.From);
            Assert.Equal(5UL, message.Term);
            Assert.Equal(MessageType.MsgVote, message.Type);
            Assert.Equal(3UL, message.Index);
            Assert.Equal(2UL, message.LogTerm);
        });
    }

    [Fact]
    public void DurableSelfVoteElectsSingletonAndNoOpNeedsNextBatch()
    {
        RaftCore core = Create(
            voters: [1],
            entries: [EntryAt(1, 1)],
            term: 2).Core;
        ulong lastIndex = core.Log.LastIndex;
        ulong committed = core.Log.Committed;

        core.Step(Hup(core.Id));

        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Empty(core.Tracker.Votes);
        Assert.Empty(core.TakeMessages());

        Message selfVote = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(RaftRole.Candidate, core.Role);

        core.Step(selfVote);

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.Equal(core.Id, core.LeaderId);
        Assert.Equal(lastIndex + 1, core.Log.LastIndex);
        Assert.Equal(lastIndex, core.PendingConfigurationIndex);
        Assert.Empty(core.TakeMessages());
        Assert.Equal(committed, core.Log.Committed);

        Message noOpAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgAppResp, noOpAck.Type);
        Assert.Equal(lastIndex + 1, noOpAck.Index);

        core.Step(noOpAck);

        Assert.Equal(lastIndex + 1, core.Log.Committed);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void DelayedSelfVoteIsIgnoredAfterCandidateStepsDown()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 4).Core;
        core.Step(Hup(core.Id));
        core.TakeMessages();
        Message selfVote = Assert.Single(
            core.TakeMessagesAfterAppend());

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgHeartbeat,
        });
        Assert.Equal(RaftRole.Follower, core.Role);

        core.Step(selfVote);

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Empty(core.Tracker.Votes);
    }

    [Theory]
    [InlineData(ElectionTestRole.Follower)]
    [InlineData(ElectionTestRole.Candidate)]
    public void ElectionTickStartsCampaignAtExactTimeout(
        ElectionTestRole initialRole)
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 1,
            electionTick: 3).Core;
        EnterRole(core, initialRole);
        ulong startingTerm = core.Term;

        core.TickElection();
        core.TickElection();

        Assert.Equal(startingTerm, core.Term);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());

        core.TickElection();

        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Equal(startingTerm + 1, core.Term);
        Assert.Equal(
            [2UL, 3UL],
            core.TakeMessages().Select(message => message.To));
        Assert.Single(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void CandidateTimeoutStartsFreshTermAndClearsPriorVotes()
    {
        RaftCore core = Create(
            voters: [1, 2, 3, 4, 5],
            electionTick: 3).Core;
        core.Step(Hup(core.Id));
        core.TakeMessages();
        StepAcceptedSelfMessages(core);
        core.Step(VoteResponse(core, 2, granted: true));
        Assert.Equal(2, core.Tracker.Votes.Count);
        Assert.Equal(RaftRole.Candidate, core.Role);

        core.TickElection();
        core.TickElection();
        core.TickElection();

        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Equal(2UL, core.Term);
        Assert.Equal(1UL, core.Vote);
        Assert.Empty(core.Tracker.Votes);
        Assert.Equal(
            [2UL, 3UL, 4UL, 5UL],
            core.TakeMessages().Select(message => message.To));
        Assert.Single(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void LeaderAndUnpromotablePeersIgnoreHup()
    {
        RaftCore leader = Create(voters: [1], term: 1).Core;
        leader.BecomeCandidate();
        leader.BecomeLeader();
        leader.TakeMessagesAfterAppend();

        RaftCore learner = Create(
            voters: [2],
            learners: [1],
            term: 1).Core;
        RaftCore absent = Create(
            voters: [2, 3],
            term: 1).Core;
        RaftCore snapshot = Create(
            voters: [1],
            term: 1).Core;
        snapshot.Log.Restore(new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = 5,
                Term = 1,
                ConfState = new ConfState
                {
                    Voters = { 1 },
                },
            },
        });

        AssertHupIgnored(leader);
        AssertHupIgnored(learner);
        AssertHupIgnored(absent);
        AssertHupIgnored(snapshot);
    }

    [Theory]
    [InlineData(EntryType.EntryConfChange)]
    [InlineData(EntryType.EntryConfChangeV2)]
    public void UnappliedConfigurationChangeOnLaterPageBlocksCampaign(
        EntryType type)
    {
        ElectionCoreFixture fixture = Create(
            voters: [1, 2, 3],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 1, type),
                EntryAt(3, 1),
            ],
            term: 2,
            commit: 3,
            maxCommittedSizePerReady: 1);

        fixture.Core.Step(Hup(fixture.Core.Id));

        Assert.Equal(RaftRole.Follower, fixture.Core.Role);
        Assert.Equal(2UL, fixture.Core.Term);
        Assert.Empty(fixture.Core.TakeMessages());
        Assert.Empty(fixture.Core.TakeMessagesAfterAppend());
        Assert.Equal(
            [1UL, 2UL],
            fixture.Storage.EntryRequests.Select(
                request => request.LowInclusive));
        Assert.All(
            fixture.Storage.EntryRequests,
            request => Assert.Equal(1UL, request.MaxSize));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AppliedOrUncommittedConfigurationChangeDoesNotBlockCampaign(
        bool applied)
    {
        ElectionCoreFixture fixture = Create(
            voters: [1, 2, 3],
            entries:
            [
                EntryAt(
                    1,
                    1,
                    EntryType.EntryConfChangeV2),
            ],
            term: 2,
            commit: applied ? 1UL : 0UL,
            applied: applied ? 1UL : 0UL,
            maxCommittedSizePerReady: 1);

        fixture.Core.Step(Hup(fixture.Core.Id));

        Assert.Equal(RaftRole.Candidate, fixture.Core.Role);
        Assert.Empty(fixture.Storage.EntryRequests);
    }

    [Theory]
    [MemberData(nameof(SimpleElectionOutcomes))]
    public void SimpleElectionMatchesMajorityOutcome(
        int clusterSize,
        string responses,
        RaftRole expectedRole)
    {
        ulong[] voters = Enumerable.Range(1, clusterSize)
            .Select(value => (ulong)value)
            .ToArray();
        RaftCore core = Create(voters: voters).Core;
        core.Step(Hup(core.Id));
        core.TakeMessages();
        StepAcceptedSelfMessages(core);

        foreach (string response in responses.Split(
                     ',',
                     StringSplitOptions.RemoveEmptyEntries))
        {
            ulong from = ulong.Parse(
                response[..^1],
                CultureInfo.InvariantCulture);
            bool granted = response[^1] == 'g';
            core.Step(VoteResponse(core, from, granted));
        }

        Assert.Equal(expectedRole, core.Role);
        Assert.Equal(1UL, core.Term);
    }

    [Fact]
    public void JointElectionRequiresBothConstituentMajorities()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            outgoingVoters: [1, 4, 5]).Core;
        core.Step(Hup(core.Id));
        core.TakeMessages();
        StepAcceptedSelfMessages(core);

        core.Step(VoteResponse(core, 2, granted: true));
        core.Step(VoteResponse(core, 3, granted: true));

        Assert.Equal(RaftRole.Candidate, core.Role);

        core.Step(VoteResponse(core, 4, granted: true));

        Assert.Equal(RaftRole.Leader, core.Role);
    }

    [Fact]
    public void UnknownDuplicateAndWrongRoleResponsesCannotCreateQuorum()
    {
        RaftCore core = Create(voters: [1, 2, 3]).Core;
        core.Step(Hup(core.Id));
        core.TakeMessages();
        StepAcceptedSelfMessages(core);

        core.Step(VoteResponse(core, 99, granted: true));
        Assert.Equal(RaftRole.Candidate, core.Role);

        core.Step(VoteResponse(core, 2, granted: false));
        core.Step(VoteResponse(core, 2, granted: true));

        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.False(core.Tracker.Votes[2]);

        core.Step(VoteResponse(core, 3, granted: true));
        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.Empty(core.Tracker.Votes);

        core.Step(VoteResponse(core, 2, granted: false));
        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.Empty(core.Tracker.Votes);
    }

    [Fact]
    public void ElectionLossPreservesTermAndPersistentSelfVote()
    {
        RaftCore core = Create(voters: [1, 2, 3]).Core;
        core.Step(Hup(core.Id));
        core.TakeMessages();
        StepAcceptedSelfMessages(core);

        core.Step(VoteResponse(core, 2, granted: false));
        core.Step(VoteResponse(core, 3, granted: false));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(1UL, core.Term);
        Assert.Equal(1UL, core.Vote);
        Assert.Equal(0UL, core.LeaderId);
        Assert.Empty(core.Tracker.Votes);
    }

    public static TheoryData<int, string, RaftRole>
        SimpleElectionOutcomes()
    {
        return new TheoryData<int, string, RaftRole>
        {
            { 1, "", RaftRole.Leader },
            { 3, "", RaftRole.Candidate },
            { 3, "2g", RaftRole.Leader },
            { 3, "2r,3r", RaftRole.Follower },
            { 5, "2g", RaftRole.Candidate },
            { 5, "2g,3g", RaftRole.Leader },
            { 5, "2r,3r", RaftRole.Candidate },
            { 5, "2r,3r,4r", RaftRole.Follower },
        };
    }

    private static void AssertHupIgnored(RaftCore core)
    {
        SoftState beforeSoftState = core.SoftState;
        HardState beforeHardState = core.HardState;
        ulong beforeLastIndex = core.Log.LastIndex;

        core.Step(Hup(core.Id));

        Assert.Equal(beforeSoftState, core.SoftState);
        Assert.Equal(beforeHardState, core.HardState);
        Assert.Equal(beforeLastIndex, core.Log.LastIndex);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }
}
