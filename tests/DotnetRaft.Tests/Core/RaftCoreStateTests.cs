using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Read;
using DotnetRaft.Storage;
using DotnetRaft.Tracker;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreStateTests
{
    [Fact]
    public void SameTermFollowerPreservesVoteAndHigherTermClearsIt()
    {
        RaftCore core = NewCore(
            voters: [1, 2],
            term: 5,
            vote: 2);

        core.BecomeFollower(5, 2);

        Assert.Equal(5UL, core.Term);
        Assert.Equal(2UL, core.Vote);
        Assert.Equal(2UL, core.LeaderId);
        Assert.Equal(RaftRole.Follower, core.Role);

        core.BecomeFollower(6, 1);

        Assert.Equal(6UL, core.Term);
        Assert.Equal(0UL, core.Vote);
        Assert.Equal(1UL, core.LeaderId);
    }

    [Fact]
    public void LowerTermFollowerTransitionFailsAtomically()
    {
        RaftCore core = NewCore(
            voters: [1, 2],
            term: 5,
            vote: 2,
            lastIndex: 2);
        core.BecomeFollower(5, 2);
        core.Tracker.RecordVote(2, true);
        core.ReadOnly.AddRequest(2, ReadRequest("pending"));
        core.ElectionElapsed = 2;
        Progress beforeProgress = core.Tracker.Progress[1];
        SoftState beforeSoftState = core.SoftState;
        HardState beforeHardState = core.HardState;
        int beforeTimeout = core.RandomizedElectionTimeout;

        Assert.Throws<RaftInvariantException>(
            () => core.BecomeFollower(4, 3));

        Assert.Equal(beforeSoftState, core.SoftState);
        Assert.Equal(beforeHardState, core.HardState);
        Assert.Equal(2, core.ElectionElapsed);
        Assert.Equal(beforeTimeout, core.RandomizedElectionTimeout);
        Assert.Single(core.Tracker.Votes);
        Assert.Equal(1, core.ReadOnly.PendingCount);
        Assert.Same(beforeProgress, core.Tracker.Progress[1]);
    }

    [Fact]
    public void CandidateIncrementsTermVotesForSelfAndPerformsFullReset()
    {
        RaftCore core = NewCore(
            voters: [1, 2],
            term: 5,
            vote: 2,
            lastIndex: 2);
        core.BecomeFollower(5, 2);
        core.Tracker.RecordVote(2, true);
        core.ReadOnly.AddRequest(2, ReadRequest("pending"));
        core.ElectionElapsed = 3;

        core.BecomeCandidate();

        Assert.Equal(6UL, core.Term);
        Assert.Equal(1UL, core.Vote);
        Assert.Equal(0UL, core.LeaderId);
        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Equal(0, core.ElectionElapsed);
        Assert.Equal(0, core.HeartbeatElapsed);
        Assert.Empty(core.Tracker.Votes);
        Assert.Equal(0, core.ReadOnly.PendingCount);
        Assert.Equal(2UL, core.Tracker.Progress[1].Match);
        Assert.Equal(3UL, core.Tracker.Progress[1].Next);
    }

    [Fact]
    public void LeaderCannotBecomeCandidateOrPreCandidate()
    {
        RaftCore core = NewCore(voters: [1], term: 1);
        core.BecomeCandidate();
        core.BecomeLeader();
        HardState before = core.HardState;

        Assert.Throws<RaftInvariantException>(
            core.BecomeCandidate);
        Assert.Throws<RaftInvariantException>(
            core.BecomePreCandidate);

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.Equal(before, core.HardState);
    }

    [Fact]
    public void CandidateTermOverflowFailsAtomically()
    {
        RaftCore core = NewCore(
            voters: [1],
            term: ulong.MaxValue,
            vote: 1);
        SoftState beforeSoftState = core.SoftState;
        HardState beforeHardState = core.HardState;
        Progress beforeProgress = core.Tracker.Progress[1];

        Assert.Throws<RaftInvariantException>(
            core.BecomeCandidate);

        Assert.Equal(beforeSoftState, core.SoftState);
        Assert.Equal(beforeHardState, core.HardState);
        Assert.Same(beforeProgress, core.Tracker.Progress[1]);
    }

    [Fact]
    public void PreCandidateClearsVolatileElectionIdentityOnly()
    {
        RaftCore core = NewCore(
            voters: [1, 2],
            term: 5,
            vote: 2,
            lastIndex: 2);
        core.BecomeFollower(5, 2);
        core.Tracker.RecordVote(2, true);
        core.ReadOnly.AddRequest(2, ReadRequest("pending"));
        core.ElectionElapsed = 2;
        core.HeartbeatElapsed = 1;
        core.PendingConfigurationIndex = 7;
        core.UncommittedSize = 99;
        core.LeaderTransferee = 2;
        int timeout = core.RandomizedElectionTimeout;
        Progress progress = core.Tracker.Progress[2];
        progress.MaybeUpdate(1);
        progress.BecomeReplicate();

        core.BecomePreCandidate();

        Assert.Equal(RaftRole.PreCandidate, core.Role);
        Assert.Equal(5UL, core.Term);
        Assert.Equal(2UL, core.Vote);
        Assert.Equal(0UL, core.LeaderId);
        Assert.Empty(core.Tracker.Votes);
        Assert.Equal(2, core.ElectionElapsed);
        Assert.Equal(1, core.HeartbeatElapsed);
        Assert.Equal(timeout, core.RandomizedElectionTimeout);
        Assert.Equal(7UL, core.PendingConfigurationIndex);
        Assert.Equal(99UL, core.UncommittedSize);
        Assert.Equal(2UL, core.LeaderTransferee);
        Assert.Equal(1, core.ReadOnly.PendingCount);
        Assert.Same(progress, core.Tracker.Progress[2]);
        Assert.Equal(ProgressState.Replicate, progress.State);
        Assert.Equal(1UL, progress.Match);
    }

    [Fact]
    public void FullResetRestoresEveryProgressAndPreservesLearners()
    {
        RaftCore core = NewCore(
            voters: [1, 2],
            learners: [3],
            term: 5,
            vote: 2,
            lastIndex: 3);
        core.Tracker.RecordVote(2, true);
        core.ReadOnly.AddRequest(3, ReadRequest("pending"));
        core.PendingConfigurationIndex = 3;
        core.UncommittedSize = 100;
        core.LeaderTransferee = 2;
        core.ElectionElapsed = 3;
        core.HeartbeatElapsed = 2;

        Progress remote = core.Tracker.Progress[2];
        remote.MaybeUpdate(2);
        remote.BecomeReplicate();
        remote.SentEntries(1, 10);
        remote.RecordSentCommit(2);
        remote.RecentActive = true;

        Progress learner = core.Tracker.Progress[3];
        learner.BecomeSnapshot(3);
        learner.RecentActive = true;

        core.BecomeFollower(6, 0);

        Assert.Empty(core.Tracker.Votes);
        Assert.Equal(0, core.ReadOnly.PendingCount);
        Assert.Equal(0UL, core.PendingConfigurationIndex);
        Assert.Equal(0UL, core.UncommittedSize);
        Assert.Equal(0UL, core.LeaderTransferee);
        Assert.Equal(0, core.ElectionElapsed);
        Assert.Equal(0, core.HeartbeatElapsed);

        AssertResetProgress(core.Tracker.Progress[1], 3, 4, false);
        AssertResetProgress(remote, 0, 4, false);
        AssertResetProgress(learner, 0, 4, true);
    }

    [Fact]
    public void LeaderTransitionSetsLocalReplicationStateBeforeNoOpAck()
    {
        RaftCore core = NewCore(
            voters: [1, 2],
            term: 4,
            lastIndex: 3);
        core.BecomeCandidate();
        ulong lastIndex = core.Log.LastIndex;

        core.BecomeLeader();

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.Equal(core.Id, core.LeaderId);
        Assert.Equal(lastIndex + 1, core.Log.LastIndex);
        Assert.Equal(lastIndex, core.PendingConfigurationIndex);
        Progress local = core.Tracker.Progress[core.Id];
        Assert.Equal(ProgressState.Replicate, local.State);
        Assert.Equal(lastIndex, local.Match);
        Assert.Equal(lastIndex + 1, local.Next);
        Assert.True(local.RecentActive);
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgAppResp, response.Type);
        Assert.Equal(core.Id, response.To);
        Assert.Equal(lastIndex + 1, response.Index);
        Assert.Empty(core.TakeMessages());
    }

    [Fact]
    public void FollowerCannotBecomeLeader()
    {
        RaftCore core = NewCore(voters: [1], term: 1);

        Assert.Throws<RaftInvariantException>(
            core.BecomeLeader);

        Assert.Equal(RaftRole.Follower, core.Role);
    }

    [Fact]
    public void LeaderTransitionRequiresLocalProgress()
    {
        RaftCore core = NewCore(voters: [2], term: 1);
        core.BecomeCandidate();

        Assert.Throws<RaftInvariantException>(
            core.BecomeLeader);
    }

    [Fact]
    public void RandomizedElectionTimeoutCoversBothRangeEndpoints()
    {
        RaftCore low = NewCore(
            voters: [1],
            electionTick: 5,
            randomOffset: _ => 0);
        RaftCore high = NewCore(
            voters: [1],
            electionTick: 5,
            randomOffset: maximum => maximum - 1);

        Assert.Equal(5, low.RandomizedElectionTimeout);
        Assert.Equal(9, high.RandomizedElectionTimeout);
    }

    [Fact]
    public void LeaderTransitionRandomizesClockOncePerReset()
    {
        var calls = 0;
        RaftCore core = NewCore(
            voters: [1],
            randomOffset: _ =>
            {
                calls++;
                return 0;
            });
        Assert.Equal(1, calls);

        core.BecomeCandidate();
        Assert.Equal(2, calls);

        core.BecomeLeader();
        Assert.Equal(3, calls);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(5)]
    public void InvalidRandomOffsetFailsExplicitly(int offset)
    {
        Assert.Throws<RaftInvariantException>(
            () => NewCore(
                voters: [1],
                electionTick: 5,
                randomOffset: _ => offset));
    }

    [Fact]
    public void PromotableCoversMembershipAndSnapshotLifecycle()
    {
        Assert.True(NewCore(voters: [1]).Promotable);
        Assert.False(NewCore(voters: [2]).Promotable);
        Assert.False(NewCore(
            voters: [2],
            learners: [1]).Promotable);

        RaftCore snapshotCore = NewCore(voters: [1]);
        snapshotCore.Log.Restore(
            RaftCoreInitializationTests.SnapshotAt(
                5,
                2,
                voters: [1]));
        Assert.True(snapshotCore.Log.HasNextUnstableSnapshot);
        Assert.True(snapshotCore.Log.HasUnstableSnapshot);
        Assert.False(snapshotCore.Promotable);

        snapshotCore.Log.AcceptUnstable();
        Assert.False(snapshotCore.Log.HasNextUnstableSnapshot);
        Assert.True(snapshotCore.Log.HasUnstableSnapshot);
        Assert.False(snapshotCore.Promotable);

        snapshotCore.Log.AcknowledgeSnapshot(5);
        Assert.False(snapshotCore.Log.HasUnstableSnapshot);
        Assert.True(snapshotCore.Promotable);
    }

    [Fact]
    public void ElectionClockSignalsOnlyPromotableRandomizedTimeout()
    {
        RaftCore voter = NewCore(
            voters: [1],
            electionTick: 3,
            randomOffset: _ => 0);

        Assert.False(voter.TickElectionClock());
        Assert.False(voter.TickElectionClock());
        Assert.True(voter.TickElectionClock());
        Assert.Equal(0, voter.ElectionElapsed);

        RaftCore learner = NewCore(
            voters: [2],
            learners: [1],
            electionTick: 3,
            randomOffset: _ => 0);

        Assert.False(learner.TickElectionClock());
        Assert.False(learner.TickElectionClock());
        Assert.False(learner.TickElectionClock());
        Assert.Equal(3, learner.ElectionElapsed);
    }

    [Fact]
    public void LearnerStatusIsDerivedFromTrackerProgress()
    {
        RaftCore core = NewCore(voters: [1]);
        Progress local = core.Tracker.Progress[1];
        Assert.False(core.IsLearner);

        local.IsLearner = true;
        Assert.True(core.IsLearner);

        local.IsLearner = false;
        Assert.False(core.IsLearner);
    }

    [Fact]
    public void LeaderClocksSignalAndResetBothCadences()
    {
        RaftCore core = NewCore(
            voters: [1],
            electionTick: 4,
            heartbeatTick: 2);
        core.BecomeCandidate();
        core.BecomeLeader();

        Assert.Equal(
            new LeaderClockTick(false, false),
            core.TickLeaderClocks());
        Assert.Equal(
            new LeaderClockTick(false, true),
            core.TickLeaderClocks());
        Assert.Equal(2, core.ElectionElapsed);
        Assert.Equal(0, core.HeartbeatElapsed);

        Assert.Equal(
            new LeaderClockTick(false, false),
            core.TickLeaderClocks());
        Assert.Equal(
            new LeaderClockTick(true, true),
            core.TickLeaderClocks());
        Assert.Equal(0, core.ElectionElapsed);
        Assert.Equal(0, core.HeartbeatElapsed);
    }

    [Fact]
    public void LeaderClockRejectsNonLeaderAndElapsedOverflow()
    {
        RaftCore follower = NewCore(voters: [1]);
        Assert.Throws<RaftInvariantException>(
            () => follower.TickLeaderClocks());

        follower.ElectionElapsed = int.MaxValue;
        Assert.Throws<RaftInvariantException>(
            () => follower.TickElectionClock());
        Assert.Equal(int.MaxValue, follower.ElectionElapsed);
    }

    private static RaftCore NewCore(
        IEnumerable<ulong> voters,
        IEnumerable<ulong>? learners = null,
        ulong id = 1,
        ulong term = 0,
        ulong vote = 0,
        ulong lastIndex = 0,
        int electionTick = 10,
        int heartbeatTick = 1,
        Func<int, int>? randomOffset = null)
    {
        var storage = new CoreTestStorage();
        if (lastIndex > 0)
        {
            storage.LogStorage.Append(
                Enumerable.Range(1, checked((int)lastIndex))
                    .Select(index => new Entry
                    {
                        Index = (ulong)index,
                        Term = 1,
                    }));
        }

        storage.InitialState = new StorageState(
            new HardState
            {
                Term = term,
                Vote = vote,
                Commit = 0,
            },
            RaftCoreInitializationTests.ConfState(
                voters,
                learners));

        return new RaftCore(
            new RaftConfig
            {
                Id = id,
                Storage = storage,
                ElectionTick = electionTick,
                HeartbeatTick = heartbeatTick,
            },
            randomOffset ?? (_ => 0));
    }

    private static Message ReadRequest(string context)
    {
        var request = new Message
        {
            Type = MessageType.MsgReadIndex,
        };
        request.Entries.Add(new Entry
        {
            Data = Google.Protobuf.ByteString.CopyFromUtf8(context),
        });
        return request;
    }

    private static void AssertResetProgress(
        Progress progress,
        ulong match,
        ulong next,
        bool isLearner)
    {
        Assert.Equal(match, progress.Match);
        Assert.Equal(next, progress.Next);
        Assert.Equal(isLearner, progress.IsLearner);
        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.Equal(0UL, progress.PendingSnapshot);
        Assert.Equal(0UL, progress.LastSentCommit);
        Assert.False(progress.RecentActive);
        Assert.False(progress.AppendFlowPaused);
        Assert.Equal(0, progress.Inflights.Count);
        Assert.Equal(0UL, progress.Inflights.Bytes);
    }
}
