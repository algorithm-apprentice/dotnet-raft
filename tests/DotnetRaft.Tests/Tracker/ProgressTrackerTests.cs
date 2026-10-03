using DotnetRaft.Protocol;
using DotnetRaft.Quorum;
using DotnetRaft.Tracker;

namespace DotnetRaft.Tests.Tracker;

public sealed class ProgressTrackerTests
{
    [Fact]
    public void ConstructorStartsEmptyAndStoresInflightLimits()
    {
        var tracker = new ProgressTracker(
            maxInflightMessages: 8,
            maxInflightBytes: 1024);

        Assert.Equal(8, tracker.MaxInflightMessages);
        Assert.Equal(1024UL, tracker.MaxInflightBytes);
        Assert.Empty(tracker.Config.Voters.Incoming);
        Assert.Empty(tracker.Config.Voters.Outgoing);
        Assert.Empty(tracker.Config.Learners);
        Assert.Empty(tracker.Config.LearnersNext);
        Assert.False(tracker.Config.AutoLeave);
        Assert.Empty(tracker.Progress);
        Assert.Empty(tracker.Votes);
        Assert.False(tracker.IsSingleton);

        ConfState state = tracker.ToConfState();
        Assert.Empty(state.Voters);
        Assert.Empty(state.VotersOutgoing);
        Assert.Empty(state.Learners);
        Assert.Empty(state.LearnersNext);
        Assert.True(state.HasAutoLeave);
        Assert.False(state.AutoLeave);

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new ProgressTracker(-1, 0));
    }

    [Fact]
    public void InstallReplacesOwnedStateWithoutResettingVotes()
    {
        var tracker = new ProgressTracker(4, 0);
        tracker.RecordVote(9, granted: true);
        var config = new TrackerConfig(
            new JointConfig(
                new MajorityConfig([1, 2])));
        var progress = new ProgressMap
        {
            [1] = NewProgress(1),
            [2] = NewProgress(2),
        };

        tracker.Install(config, progress);

        Assert.Same(config, tracker.Config);
        Assert.Same(progress, tracker.Progress);
        Assert.True(tracker.Votes[9]);

        Assert.Throws<ArgumentNullException>(
            () => tracker.Install(null!, progress));
        Assert.Throws<ArgumentNullException>(
            () => tracker.Install(config, null!));
        Assert.Same(config, tracker.Config);
        Assert.Same(progress, tracker.Progress);
    }

    [Fact]
    public void MembershipQueriesDeriveFromInstalledState()
    {
        var tracker = new ProgressTracker(4, 0);
        var config = new TrackerConfig(
            new JointConfig(
                new MajorityConfig([1, 5]),
                new MajorityConfig([2])),
            learners: [3],
            learnersNext: [2]);
        var progress = new ProgressMap
        {
            [1] = NewProgress(0),
            [2] = NewProgress(0),
            [3] = NewProgress(0, isLearner: true),
            [4] = NewProgress(0),
        };
        tracker.Install(config, progress);

        Assert.True(tracker.Contains(1));
        Assert.True(tracker.Contains(4));
        Assert.False(tracker.Contains(5));
        Assert.False(tracker.IsLearner(1));
        Assert.True(tracker.IsLearner(3));
        Assert.False(tracker.IsLearner(5));
        Assert.True(tracker.IsVoter(1));
        Assert.True(tracker.IsVoter(2));
        Assert.False(tracker.IsVoter(3));
        Assert.False(tracker.IsVoter(4));
        Assert.False(tracker.IsVoter(5));
        Assert.False(tracker.IsSingleton);

        var singleton = new TrackerConfig(
            new JointConfig(
                new MajorityConfig([1])));
        tracker.Install(singleton, progress);

        Assert.True(tracker.IsSingleton);
    }

    [Fact]
    public void TrackerConfigOwnsAllConstructorInputs()
    {
        var incoming = new MajorityConfig([1, 2]);
        var outgoing = new MajorityConfig([2, 3]);
        var learners = new HashSet<ulong> { 4 };
        var learnersNext = new HashSet<ulong> { 3 };

        var config = new TrackerConfig(
            new JointConfig(incoming, outgoing),
            learners,
            learnersNext,
            autoLeave: true);

        incoming.Add(9);
        outgoing.Remove(3);
        learners.Add(5);
        learnersNext.Clear();

        Assert.True(config.Voters.Incoming.SetEquals([1UL, 2UL]));
        Assert.True(config.Voters.Outgoing.SetEquals([2UL, 3UL]));
        Assert.Equal([4UL], config.Learners);
        Assert.Equal([3UL], config.LearnersNext);
        Assert.True(config.AutoLeave);
    }

    [Fact]
    public void TrackerConfigCloneIsIndependentAndCopiesAutoLeave()
    {
        var original = new TrackerConfig(
            new JointConfig(
                new MajorityConfig([1, 2]),
                new MajorityConfig([2, 3])),
            learners: [4],
            learnersNext: [3],
            autoLeave: true);

        TrackerConfig clone = original.Clone();

        Assert.True(clone.Voters.Incoming.SetEquals([1UL, 2UL]));
        Assert.True(clone.Voters.Outgoing.SetEquals([2UL, 3UL]));
        Assert.Equal([4UL], clone.Learners);
        Assert.Equal([3UL], clone.LearnersNext);
        Assert.True(clone.AutoLeave);

        clone.Voters.Incoming.Add(9);
        clone.Voters.Outgoing.Remove(3);
        clone.Learners.Add(5);
        clone.LearnersNext.Clear();
        clone.AutoLeave = false;

        Assert.True(original.Voters.Incoming.SetEquals([1UL, 2UL]));
        Assert.True(original.Voters.Outgoing.SetEquals([2UL, 3UL]));
        Assert.Equal([4UL], original.Learners);
        Assert.Equal([3UL], original.LearnersNext);
        Assert.True(original.AutoLeave);
    }

    [Fact]
    public void ConfStateIsSortedPresentAndDefensivelyOwned()
    {
        var config = new TrackerConfig(
            new JointConfig(
                new MajorityConfig([3, 1, 2]),
                new MajorityConfig([3, 1])),
            learners: [8, 7],
            learnersNext: [6, 5],
            autoLeave: true);

        ConfState state = config.ToConfState();

        Assert.Equal([1UL, 2UL, 3UL], state.Voters);
        Assert.Equal([1UL, 3UL], state.VotersOutgoing);
        Assert.Equal([7UL, 8UL], state.Learners);
        Assert.Equal([5UL, 6UL], state.LearnersNext);
        Assert.True(state.HasAutoLeave);
        Assert.True(state.AutoLeave);

        state.Voters.Clear();
        state.Learners.Add(99);
        state.AutoLeave = false;

        ConfState second = config.ToConfState();
        Assert.Equal([1UL, 2UL, 3UL], second.Voters);
        Assert.Equal([7UL, 8UL], second.Learners);
        Assert.True(second.AutoLeave);
    }

    [Fact]
    public void TrackerConfigStringMatchesReferenceLayout()
    {
        var config = new TrackerConfig(
            new JointConfig(
                new MajorityConfig([3, 1, 2]),
                new MajorityConfig([2, 1])),
            learners: [4],
            learnersNext: [2],
            autoLeave: true);

        Assert.Equal(
            "voters=(1 2 3)&&(1 2) learners=(4) learners_next=(2) autoleave",
            config.ToString());
    }

    [Fact]
    public void ProgressMapFormattingAndVisitationUseStableIdOrder()
    {
        var tracker = new ProgressTracker(4, 0);
        var first = NewProgress(match: 0, recentActive: true);
        var second = new Progress(1, 2, 4, 0, recentActive: true);
        var third = new Progress(2, 3, 4, 0, recentActive: true);
        tracker.Install(
            tracker.Config,
            new ProgressMap
            {
                [3] = third,
                [1] = first,
                [2] = second,
            });

        const string expected =
            "1: StateProbe match=0 next=1\n" +
            "2: StateProbe match=1 next=2\n" +
            "3: StateProbe match=2 next=3\n";
        Assert.Equal(expected, tracker.Progress.ToString());

        var visitedIds = new List<ulong>();
        var visitedProgress = new List<Progress>();
        tracker.Visit((id, progress) =>
        {
            visitedIds.Add(id);
            visitedProgress.Add(progress);
        });

        Assert.Equal([1UL, 2UL, 3UL], visitedIds);
        Assert.Same(first, visitedProgress[0]);
        Assert.Same(second, visitedProgress[1]);
        Assert.Same(third, visitedProgress[2]);
        Assert.Throws<ArgumentNullException>(
            () => tracker.Visit(null!));
    }

    [Fact]
    public void CommittedIndexUsesOnlyConfiguredVoterQuorums()
    {
        ProgressTracker tracker = NewTracker(
            new TrackerConfig(
                new JointConfig(new MajorityConfig([1, 2, 3])),
                learners: [4]),
            new ProgressMap
            {
                [1] = NewProgress(30),
                [2] = NewProgress(20),
                [3] = NewProgress(10),
                [4] = NewProgress(100, isLearner: true),
                [5] = NewProgress(5),
            });

        Assert.Equal(20UL, tracker.CommittedIndex);

        tracker.Install(
            new TrackerConfig(
                new JointConfig(
                    new MajorityConfig([1, 2, 3]),
                    new MajorityConfig([2, 3, 5])),
                learners: [4]),
            tracker.Progress);

        Assert.Equal(10UL, tracker.CommittedIndex);
    }

    [Fact]
    public void QuorumActivePreservesEveryActivityFlag()
    {
        ProgressTracker tracker = NewTracker(
            new TrackerConfig(
                new JointConfig(new MajorityConfig([1, 2, 3])),
                learners: [4]),
            new ProgressMap
            {
                [1] = NewProgress(0, recentActive: true),
                [2] = NewProgress(0, recentActive: true),
                [3] = NewProgress(0, recentActive: false),
                [4] = NewProgress(
                    0,
                    isLearner: true,
                    recentActive: true),
            });

        Dictionary<ulong, bool> before = ActivitySnapshot(tracker);
        Assert.True(tracker.QuorumActive());
        Assert.Equal(before, ActivitySnapshot(tracker));

        tracker.Progress[2].RecentActive = false;
        before = ActivitySnapshot(tracker);
        Assert.False(tracker.QuorumActive());
        Assert.Equal(before, ActivitySnapshot(tracker));
    }

    [Fact]
    public void JointActivityCountsStagedButNotActiveLearners()
    {
        ProgressTracker tracker = NewTracker(
            new TrackerConfig(
                new JointConfig(
                    new MajorityConfig([1, 2, 3]),
                    new MajorityConfig([2, 3, 4])),
                learners: [5],
                learnersNext: [4]),
            new ProgressMap
            {
                [1] = NewProgress(0, recentActive: true),
                [2] = NewProgress(0, recentActive: true),
                [3] = NewProgress(0, recentActive: false),
                [4] = NewProgress(0, recentActive: true),
                [5] = NewProgress(
                    0,
                    isLearner: true,
                    recentActive: true),
            });

        Dictionary<ulong, bool> before = ActivitySnapshot(tracker);
        Assert.True(tracker.QuorumActive());
        Assert.Equal(before, ActivitySnapshot(tracker));

        tracker.Progress[4].RecentActive = false;
        before = ActivitySnapshot(tracker);
        Assert.False(tracker.QuorumActive());
        Assert.Equal(before, ActivitySnapshot(tracker));
    }

    [Fact]
    public void NodeListsAreSortedOwnedAndRespectLearnerStages()
    {
        ProgressTracker tracker = NewTracker(
            new TrackerConfig(
                new JointConfig(
                    new MajorityConfig([3, 1]),
                    new MajorityConfig([4, 3])),
                learners: [9, 7],
                learnersNext: [4]));

        ulong[] voters = tracker.VoterNodes();
        ulong[] learners = tracker.LearnerNodes();

        Assert.Equal([1UL, 3UL, 4UL], voters);
        Assert.Equal([7UL, 9UL], learners);

        voters[0] = 99;
        learners[0] = 99;
        Assert.Equal([1UL, 3UL, 4UL], tracker.VoterNodes());
        Assert.Equal([7UL, 9UL], tracker.LearnerNodes());
    }

    [Fact]
    public void SingletonRequiresOneIncomingVoterAndNoOutgoingVoters()
    {
        var tracker = new ProgressTracker(4, 0);

        tracker.Install(
            new TrackerConfig(
                new JointConfig(new MajorityConfig([1]))),
            tracker.Progress);
        Assert.True(tracker.IsSingleton);

        tracker.Install(
            new TrackerConfig(
                new JointConfig(
                    new MajorityConfig([1]),
                    new MajorityConfig([1]))),
            tracker.Progress);
        Assert.False(tracker.IsSingleton);

        tracker.Install(
            new TrackerConfig(
                new JointConfig(new MajorityConfig([1, 2]))),
            tracker.Progress);
        Assert.False(tracker.IsSingleton);
    }

    [Fact]
    public void VoteRecordingIsFirstWinsAndCountsTrackedNonLearners()
    {
        ProgressTracker tracker = NewTracker(
            new TrackerConfig(
                new JointConfig(new MajorityConfig([1, 2, 3])),
                learners: [4]),
            new ProgressMap
            {
                [1] = NewProgress(0),
                [2] = NewProgress(0),
                [3] = NewProgress(0),
                [4] = NewProgress(0, isLearner: true),
                [5] = NewProgress(0),
            });

        tracker.RecordVote(1, granted: true);
        tracker.RecordVote(1, granted: false);
        tracker.RecordVote(2, granted: false);
        tracker.RecordVote(2, granted: true);
        tracker.RecordVote(4, granted: true);
        tracker.RecordVote(5, granted: true);
        tracker.RecordVote(99, granted: true);

        (int granted, int rejected, VoteResult result) =
            tracker.TallyVotes();

        Assert.True(tracker.Votes[1]);
        Assert.False(tracker.Votes[2]);
        Assert.Equal(2, granted);
        Assert.Equal(1, rejected);
        Assert.Equal(VoteResult.Pending, result);

        tracker.RecordVote(3, granted: false);
        (granted, rejected, result) = tracker.TallyVotes();
        Assert.Equal(2, granted);
        Assert.Equal(2, rejected);
        Assert.Equal(VoteResult.Lost, result);
    }

    [Fact]
    public void VoteResetAndJointResultsMatchBothMajorities()
    {
        ProgressTracker tracker = NewTracker(
            new TrackerConfig(
                new JointConfig(
                    new MajorityConfig([1, 2, 3]),
                    new MajorityConfig([2, 3, 4]))),
            new ProgressMap
            {
                [1] = NewProgress(0),
                [2] = NewProgress(0),
                [3] = NewProgress(0),
                [4] = NewProgress(0),
            });

        tracker.RecordVote(1, granted: true);
        tracker.RecordVote(2, granted: true);
        Assert.Equal(VoteResult.Pending, tracker.TallyVotes().Result);

        tracker.RecordVote(3, granted: true);
        Assert.Equal(VoteResult.Won, tracker.TallyVotes().Result);

        tracker.ResetVotes();
        Assert.Empty(tracker.Votes);
        Assert.Equal(
            (0, 0, VoteResult.Pending),
            tracker.TallyVotes());

        tracker.RecordVote(1, granted: false);
        tracker.RecordVote(2, granted: false);
        Assert.Equal(VoteResult.Lost, tracker.TallyVotes().Result);
    }

    private static Progress NewProgress(
        ulong match,
        bool isLearner = false,
        bool recentActive = false)
    {
        return new Progress(
            match,
            checked(match + 1),
            maxInflightMessages: 4,
            maxInflightBytes: 0,
            isLearner,
            recentActive);
    }

    private static ProgressTracker NewTracker(
        TrackerConfig config,
        ProgressMap? progress = null)
    {
        var tracker = new ProgressTracker(4, 0);
        tracker.Install(
            config,
            progress ?? []);
        return tracker;
    }

    private static Dictionary<ulong, bool> ActivitySnapshot(
        ProgressTracker tracker)
    {
        return tracker.Progress.ToDictionary(
            pair => pair.Key,
            pair => pair.Value.RecentActive);
    }
}
