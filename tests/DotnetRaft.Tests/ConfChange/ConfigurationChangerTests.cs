using DotnetRaft.ConfChange;
using DotnetRaft.Protocol;
using DotnetRaft.Quorum;
using DotnetRaft.Tracker;

namespace DotnetRaft.Tests.ConfChange;

public sealed class ConfigurationChangerTests
{
    [Fact]
    public void FailedChangeLeavesLiveTrackerAndInflightsUnchanged()
    {
        var tracker = new ProgressTracker(4, 0);
        InstallSimple(tracker, 1, Voter(1));
        InstallSimple(tracker, 2, Voter(2));

        Progress originalProgress = tracker.Progress[2];
        originalProgress.BecomeReplicate();
        originalProgress.SentEntries(1, 64);
        TrackerConfig originalConfig = tracker.Config;
        ProgressMap originalMap = tracker.Progress;

        ConfigurationChangeException exception =
            Assert.Throws<ConfigurationChangeException>(
                () => new ConfigurationChanger(tracker, 10).Simple(
                    [Remove(1), Remove(2)]));

        Assert.Equal("removed all voters", exception.Message);
        Assert.Same(originalConfig, tracker.Config);
        Assert.Same(originalMap, tracker.Progress);
        Assert.Same(originalProgress, tracker.Progress[2]);
        Assert.True(
            tracker.Config.Voters.Incoming.SetEquals([1UL, 2UL]));
        Assert.Equal(ProgressState.Replicate, originalProgress.State);
        Assert.Equal(2UL, originalProgress.Next);
        Assert.Equal(1, originalProgress.Inflights.Count);
        Assert.Equal(64UL, originalProgress.Inflights.Bytes);
    }

    [Fact]
    public void SuccessfulCandidateSharesNoMutableStateWithLiveTracker()
    {
        var tracker = new ProgressTracker(4, 0);
        InstallSimple(tracker, 1, Voter(1));

        ConfigurationChangeResult result =
            new ConfigurationChanger(tracker, 5).Simple(
                [Learner(2)]);

        Assert.NotSame(tracker.Config, result.Config);
        Assert.NotSame(tracker.Progress, result.Progress);
        Assert.NotSame(
            tracker.Progress[1],
            result.Progress[1]);
        Assert.NotSame(
            tracker.Progress[1].Inflights,
            result.Progress[1].Inflights);

        result.Config.Voters.Incoming.Add(9);
        result.Config.Learners.Add(8);
        result.Progress[1].IsLearner = true;
        result.Progress[1].BecomeReplicate();

        Assert.True(
            tracker.Config.Voters.Incoming.SetEquals([1UL]));
        Assert.Empty(tracker.Config.Learners);
        Assert.False(tracker.Progress[1].IsLearner);
        Assert.Equal(ProgressState.Probe, tracker.Progress[1].State);
    }

    [Fact]
    public void PromotionAndDemotionPreserveCompleteProgressState()
    {
        var tracker = new ProgressTracker(4, 0);
        InstallSimple(tracker, 10, Voter(1));
        InstallSimple(tracker, 10, Voter(2));

        Progress live = tracker.Progress[2];
        Assert.True(live.MaybeUpdate(5));
        live.BecomeReplicate();
        live.SentEntries(2, 64);

        ConfigurationChangeResult demoted =
            new ConfigurationChanger(tracker, 10).Simple(
                [Learner(2)]);
        Progress learner = demoted.Progress[2];

        Assert.True(learner.IsLearner);
        Assert.Equal(ProgressState.Replicate, learner.State);
        Assert.Equal(5UL, learner.Match);
        Assert.Equal(8UL, learner.Next);
        Assert.Equal(1, learner.Inflights.Count);
        Assert.Equal(64UL, learner.Inflights.Bytes);
        Assert.False(live.IsLearner);

        Install(tracker, demoted);
        ConfigurationChangeResult promoted =
            new ConfigurationChanger(tracker, 10).Simple(
                [Voter(2)]);
        Progress voter = promoted.Progress[2];

        Assert.False(voter.IsLearner);
        Assert.Equal(ProgressState.Replicate, voter.State);
        Assert.Equal(5UL, voter.Match);
        Assert.Equal(8UL, voter.Next);
        Assert.Equal(1, voter.Inflights.Count);
        Assert.Equal(64UL, voter.Inflights.Bytes);
    }

    [Fact]
    public void JointDemotionStagesLearnerUntilJointExit()
    {
        var tracker = new ProgressTracker(4, 0);
        InstallSimple(tracker, 1, Voter(1));
        Assert.True(tracker.Progress[1].MaybeUpdate(1));

        ConfigurationChangeResult joint =
            new ConfigurationChanger(tracker, 1).EnterJoint(
                autoLeave: false,
                [Voter(2), Learner(1)]);

        Assert.True(joint.Config.Voters.Incoming.SetEquals([2UL]));
        Assert.True(joint.Config.Voters.Outgoing.SetEquals([1UL]));
        Assert.Empty(joint.Config.Learners);
        Assert.Equal([1UL], joint.Config.LearnersNext);
        Assert.False(joint.Progress[1].IsLearner);
        Assert.Equal(2UL, joint.Progress[1].Next);

        Install(tracker, joint);
        ConfigurationChangeResult left =
            new ConfigurationChanger(tracker, 1).LeaveJoint();

        Assert.True(left.Config.Voters.Incoming.SetEquals([2UL]));
        Assert.Empty(left.Config.Voters.Outgoing);
        Assert.Equal([1UL], left.Config.Learners);
        Assert.Empty(left.Config.LearnersNext);
        Assert.True(left.Progress[1].IsLearner);
        Assert.Equal(2UL, left.Progress[1].Next);
    }

    [Fact]
    public void OutgoingOnlyProgressIsRetainedUntilJointExit()
    {
        var tracker = new ProgressTracker(4, 0);
        InstallSimple(tracker, 1, Voter(1));
        InstallSimple(tracker, 1, Voter(2));

        ConfigurationChangeResult joint =
            new ConfigurationChanger(tracker, 1).EnterJoint(
                autoLeave: false,
                [Remove(2)]);

        Assert.True(joint.Config.Voters.Incoming.SetEquals([1UL]));
        Assert.True(
            joint.Config.Voters.Outgoing.SetEquals([1UL, 2UL]));
        Assert.True(joint.Progress.ContainsKey(2));

        Install(tracker, joint);
        ConfigurationChangeResult left =
            new ConfigurationChanger(tracker, 1).LeaveJoint();

        Assert.False(left.Progress.ContainsKey(2));
    }

    [Fact]
    public void NewProgressUsesPinnedNextIndexAndActivityDefaults()
    {
        var tracker = new ProgressTracker(3, 256);
        InstallSimple(tracker, 0, Voter(1));

        ConfigurationChangeResult result =
            new ConfigurationChanger(tracker, 42).Simple(
                [Learner(2)]);
        Progress progress = result.Progress[2];

        Assert.Equal(0UL, progress.Match);
        Assert.Equal(42UL, progress.Next);
        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.True(progress.RecentActive);
        Assert.True(progress.IsLearner);
        Assert.Equal(3, progress.Inflights.Capacity);
        Assert.Equal(256UL, progress.Inflights.MaxBytes);
    }

    [Fact]
    public void InvalidTrackerRoleAssignmentsAreRejectedBeforeChanges()
    {
        var incomingLearner = new ProgressTracker(4, 0)
        {
            Config = new TrackerConfig(
                new JointConfig(new MajorityConfig([1]))),
            Progress = new ProgressMap
            {
                [1] = NewProgress(isLearner: true),
            },
        };
        AssertInvalidSimple(incomingLearner);

        var outgoingLearner = new ProgressTracker(4, 0)
        {
            Config = new TrackerConfig(
                new JointConfig(
                    new MajorityConfig([2]),
                    new MajorityConfig([1]))),
            Progress = new ProgressMap
            {
                [1] = NewProgress(isLearner: true),
                [2] = NewProgress(),
            },
        };
        Assert.Throws<ConfigurationChangeException>(
            () => new ConfigurationChanger(
                outgoingLearner,
                1).LeaveJoint());

        var unmarkedLearner = new ProgressTracker(4, 0)
        {
            Config = new TrackerConfig(
                new JointConfig(new MajorityConfig([1])),
                learners: [2]),
            Progress = new ProgressMap
            {
                [1] = NewProgress(),
                [2] = NewProgress(),
            },
        };
        AssertInvalidSimple(unmarkedLearner);

        var unstaged = new ProgressTracker(4, 0)
        {
            Config = new TrackerConfig(
                new JointConfig(
                    new MajorityConfig([1]),
                    new MajorityConfig([1])),
                learnersNext: [2]),
            Progress = new ProgressMap
            {
                [1] = NewProgress(),
                [2] = NewProgress(),
            },
        };
        Assert.Throws<ConfigurationChangeException>(
            () => new ConfigurationChanger(unstaged, 1).LeaveJoint());

        var missingProgress = new ProgressTracker(4, 0)
        {
            Config = new TrackerConfig(
                new JointConfig(new MajorityConfig([1]))),
        };
        AssertInvalidSimple(missingProgress);

        var invalidNonJoint = new ProgressTracker(4, 0)
        {
            Config = new TrackerConfig(
                new JointConfig(new MajorityConfig([1])),
                autoLeave: true),
            Progress = new ProgressMap
            {
                [1] = NewProgress(),
            },
        };
        AssertInvalidSimple(invalidNonJoint);

        var overlap = new ProgressTracker(4, 0)
        {
            Config = new TrackerConfig(
                new JointConfig(new MajorityConfig([1])),
                learners: [1]),
            Progress = new ProgressMap
            {
                [1] = NewProgress(isLearner: true),
            },
        };
        AssertInvalidSimple(overlap);
    }

    [Fact]
    public void InvalidChangeInputsFailExplicitly()
    {
        var tracker = new ProgressTracker(4, 0);
        InstallSimple(tracker, 1, Voter(1));
        var changer = new ConfigurationChanger(tracker, 1);

        Assert.Throws<ArgumentNullException>(
            () => changer.Simple(null!));
        Assert.Throws<ArgumentException>(
            () => changer.Simple(
                new ConfChangeSingle[] { null! }));
        Assert.Throws<ConfigurationChangeException>(
            () => changer.Simple(
                [
                    new ConfChangeSingle
                    {
                        Type = (ConfChangeType)99,
                        NodeId = 2,
                    },
                ]));
    }

    private static void AssertInvalidSimple(
        ProgressTracker tracker)
    {
        Assert.Throws<ConfigurationChangeException>(
            () => new ConfigurationChanger(tracker, 1).Simple(
                [Update(1)]));
    }

    private static Progress NewProgress(bool isLearner = false)
    {
        return new Progress(
            match: 0,
            next: 1,
            maxInflightMessages: 4,
            maxInflightBytes: 0,
            isLearner);
    }

    private static void InstallSimple(
        ProgressTracker tracker,
        ulong lastIndex,
        params ConfChangeSingle[] changes)
    {
        Install(
            tracker,
            new ConfigurationChanger(tracker, lastIndex).Simple(
                changes));
    }

    private static void Install(
        ProgressTracker tracker,
        ConfigurationChangeResult result)
    {
        tracker.Config = result.Config;
        tracker.Progress = result.Progress;
    }

    private static ConfChangeSingle Voter(ulong id)
    {
        return Change(ConfChangeType.ConfChangeAddNode, id);
    }

    private static ConfChangeSingle Learner(ulong id)
    {
        return Change(
            ConfChangeType.ConfChangeAddLearnerNode,
            id);
    }

    private static ConfChangeSingle Remove(ulong id)
    {
        return Change(ConfChangeType.ConfChangeRemoveNode, id);
    }

    private static ConfChangeSingle Update(ulong id)
    {
        return Change(ConfChangeType.ConfChangeUpdateNode, id);
    }

    private static ConfChangeSingle Change(
        ConfChangeType type,
        ulong id)
    {
        return new ConfChangeSingle
        {
            Type = type,
            NodeId = id,
        };
    }
}
