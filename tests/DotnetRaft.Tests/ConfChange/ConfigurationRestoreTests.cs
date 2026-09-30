using DotnetRaft.ConfChange;
using DotnetRaft.Protocol;
using DotnetRaft.Tracker;

namespace DotnetRaft.Tests.ConfChange;

public sealed class ConfigurationRestoreTests
{
    [Theory]
    [MemberData(nameof(ValidStates))]
    public void RestoreReproducesPinnedUnitStates(ConfState state)
    {
        var tracker = new ProgressTracker(20, 0);
        ConfState input = state.Clone();

        ConfigurationChangeResult result =
            ConfigurationRestore.Restore(
                new ConfigurationChanger(tracker, 10),
                state);

        Assert.True(input.IsEquivalentTo(result.Config.ToConfState()));
        Assert.Equal(input, state);
        Assert.Empty(tracker.Config.Voters.Incoming);
        Assert.Empty(tracker.Config.Voters.Outgoing);
        Assert.Empty(tracker.Progress);

        foreach (Progress progress in result.Progress.Values)
        {
            Assert.Equal(10UL, progress.Next);
            Assert.True(progress.RecentActive);
        }
    }

    [Fact]
    public void EmptyRestoreReturnsFreshOwnedObjects()
    {
        var tracker = new ProgressTracker(4, 0);

        ConfigurationChangeResult result =
            ConfigurationRestore.Restore(
                new ConfigurationChanger(tracker, 0),
                new ConfState());

        Assert.NotSame(tracker.Config, result.Config);
        Assert.NotSame(tracker.Progress, result.Progress);
        result.Config.Voters.Incoming.Add(1);
        result.Progress[1] = new Progress(0, 1, 4, 0);

        Assert.Empty(tracker.Config.Voters.Incoming);
        Assert.Empty(tracker.Progress);
    }

    [Fact]
    public void RestoreRejectsNonEmptyStartingTracker()
    {
        var tracker = new ProgressTracker(4, 0);
        ConfigurationChangeResult initial =
            new ConfigurationChanger(tracker, 1).Simple(
                [Voter(1)]);
        tracker.Config = initial.Config;
        tracker.Progress = initial.Progress;

        Assert.Throws<ConfigurationChangeException>(
            () => ConfigurationRestore.Restore(
                new ConfigurationChanger(tracker, 1),
                new ConfState
                {
                    Voters = { 1 },
                }));
    }

    [Fact]
    public void LateRestoreFailureDoesNotMutateSuppliedTracker()
    {
        var tracker = new ProgressTracker(4, 0);
        var invalid = new ConfState
        {
            VotersOutgoing = { 1 },
        };

        Assert.Throws<ConfigurationChangeException>(
            () => ConfigurationRestore.Restore(
                new ConfigurationChanger(tracker, 10),
                invalid));

        Assert.Empty(tracker.Config.Voters.Incoming);
        Assert.Empty(tracker.Config.Voters.Outgoing);
        Assert.Empty(tracker.Progress);
    }

    [Theory]
    [MemberData(nameof(InvalidStates))]
    public void RestoreRejectsMalformedPersistedState(ConfState state)
    {
        var tracker = new ProgressTracker(4, 0);

        Assert.Throws<ConfigurationChangeException>(
            () => ConfigurationRestore.Restore(
                new ConfigurationChanger(tracker, 10),
                state));

        Assert.Empty(tracker.Config.Voters.Incoming);
        Assert.Empty(tracker.Config.Voters.Outgoing);
        Assert.Empty(tracker.Progress);
    }

    [Fact]
    public void RandomValidStatesRoundTripThroughRestore()
    {
        var random = new Random(1);

        for (var iteration = 0; iteration < 1000; iteration++)
        {
            ConfState state = GenerateValidState(random);
            var tracker = new ProgressTracker(20, 0);

            ConfigurationChangeResult result =
                ConfigurationRestore.Restore(
                    new ConfigurationChanger(tracker, 10),
                    state);

            Assert.True(
                state.IsEquivalentTo(result.Config.ToConfState()),
                $"Restore mismatch at iteration {iteration}: {state}");
            Assert.Empty(tracker.Progress);
        }
    }

    public static TheoryData<ConfState> ValidStates()
    {
        return new TheoryData<ConfState>
        {
            new ConfState
            {
                AutoLeave = false,
            },
            new ConfState
            {
                Voters = { 1, 2, 3 },
                AutoLeave = false,
            },
            new ConfState
            {
                Voters = { 1, 2, 3 },
                Learners = { 4, 5, 6 },
                AutoLeave = false,
            },
            new ConfState
            {
                Voters = { 1, 2, 3 },
                Learners = { 5 },
                VotersOutgoing = { 1, 2, 4, 6 },
                LearnersNext = { 4 },
                AutoLeave = true,
            },
        };
    }

    public static TheoryData<ConfState> InvalidStates()
    {
        return new TheoryData<ConfState>
        {
            new ConfState
            {
                Voters = { 1 },
                LearnersNext = { 2 },
                AutoLeave = false,
            },
            new ConfState
            {
                Voters = { 1 },
                AutoLeave = true,
            },
            new ConfState
            {
                Voters = { 1 },
                Learners = { 1 },
                AutoLeave = false,
            },
            new ConfState
            {
                Voters = { 1, 1 },
                AutoLeave = false,
            },
            new ConfState
            {
                Voters = { 0, 1 },
                AutoLeave = false,
            },
            new ConfState
            {
                Voters = { 1 },
                VotersOutgoing = { 1 },
                LearnersNext = { 2 },
                AutoLeave = false,
            },
        };
    }

    private static ConfState GenerateValidState(Random random)
    {
        int voterCount = 1 + random.Next(5);
        int learnerCount = random.Next(5);
        int removedVoterCount = random.Next(3);
        int totalIds = 2
            * (voterCount + learnerCount + removedVoterCount);
        ulong[] ids = Enumerable.Range(1, totalIds)
            .Select(value => (ulong)value)
            .OrderBy(_ => random.Next())
            .ToArray();
        var position = 0;

        ulong[] voters = ids[
            position..(position + voterCount)];
        position += voterCount;
        ulong[] learners = ids[
            position..(position + learnerCount)];
        position += learnerCount;
        ulong[] removedVoters = ids[
            position..(position + removedVoterCount)];

        int retainedCount = random.Next(voterCount + 1);
        ulong[] outgoing = voters.Take(retainedCount)
            .Concat(removedVoters)
            .ToArray();
        int learnerNextCount = removedVoterCount == 0
            ? 0
            : random.Next(removedVoterCount + 1);

        var state = new ConfState
        {
            AutoLeave = outgoing.Length > 0
                && random.Next(2) == 1,
        };
        state.Voters.Add(voters);
        state.Learners.Add(learners);
        state.VotersOutgoing.Add(outgoing);
        state.LearnersNext.Add(
            removedVoters.Take(learnerNextCount));
        return state;
    }

    private static ConfChangeSingle Voter(ulong id)
    {
        return new ConfChangeSingle
        {
            Type = ConfChangeType.ConfChangeAddNode,
            NodeId = id,
        };
    }
}
