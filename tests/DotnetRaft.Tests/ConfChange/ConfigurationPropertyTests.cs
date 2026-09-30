using DotnetRaft.ConfChange;
using DotnetRaft.Protocol;
using DotnetRaft.Tracker;

namespace DotnetRaft.Tests.ConfChange;

public sealed class ConfigurationPropertyTests
{
    [Fact]
    public void SequentialSimpleAndJointChangesConverge()
    {
        var random = new Random(1);

        for (var iteration = 0; iteration < 1000; iteration++)
        {
            ConfChangeSingle[] setup = GenerateSetup(random);
            ConfChangeSingle[] changes = GenerateChanges(random);

            ProgressTracker simple = BuildSetup(setup);
            ProgressTracker joint = BuildSetup(setup);
            ProgressTracker automatic = BuildSetup(setup);

            foreach (ConfChangeSingle change in changes)
            {
                Install(
                    simple,
                    new ConfigurationChanger(simple, 10).Simple(
                        [change]));
            }

            ConfigurationChangeResult jointResult =
                new ConfigurationChanger(joint, 10).EnterJoint(
                    autoLeave: false,
                    changes);
            ConfigurationChangeResult automaticResult =
                new ConfigurationChanger(automatic, 10).EnterJoint(
                    autoLeave: true,
                    changes);

            AssertEquivalentExceptAutoLeave(
                jointResult,
                automaticResult,
                iteration);

            Install(joint, jointResult);
            Install(automatic, automaticResult);
            ConfigurationChangeResult jointFinal =
                new ConfigurationChanger(joint, 10).LeaveJoint();
            ConfigurationChangeResult automaticFinal =
                new ConfigurationChanger(
                    automatic,
                    10).LeaveJoint();

            AssertEquivalent(
                jointFinal,
                automaticFinal,
                iteration);
            AssertEquivalent(
                new ConfigurationChangeResult(
                    simple.Config,
                    simple.Progress),
                jointFinal,
                iteration);
        }
    }

    private static ProgressTracker BuildSetup(
        IEnumerable<ConfChangeSingle> setup)
    {
        var tracker = new ProgressTracker(10, 0);
        foreach (ConfChangeSingle change in setup)
        {
            Install(
                tracker,
                new ConfigurationChanger(tracker, 10).Simple(
                    [change]));
        }

        return tracker;
    }

    private static ConfChangeSingle[] GenerateSetup(Random random)
    {
        int count = 1 + random.Next(5);
        var changes = new List<ConfChangeSingle>
        {
            Voter(1),
        };

        for (var index = 0; index < count; index++)
        {
            changes.Add(Voter((ulong)(1 + random.Next(5))));
        }

        return [.. changes];
    }

    private static ConfChangeSingle[] GenerateChanges(Random random)
    {
        int count = 1 + random.Next(9);
        var changes = new ConfChangeSingle[count];

        for (var index = 0; index < changes.Length; index++)
        {
            changes[index] = new ConfChangeSingle
            {
                Type = (ConfChangeType)random.Next(4),
                NodeId = (ulong)(2 + random.Next(9)),
            };
        }

        return changes;
    }

    private static void AssertEquivalentExceptAutoLeave(
        ConfigurationChangeResult expected,
        ConfigurationChangeResult actual,
        int iteration)
    {
        ConfState expectedState = expected.Config.ToConfState();
        ConfState actualState = actual.Config.ToConfState();

        Assert.False(expectedState.AutoLeave);
        Assert.True(actualState.AutoLeave);
        actualState.AutoLeave = false;
        Assert.True(
            expectedState.IsEquivalentTo(actualState),
            $"Joint membership mismatch at iteration {iteration}.");
        AssertProgressEqual(
            expected.Progress,
            actual.Progress,
            iteration);
    }

    private static void AssertEquivalent(
        ConfigurationChangeResult expected,
        ConfigurationChangeResult actual,
        int iteration)
    {
        Assert.True(
            expected.Config.ToConfState().IsEquivalentTo(
                actual.Config.ToConfState()),
            $"Configuration mismatch at iteration {iteration}.");
        AssertProgressEqual(
            expected.Progress,
            actual.Progress,
            iteration);
    }

    private static void AssertProgressEqual(
        ProgressMap expected,
        ProgressMap actual,
        int iteration)
    {
        Assert.Equal(
            expected.Keys.Order(),
            actual.Keys.Order());

        foreach ((ulong id, Progress expectedProgress) in expected)
        {
            Progress actualProgress = actual[id];
            Assert.True(
                ProgressEquals(
                    expectedProgress,
                    actualProgress),
                $"Progress {id} mismatch at iteration {iteration}." +
                $"\nExpected: {expectedProgress}" +
                $"\nActual: {actualProgress}");
        }
    }

    private static bool ProgressEquals(
        Progress left,
        Progress right)
    {
        return left.Match == right.Match
            && left.Next == right.Next
            && left.LastSentCommit == right.LastSentCommit
            && left.State == right.State
            && left.PendingSnapshot == right.PendingSnapshot
            && left.RecentActive == right.RecentActive
            && left.AppendFlowPaused == right.AppendFlowPaused
            && left.IsLearner == right.IsLearner
            && left.Inflights.Count == right.Inflights.Count
            && left.Inflights.Bytes == right.Inflights.Bytes
            && left.Inflights.Capacity == right.Inflights.Capacity
            && left.Inflights.MaxBytes == right.Inflights.MaxBytes;
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
        return new ConfChangeSingle
        {
            Type = ConfChangeType.ConfChangeAddNode,
            NodeId = id,
        };
    }
}
