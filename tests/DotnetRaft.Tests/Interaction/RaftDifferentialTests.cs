using System.Text.Json;

using DotnetRaft.Tests.Release;

namespace DotnetRaft.Tests.Interaction;

public sealed class RaftDifferentialTests
{
    private static readonly string ScheduleDirectory = Path.Combine(ReleaseTestPaths.RepositoryRoot,
        "tests", "DotnetRaft.Tests", "Interaction", "DifferentialTestData");

    public static TheoryData<string, bool> Schedules()
    {
        var data = new TheoryData<string, bool>();
        foreach (string path in Directory.EnumerateFiles(ScheduleDirectory, "*.json").Order(StringComparer.Ordinal))
        {
            string name = Path.GetFileName(path);
            if (!RaftDifferential.IsAsyncOnly(name))
            {
                data.Add(name, false);
            }

            data.Add(name, true);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Schedules))]
    public void CheckpointsWitnessProgressAndNotJustAgreement(string file, bool asyncStorage)
    {
        DifferentialSchedule schedule = ReadSchedule(file);
        RaftDifferential.VerifyWitnesses(schedule, RaftDifferential.Run(schedule, asyncStorage));
    }

    [Fact]
    public void ComparisonRejectsDifferentAppliedBytesAtTheSameIndex()
    {
        DifferentialSchedule schedule = ReadSchedule("failover-read.json");
        DifferentialCheckpoint[] observed = RaftDifferential.Run(schedule, asyncStorage: true);
        RaftDifferential.VerifyWitnesses(schedule, observed);
        string reference = JsonSerializer.Serialize(observed, RaftDifferential.Json);
        RaftDifferential.Compare(schedule.Name, "async", reference, observed);

        DifferentialNode follower = observed[0].Nodes[1];
        follower.Entries[^1] = follower.Entries[^1] with { Data = Convert.ToBase64String("wrong/"u8) };
        InvalidDataException failure = Assert.Throws<InvalidDataException>(
            () => RaftDifferential.Compare(schedule.Name, "async", reference, observed));
        Assert.Contains("failover-read/async/first-read", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ComparisonDoesNotDefaultMissingObservationFields()
    {
        DifferentialSchedule schedule = ReadSchedule("failover-read.json");
        DifferentialCheckpoint[] checkpoints = RaftDifferential.Run(schedule, asyncStorage: false);
        string reference = JsonSerializer.Serialize(checkpoints, RaftDifferential.Json)
            .Replace("\"vote\":0,", "", StringComparison.Ordinal);
        InvalidDataException failure = Assert.Throws<InvalidDataException>(
            () => RaftDifferential.Compare(schedule.Name, "sync", reference, checkpoints));
        Assert.Contains("failover-read/sync/healed", failure.Message, StringComparison.Ordinal);
    }

    private static DifferentialSchedule ReadSchedule(string file) =>
        RaftDifferential.ReadSchedule(Path.Combine(ScheduleDirectory, file));
}
