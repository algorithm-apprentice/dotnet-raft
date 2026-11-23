using DotnetRaft.Tests.Interaction;

if (args is ["preview-golden", string fixture])
{
    Console.Write(InteractionScriptRunner.Preview(File.ReadAllText(fixture), fixture));
    return 0;
}

if (args is ["compare-raft", string scheduleFile, string mode, string referenceFile] && mode is "sync" or "async")
{
    try
    {
        DifferentialSchedule schedule = RaftDifferential.ReadSchedule(scheduleFile);
        if (mode == "sync" && RaftDifferential.IsAsyncOnly(scheduleFile))
        {
            throw new InvalidDataException($"{schedule.Name} requires async storage.");
        }

        DifferentialCheckpoint[] checkpoints = RaftDifferential.Run(schedule, mode == "async");
        RaftDifferential.VerifyWitnesses(schedule, checkpoints);
        RaftDifferential.Compare(schedule.Name, mode, File.ReadAllText(referenceFile), checkpoints);
        Console.WriteLine($"{schedule.Name}/{mode}: {checkpoints.Length} checkpoints match pinned etcd/raft");
    }
    catch (Exception exception)
    {
        Console.Error.WriteLine(exception);
        return 1;
    }

    return 0;
}

Console.Error.WriteLine("Usage: preview-golden <interaction-fixture> | compare-raft <schedule> <sync|async> <reference-output>");
return 2;
