using DotnetRaft;

namespace DotnetRaft.Examples.Counter;

internal static class Program
{
    // The lesson: submitting, committing, and applying a command are three
    // different events. Only the last one changes the application's counter.
    private static int Main(string[] args)
    {
        if (!TryParseArguments(args, out bool asyncStorage, out bool tracing, out int exitCode))
        {
            return exitCode;
        }

        CounterWriteTrace? trace = tracing ? new CounterWriteTrace() : null;
        Console.WriteLine(asyncStorage ? "Mode: asynchronous storage writes" : "Mode: Ready / Advance");
        var cluster = new CounterCluster(asyncStorage, trace);

        InitializeCluster(cluster);
        CounterReplica leader = ElectLeader(cluster);
        ulong commandIndex = SubmitIncrement(leader, trace);

        if (asyncStorage)
        {
            ObserveCommitBeforeApplication(cluster, leader, commandIndex);
        }
        else
        {
            Console.WriteLine("Delivering protocol messages and processing Ready batches.");
        }

        cluster.Drain();
        VerifyAndPrintReplicas(cluster, commandIndex);
        return 0;
    }

    private static void InitializeCluster(CounterCluster cluster)
    {
        // Bootstrap configuration must be stored and applied before campaigning.
        cluster.Drain();
    }

    private static CounterReplica ElectLeader(CounterCluster cluster)
    {
        CounterReplica candidate = cluster.Replicas[0];
        candidate.Node.Campaign();
        cluster.Drain();

        BasicStatus elected = candidate.Node.GetBasicStatus();
        if (elected.Role != RaftRole.Leader
            || cluster.Replicas.Any(replica =>
                replica.Node.GetBasicStatus().LeaderId != candidate.Id
                || replica.Applied != elected.Commit))
        {
            throw new InvalidOperationException("The cluster did not finish its initial election.");
        }

        Console.WriteLine($"Elected node {candidate.Id} in term {elected.Term}.");
        return candidate;
    }

    private static ulong SubmitIncrement(CounterReplica leader, CounterWriteTrace? trace)
    {
        ulong previousCommit = leader.Node.GetBasicStatus().Commit;
        ulong commandIndex = previousCommit + 1; // No uncommitted tail remains after the election.
        trace?.Follow(commandIndex);
        leader.Node.Propose(CounterReplica.IncrementCommand.Span);

        if (leader.Node.GetBasicStatus().Commit != previousCommit || leader.Count != 0)
        {
            throw new InvalidOperationException("The command must not commit or apply on submission alone.");
        }

        Console.WriteLine($"Proposed increment: commit={previousCommit}, counter={leader.Count}.");
        return commandIndex;
    }

    private static void ObserveCommitBeforeApplication(
        CounterCluster cluster,
        CounterReplica leader,
        ulong commandIndex)
    {
        // Storage and network work continue while the application workers pause.
        cluster.Drain(runApplyWorkers: false);
        ulong commit = leader.Node.GetBasicStatus().Commit;
        if (commit != commandIndex
            || cluster.Replicas.Any(replica => replica.Applied != commandIndex - 1 || replica.Count != 0))
        {
            throw new InvalidOperationException("Raft must commit before the paused apply workers change the counters.");
        }

        Console.WriteLine($"Committed at index {commit}: counter={leader.Count} (apply workers paused).");
    }

    private static void VerifyAndPrintReplicas(CounterCluster cluster, ulong commandIndex)
    {
        foreach (CounterReplica replica in cluster.Replicas)
        {
            BasicStatus status = replica.Node.GetBasicStatus();
            if (status.Commit != commandIndex
                || status.Applied != commandIndex
                || replica.Applied != commandIndex
                || replica.Count != 1)
            {
                throw new InvalidOperationException($"Node {replica.Id} did not commit and apply the increment exactly once.");
            }

            Console.WriteLine($"Node {replica.Id}: committed={status.Commit}, applied={replica.Applied}, counter={replica.Count}.");
        }
    }

    private static bool TryParseArguments(
        string[] args,
        out bool asyncStorage,
        out bool tracing,
        out int exitCode)
    {
        asyncStorage = false;
        tracing = false;
        exitCode = 0;
        if (args is ["--help"])
        {
            Console.WriteLine("Usage: dotnet run --project examples/DotnetRaft.Counter -- [--async-storage] [--trace]");
            return false;
        }

        if (args.Any(arg => arg is not ("--async-storage" or "--trace"))
            || args.Distinct(StringComparer.Ordinal).Count() != args.Length)
        {
            Console.Error.WriteLine("Expected --async-storage and/or --trace. Use --help for usage.");
            exitCode = 2;
            return false;
        }

        asyncStorage = args.Contains("--async-storage", StringComparer.Ordinal);
        tracing = args.Contains("--trace", StringComparer.Ordinal);
        return true;
    }
}
