using DotnetRaft;
using DotnetRaft.Examples.Counter;

if (args is ["--help"])
{
    Console.WriteLine("Usage: dotnet run --project examples/DotnetRaft.Counter -- [--async-storage]");
    return 0;
}

if (args.Length != 0 && args is not ["--async-storage"])
{
    Console.Error.WriteLine("Expected no arguments or --async-storage. Use --help for usage.");
    return 2;
}

bool asyncStorage = args.Length != 0;
Console.WriteLine(asyncStorage ? "Mode: asynchronous storage writes" : "Mode: Ready / Advance");
var cluster = new CounterCluster(asyncStorage);

// Persist and apply bootstrap entries before manually starting an election.
cluster.Drain();
CounterReplica leader = cluster.Replicas[0];
leader.Node.Campaign();
cluster.Drain();
BasicStatus elected = leader.Node.GetBasicStatus();
if (elected.Role != RaftRole.Leader
    || cluster.Replicas.Any(replica =>
        replica.Node.GetBasicStatus().LeaderId != leader.Id
        || replica.Applied != elected.Commit))
{
    throw new InvalidOperationException("The cluster did not finish its initial election.");
}

Console.WriteLine($"Elected node {leader.Id} in term {elected.Term}.");
ulong commandIndex = elected.Commit + 1; // The cluster is idle with no uncommitted tail.
leader.Node.Propose(CounterReplica.IncrementCommand.Span);
if (leader.Node.GetBasicStatus().Commit != elected.Commit || leader.Count != 0)
{
    throw new InvalidOperationException("The command must not commit or apply on submission alone.");
}

Console.WriteLine($"Proposed increment: commit={elected.Commit}, counter={leader.Count}.");

if (asyncStorage)
{
    // Keep the network and append workers running, but deliberately delay application.
    cluster.Drain(runApplyWorkers: false);
    ulong commit = leader.Node.GetBasicStatus().Commit;
    if (commit != commandIndex
        || cluster.Replicas.Any(replica => replica.Applied != elected.Commit || replica.Count != 0))
    {
        throw new InvalidOperationException("Raft must commit before the paused apply workers change the counters.");
    }

    Console.WriteLine($"Committed at index {commit}: counter={leader.Count} (apply workers paused).");
}
else
{
    Console.WriteLine("Delivering protocol messages and processing Ready batches.");
}

cluster.Drain();
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

return 0;
