using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

using DotnetRaft.Protocol;

namespace DotnetRaft.Tests.Interaction;

internal sealed record DifferentialSchedule(
    string Name, int NodeCount, ulong[] Voters, DifferentialAction[] Actions, DifferentialWitness[] Witnesses);

internal sealed record DifferentialAction(
    string Kind, int Node = 0, ulong Index = 0, string? Text = null, bool PendingWorkAllowed = false);

internal sealed record DifferentialWitness(string Label, DifferentialNodeWitness[] Nodes, DifferentialReadWitness[] Reads);

internal sealed record DifferentialNodeWitness(
    ulong Id, ulong? Term, ulong? Leader, ulong? Commit, ulong? PhysicalApplied,
    ulong? FirstIndex, ulong? LastIndex, ulong[]? Learners, string? Application);

internal sealed record DifferentialReadWitness(ulong Node, ulong Index, string Context);

internal sealed record DifferentialCheckpoint(string Label, DifferentialNode[] Nodes, DifferentialRead[] Reads);

internal sealed record DifferentialNode(
    ulong Id, ulong Term, ulong Vote, ulong Leader, ulong Commit, ulong Applied, ulong PhysicalApplied,
    ulong FirstIndex, ulong LastIndex, DifferentialMembership Membership, string Application, DifferentialEntry[] Entries);

internal sealed record DifferentialMembership(
    ulong[] Voters, ulong[] VotersOutgoing, ulong[] Learners, ulong[] LearnersNext, bool AutoLeave);

internal sealed record DifferentialEntry(ulong Index, ulong Term, EntryType Type, string Data);

internal sealed record DifferentialRead(ulong Node, ulong Index, string Context);

internal static class RaftDifferential
{
    internal static JsonSerializerOptions Json { get; } = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
    };

    internal static DifferentialSchedule ReadSchedule(string path)
    {
        var schedule = JsonSerializer.Deserialize<DifferentialSchedule>(File.ReadAllText(path), Json)
            ?? throw new InvalidDataException($"Missing schedule in {path}.");
        if (schedule.Actions is not { Length: > 0 } || schedule.Actions[^1].Kind != "checkpoint"
            || schedule.Witnesses is not { Length: > 0 })
        {
            throw new InvalidDataException($"{path}: a schedule must end at an observed checkpoint and have witnesses.");
        }

        return schedule;
    }

    internal static bool IsAsyncOnly(string path) => Path.GetFileName(path).StartsWith("async-", StringComparison.Ordinal);

    internal static DifferentialCheckpoint[] Run(DifferentialSchedule schedule, bool asyncStorage)
    {
        List<DifferentialEntry>[] entries = Enumerable.Range(0, schedule.NodeCount)
            .Select(_ => new List<DifferentialEntry>()).ToArray();
        var reads = new List<DifferentialRead>();
        var checkpoints = new List<DifferentialCheckpoint>();
        var environment = new InteractionEnvironment
        {
            EntryApplied = (node, entry) => entries[checked((int)node - 1)].Add(
                new DifferentialEntry(entry.Index, entry.Term, entry.Type, Convert.ToBase64String(entry.Data.Span))),
            ReadStateDelivered = (node, read) => reads.Add(
                new DifferentialRead(node, read.Index, Convert.ToBase64String(read.RequestContext.Span))),
        };
        _ = environment.Handle("log-level none", string.Empty);
        environment.AddNodes(schedule.NodeCount, new InteractionNodeOptions
        {
            Voters = schedule.Voters,
            SnapshotIndex = 2,
            AsyncStorageWrites = asyncStorage,
            MaxInflightMessages = int.MaxValue,
        });

        for (var step = 0; step < schedule.Actions.Length; step++)
        {
            DifferentialAction action = schedule.Actions[step];
            try
            {
                if (action.Kind == "checkpoint")
                {
                    checkpoints.Add(Observe(environment, RequiredText(action), entries, reads, action.PendingWorkAllowed));
                    reads.Clear();
                    foreach (List<DifferentialEntry> applied in entries)
                    {
                        applied.Clear();
                    }
                }
                else
                {
                    Act(environment, action);
                }
            }
            catch (Exception exception)
            {
                throw new InvalidOperationException($"{schedule.Name}/{(asyncStorage ? "async" : "sync")}: " +
                    $"action {step + 1} {action} failed.", exception);
            }
        }

        return [.. checkpoints];
    }

    internal static void VerifyWitnesses(DifferentialSchedule schedule, DifferentialCheckpoint[] checkpoints)
    {
        if (!schedule.Witnesses.Select(witness => witness.Label).SequenceEqual(checkpoints.Select(point => point.Label)))
        {
            throw new InvalidDataException($"{schedule.Name}: checkpoint labels and witnesses do not match.");
        }

        foreach ((DifferentialWitness witness, DifferentialCheckpoint actual) in schedule.Witnesses.Zip(checkpoints))
        {
            foreach (DifferentialNodeWitness expected in witness.Nodes)
            {
                DifferentialNode node = actual.Nodes.Single(node => node.Id == expected.Id);
                string where = $"{schedule.Name}/{actual.Label}/node-{node.Id}";
                Check(expected.Term, node.Term, where, nameof(node.Term));
                Check(expected.Leader, node.Leader, where, nameof(node.Leader));
                Check(expected.Commit, node.Commit, where, nameof(node.Commit));
                Check(expected.PhysicalApplied, node.PhysicalApplied, where, nameof(node.PhysicalApplied));
                Check(expected.FirstIndex, node.FirstIndex, where, nameof(node.FirstIndex));
                Check(expected.LastIndex, node.LastIndex, where, nameof(node.LastIndex));
                if (expected.Application is { } value && Encode(value) != node.Application)
                {
                    throw new InvalidDataException($"{where}: unexpected application bytes ({node.Application}), expected '{value}'.");
                }

                if (expected.Learners is { } learners && !learners.SequenceEqual(node.Membership.Learners))
                {
                    throw new InvalidDataException($"{where}: expected learners [{string.Join(',', learners)}], " +
                        $"got [{string.Join(',', node.Membership.Learners)}].");
                }
            }

            DifferentialRead[] expectedReads = [.. witness.Reads.Select(read => new DifferentialRead(read.Node, read.Index, Encode(read.Context)))];
            if (!OrderReads(expectedReads).SequenceEqual(actual.Reads))
            {
                throw new InvalidDataException($"{schedule.Name}/{actual.Label}: expected reads [{string.Join(", ", expectedReads.AsEnumerable())}], " +
                    $"got [{string.Join(", ", actual.Reads.AsEnumerable())}].");
            }
        }
    }

    internal static void Compare(string name, string mode, string referenceJson, DifferentialCheckpoint[] observed)
    {
        using var reference = JsonDocument.Parse(referenceJson);
        JsonElement root = reference.RootElement;
        if (root.ValueKind != JsonValueKind.Array || root.GetArrayLength() != observed.Length)
        {
            throw new InvalidDataException($"{name}/{mode}: reference and C# checkpoint counts differ.");
        }

        foreach ((JsonElement upstream, DifferentialCheckpoint candidate) in root.EnumerateArray().Zip(observed))
        {
            JsonElement actual = JsonSerializer.SerializeToElement(candidate, Json);
            if (!JsonElement.DeepEquals(upstream, actual))
            {
                var pretty = new JsonSerializerOptions(Json) { WriteIndented = true };
                throw new InvalidDataException($"{name}/{mode}/{candidate.Label}: semantic disagreement\n" +
                    $"Go: {JsonSerializer.Serialize(upstream, pretty)}\nC#: {JsonSerializer.Serialize(actual, pretty)}");
            }
        }
    }

    private static void Check(ulong? expected, ulong actual, string where, string field)
    {
        if (expected.HasValue && expected.Value != actual)
        {
            throw new InvalidDataException($"{where}: expected {field}={expected.Value}, got {actual}.");
        }
    }

    private static void Act(InteractionEnvironment environment, DifferentialAction action)
    {
        int index = action.Node - 1;
        switch (action.Kind)
        {
            case "campaign":
                environment.Campaign(index);
                break;
            case "heartbeat":
                environment.TickHeartbeat(index);
                break;
            case "propose":
                environment.Propose(index, Encoding.UTF8.GetBytes(RequiredText(action)));
                break;
            case "read":
                environment.Nodes[index].RawNode.ReadIndex(Encoding.UTF8.GetBytes(RequiredText(action)));
                break;
            case "add-learner":
                environment.ProposeConfiguration(index, new DotnetRaft.Protocol.ConfChange
                {
                    Type = ConfChangeType.ConfChangeAddLearnerNode,
                    NodeId = action.Index,
                });
                break;
            case "cut":
                ulong nodeId = checked((ulong)action.Node);
                environment.NetworkLinkIsOpen = message => message.From != nodeId && message.To != nodeId;
                break;
            case "heal":
                environment.NetworkLinkIsOpen = null;
                break;
            case "compact":
                environment.Compact(index, action.Index);
                break;
            case "drain":
                environment.Stabilize();
                break;
            case "ready":
                if (!environment.Nodes[index].RawNode.HasReady())
                {
                    throw new InvalidOperationException($"Node {action.Node} has no Ready to process.");
                }

                environment.ProcessReady(index);
                break;
            case "append-worker":
                if (environment.Nodes[index].AppendWorkCount == 0)
                {
                    throw new InvalidOperationException($"Node {action.Node} has no append work.");
                }

                environment.ProcessAppendThread(index);
                break;
            case "deliver":
                if (environment.DeliverMessages(type: null, new InteractionRecipient((ulong)action.Node)) == 0)
                {
                    throw new InvalidOperationException($"No messages to deliver to node {action.Node}.");
                }

                break;
            default:
                throw new InvalidDataException($"Unknown differential action '{action.Kind}'.");
        }
    }

    private static DifferentialCheckpoint Observe(InteractionEnvironment environment, string label,
        List<DifferentialEntry>[] entries, List<DifferentialRead> reads, bool pendingWorkAllowed)
    {
        if (!pendingWorkAllowed && (environment.QueuedMessages.Count != 0 || environment.Nodes.Any(node =>
                node.AppendWorkCount != 0 || node.ApplyWorkCount != 0 || node.RawNode.HasReady())))
        {
            throw new InvalidOperationException($"Checkpoint '{label}' must follow a full drain.");
        }

        return new DifferentialCheckpoint(label,
            [.. environment.Nodes.Select((node, index) => ObserveNode(node, entries[index]))],
            [.. OrderReads(reads)]);
    }

    private static DifferentialNode ObserveNode(InteractionNode node, List<DifferentialEntry> entries)
    {
        BasicStatus status = node.RawNode.GetBasicStatus();
        Snapshot application = node.GetApplicationSnapshot();
        ConfState configuration = application.Metadata.ConfState;
        var membership = new DifferentialMembership(
            [.. configuration.Voters.Order()], [.. configuration.VotersOutgoing.Order()],
            [.. configuration.Learners.Order()], [.. configuration.LearnersNext.Order()], configuration.AutoLeave);
        return new DifferentialNode(status.Id, status.Term, status.Vote, status.LeaderId, status.Commit, status.Applied,
            application.Metadata.Index, node.Storage.GetFirstIndex(), node.Storage.GetLastIndex(), membership,
            Convert.ToBase64String(application.Data.Span), [.. entries]);
    }

    private static IOrderedEnumerable<DifferentialRead> OrderReads(IEnumerable<DifferentialRead> reads) =>
        reads.OrderBy(read => read.Node).ThenBy(read => read.Context, StringComparer.Ordinal).ThenBy(read => read.Index);

    private static string Encode(string text) => Convert.ToBase64String(Encoding.UTF8.GetBytes(text));

    private static string RequiredText(DifferentialAction action) => action.Text
        ?? throw new InvalidDataException($"'{action.Kind}' requires text.");
}
