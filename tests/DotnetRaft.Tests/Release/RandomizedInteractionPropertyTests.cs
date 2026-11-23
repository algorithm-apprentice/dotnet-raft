using System.Buffers.Binary;
using System.Globalization;
using System.Text;

using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Tests.Interaction;

using static DotnetRaft.Tests.RawNode.RawNodeTestSupport;

using ProtocolMessage = DotnetRaft.Protocol.Message;
using TrackerProgress = DotnetRaft.Tracker.Progress;

namespace DotnetRaft.Tests.Release;

public sealed class RandomizedInteractionPropertyTests
{
    public static TheoryData<bool, ulong> PairedSeeds
    {
        get
        {
            var seeds = new TheoryData<bool, ulong>();
            for (ulong seed = 0; seed < 32; seed++)
            {
                seeds.Add(false, seed);
                seeds.Add(true, seed);
            }

            return seeds;
        }
    }

    [Theory]
    [MemberData(nameof(PairedSeeds))]
    public void FixedSeedClusterMaintainsInvariants(
        bool asyncStorageWrites,
        ulong seed)
    {
        InteractionEnvironment environment = CreateCluster(asyncStorageWrites, seed, out _);
        var random = new XorShift64Star(seed);
        var actions = new StringBuilder();

        for (var actionNumber = 0;
             actionNumber < 200;
             actionNumber++)
        {
            ulong value = random.Next();
            actions.Append(CultureInfo.InvariantCulture, $"{actionNumber} 0x{value:x16} {ActionName((byte)value)}: ");
            try
            {
                Act(
                    environment,
                    asyncStorageWrites,
                    seed,
                    actionNumber,
                    value,
                    actions);
                AssertInvariants(
                    environment,
                    asyncStorageWrites);
            }
            catch (Exception exception)
            {
                throw new InvalidOperationException(
                    DescribeFailure(
                        environment,
                        asyncStorageWrites,
                        seed,
                        actionNumber,
                        value,
                        actions),
                    exception);
            }
        }
    }

    [Theory]
    [InlineData(false, 1UL)]
    [InlineData(true, 1UL)]
    [InlineData(false, 8UL)]
    [InlineData(true, 8UL)]
    [InlineData(false, 23UL)]
    [InlineData(true, 23UL)]
    public void SeededRecoveryAppliesBusinessWritesBeforeDuringAndAfterAnOutage(bool asyncStorageWrites, ulong seed)
    {
        InteractionEnvironment cluster = CreateCluster(asyncStorageWrites, seed, out var oracle, availabilityFeatures: false);
        var random = new XorShift64Star(seed);
        int leader = (int)(random.Next() % 3);
        int survivor = (leader + 1 + (int)(random.Next() % 2)) % 3;
        ulong leaderId = (ulong)leader + 1;
        string prefix = string.Create(CultureInfo.InvariantCulture, $"seed-{seed}/");

        cluster.Campaign(leader);
        cluster.Stabilize();
        cluster.Propose(leader, Encoding.UTF8.GetBytes(prefix));
        cluster.Stabilize();
        Assert.All(cluster.Nodes, node => Assert.Equal(prefix, node.GetApplicationSnapshot().Data.ToStringUtf8()));
        ulong physical = oracle.AppliedThrough(leaderId);

        // Persist and commit a write, but do not run the old leader's apply
        // work. The survivors may finish their own work before the crash.
        cluster.Propose(leader, "replay/"u8);
        cluster.ProcessReady(leader);
        if (asyncStorageWrites)
        {
            cluster.ProcessAppendThread(leader);
        }

        cluster.Stabilize(Enumerable.Range(0, 3).Where(index => index != leader).ToArray());
        cluster.DeliverMessages(type: null, new InteractionRecipient(leaderId));
        Assert.Equal(physical + 1, cluster.Nodes[leader].RawNode.GetBasicStatus().Commit);
        InteractionNode previous = cluster.Nodes[leader];
        if (asyncStorageWrites)
        {
            cluster.ProcessReady(leader);
            Assert.True(previous.ApplyWorkCount > 0);
            cluster.ProcessAppendThread(leader);
        }
        else
        {
            Ready ready = previous.RawNode.Ready();
            Assert.Equal(physical + 1, Assert.Single(ready.CommittedEntries).Index);
            Persist(previous.Storage, ready);
        }

        Assert.Equal(physical + 1, previous.Storage.GetInitialState().HardState?.Commit);
        Assert.Equal(physical, oracle.AppliedThrough(leaderId));
        cluster.CrashNode(leader);
        cluster.Campaign(survivor);
        cluster.Stabilize();
        cluster.Propose(survivor, "during/"u8);
        cluster.Stabilize();
        Assert.Equal(physical, oracle.AppliedThrough(leaderId));
        Assert.Equal(prefix + "replay/during/", cluster.Nodes[survivor].GetApplicationSnapshot().Data.ToStringUtf8());

        cluster.RestartNode(leader);
        cluster.Stabilize(leader);
        Assert.Equal(physical + 1, oracle.AppliedThrough(leaderId));
        Assert.Equal(prefix + "replay/", cluster.Nodes[leader].GetApplicationSnapshot().Data.ToStringUtf8());
        cluster.TickHeartbeat(survivor);
        cluster.Stabilize();
        // Submit through the recovered replica, not just the current leader.
        cluster.Propose(leader, "after/"u8);
        cluster.Stabilize();
        Assert.All(cluster.Nodes, node => Assert.Equal(prefix + "replay/during/after/", node.GetApplicationSnapshot().Data.ToStringUtf8()));
    }

    private static InteractionEnvironment CreateCluster(bool asyncStorageWrites, ulong seed, out AppliedLogOracle oracle,
        bool availabilityFeatures = true)
    {
        const ulong initialIndex = 10;
        oracle = new AppliedLogOracle(initialIndex);
        var environment = new InteractionEnvironment { EntryApplied = oracle.Record, TraceSink = new ElectionSafetyOracle() };
        environment.Handle("log-level none", string.Empty);
        environment.AddNodes(3, new InteractionNodeOptions
        {
            Voters = [1, 2, 3],
            SnapshotIndex = initialIndex,
            ElectionTick = 3,
            HeartbeatTick = 1,
            MaxInflightMessages = 4,
            MaxCommittedSizePerReady = 256,
            PreVote = availabilityFeatures && (seed & 1) != 0,
            CheckQuorum = availabilityFeatures && (seed & 2) != 0,
            AsyncStorageWrites = asyncStorageWrites,
        });
        return environment;
    }

    private static void Act(
        InteractionEnvironment environment,
        bool asyncStorageWrites,
        ulong seed,
        int actionNumber,
        ulong value,
        StringBuilder actions)
    {
        int action = (byte)value;
        int nodeIndex = (int)((value >> 8) % 3);
        ulong peerId =
            1 + (ulong)(
                (nodeIndex
                 + 1
                 + (int)((value >> 16) & 1))
                % 3);
        switch (action)
        {
            case <= 15:
                actions.AppendLine(CultureInfo.InvariantCulture, $"node={nodeIndex + 1}");
                environment.Campaign(nodeIndex);
                break;
            case <= 63:
                actions.AppendLine(CultureInfo.InvariantCulture, $"node={nodeIndex + 1}");
                environment.Nodes[nodeIndex]
                    .RawNode.Tick();
                break;
            case <= 95:
                InteractionNode[] leaders =
                [
                    .. environment.Nodes.Where(
                        node =>
                            node.RawNode
                                .GetBasicStatus()
                                .Role
                            == RaftRole.Leader),
                ];
                if (leaders.Length == 1)
                {
                    Span<byte> data = stackalloc byte[16];
                    BinaryPrimitives
                        .WriteUInt64LittleEndian(
                            data,
                            seed);
                    BinaryPrimitives
                        .WriteUInt64LittleEndian(
                            data[8..],
                            (ulong)actionNumber);
                    try
                    {
                        actions.Append(CultureInfo.InvariantCulture, $"leader={leaders[0].Config.Id} proposal={seed}/{actionNumber}");
                        leaders[0].RawNode.Propose(data);
                        actions.AppendLine(" submitted");
                    }
                    catch (ProposalDroppedException)
                    {
                        actions.AppendLine(" dropped by node");
                    }
                }
                else
                {
                    actions.AppendLine(CultureInfo.InvariantCulture, $"skipped ({leaders.Length} leaders)");
                }

                break;
            case <= 143:
                bool ready = environment.Nodes[nodeIndex].RawNode.HasReady();
                actions.AppendLine(CultureInfo.InvariantCulture, $"node={nodeIndex + 1} {(ready ? "process" : "skipped (no Ready)")}");
                if (ready)
                {
                    environment.ProcessReady(nodeIndex);
                }

                break;
            case <= 191:
                HandleMessages(environment, value, actions);
                break;
            case <= 207:
                actions.AppendLine(CultureInfo.InvariantCulture, $"node={nodeIndex + 1} peer={peerId}");
                environment.ReportUnreachable(
                    nodeIndex,
                    peerId);
                break;
            case <= 223:
                bool hasAppendWork = asyncStorageWrites && environment.Nodes[nodeIndex].AppendWorkCount > 0;
                actions.AppendLine(CultureInfo.InvariantCulture, $"node={nodeIndex + 1} {(hasAppendWork ? "process" : "skipped (no append work)")}");
                if (hasAppendWork)
                {
                    environment.ProcessAppendThread(
                        nodeIndex);
                }

                break;
            case <= 239:
                bool hasApplyWork = asyncStorageWrites && environment.Nodes[nodeIndex].ApplyWorkCount > 0;
                actions.AppendLine(CultureInfo.InvariantCulture, $"node={nodeIndex + 1} {(hasApplyWork ? "process" : "skipped (no apply work)")}");
                if (hasApplyWork)
                {
                    environment.ProcessApplyThread(
                        nodeIndex);
                }

                break;
            case <= 247:
                actions.AppendLine(CultureInfo.InvariantCulture, $"node={nodeIndex + 1}");
                environment.CrashNode(nodeIndex);
                environment.RestartNode(nodeIndex);
                break;
            default:
                if (((value >> 40) & 1) == 0)
                {
                    actions.AppendLine("all nodes");
                    environment.Stabilize();
                }
                else
                {
                    actions.AppendLine(CultureInfo.InvariantCulture, $"node={nodeIndex + 1}");
                    environment.Stabilize(nodeIndex);
                }

                break;
        }
    }

    private static void HandleMessages(
        InteractionEnvironment environment,
        ulong value,
        StringBuilder actions)
    {
        IReadOnlyList<ProtocolMessage> queued =
            environment.QueuedMessages;
        if (queued.Count == 0)
        {
            actions.AppendLine("skipped (empty queue)");
            return;
        }

        int index = (int)((value >> 24) % (ulong)queued.Count);
        ProtocolMessage selected = queued[index];
        bool local =
            RaftLocalMessageTargets.IsLocal(
                selected.From)
            || RaftLocalMessageTargets.IsLocal(
                selected.To)
            || selected.From == selected.To;
        if (local)
        {
            actions.AppendLine(CultureInfo.InvariantCulture, $"deliver local {selected.Type} to node={selected.To}");
            environment.DeliverMessages(selected.Type, new InteractionRecipient(selected.To));
        }
        else
        {
            bool drop = ((value >> 32) & 3) == 1;
            bool duplicate = ((value >> 32) & 3) == 2;
            string disposition = drop ? "drop" : duplicate ? "duplicate" : "deliver";
            actions.AppendLine(CultureInfo.InvariantCulture,
                $"slot={index} {selected.From}->{selected.To} {selected.Type} term={selected.Term} {disposition}");
            environment.DeliverNetworkMessage(
                index,
                drop,
                duplicate);
        }
    }

    private static void AssertInvariants(
        InteractionEnvironment environment,
        bool asyncStorageWrites)
    {
        foreach (InteractionNode node in
                 environment.Nodes)
        {
            RaftLog log = node.RawNode.Core.Log;
            Assert.True(log.Applied <= log.Applying);
            Assert.True(log.Applying <= log.Committed);
            Assert.True(log.Committed <= log.LastIndex);
            Assert.True(
                log.Unstable.Offset
                <= log.Unstable.OffsetInProgress);
            Assert.True(
                log.Unstable.OffsetInProgress
                <= log.LastIndex + 1);
            ulong physical =
                node.GetApplicationSnapshot()
                    .Metadata.Index;
            if (asyncStorageWrites)
            {
                Assert.True(log.Applied <= physical);
                Assert.True(physical <= log.Committed);
            }
            else
            {
                Assert.True(physical <= log.Applied);
            }

            foreach (TrackerProgress progress in
                     node.RawNode.Core.Tracker.Progress
                         .Values)
            {
                Assert.True(
                    progress.Next > progress.Match);
            }
        }
    }

    private static string DescribeFailure(
        InteractionEnvironment environment,
        bool asyncStorageWrites,
        ulong seed,
        int action,
        ulong value,
        StringBuilder actions)
    {
        var builder = new StringBuilder();
        builder.Append("mode=")
            .Append(
                asyncStorageWrites
                    ? "async"
                    : "sync")
            .Append(" seed=")
            .Append(seed)
            .Append(" action=")
            .Append(action)
            .Append(" value=0x")
            .Append(
                value.ToString(
                    "x16",
                    CultureInfo.InvariantCulture))
            .Append(" kind=")
            .Append(ActionName((byte)value))
            .AppendLine();
        foreach (InteractionNode node in
                 environment.Nodes)
        {
            BasicStatus status =
                node.RawNode.GetBasicStatus();
            RaftLog log = node.RawNode.Core.Log;
            builder.Append(status.Id)
                .Append(':')
                .Append(status.Role)
                .Append(" term=")
                .Append(status.Term)
                .Append(" lead=")
                .Append(status.LeaderId)
                .Append(" applied=")
                .Append(log.Applied)
                .Append(" applying=")
                .Append(log.Applying)
                .Append(" committed=")
                .Append(log.Committed)
                .Append(" last=")
                .Append(log.LastIndex)
                .Append(" appendWork=")
                .Append(node.AppendWorkCount)
                .Append(" applyWork=")
                .Append(node.ApplyWorkCount)
                .AppendLine();
        }

        builder.Append("queued=")
            .AppendJoin(
                ',',
                environment.QueuedMessages.Select(
                    message =>
                        $"{message.From}->{message.To}/{message.Type}"));
        return builder.AppendLine().AppendLine("Executed actions:").Append(actions).ToString();
    }

    private static string ActionName(int action)
    {
        return action switch
        {
            <= 15 => "campaign",
            <= 63 => "tick",
            <= 95 => "propose",
            <= 143 => "ready",
            <= 191 => "message",
            <= 207 => "unreachable",
            <= 223 => "append-worker",
            <= 239 => "apply-worker",
            <= 247 => "crash-restart",
            _ => "stabilize",
        };
    }

    private struct XorShift64Star
    {
        private ulong _state;

        internal XorShift64Star(ulong seed)
        {
            _state = seed == 0
                ? 0x9e3779b97f4a7c15UL
                : seed;
        }

        internal ulong Next()
        {
            ulong value = _state;
            value ^= value >> 12;
            value ^= value << 25;
            value ^= value >> 27;
            _state = value;
            return value
                * 2685821657736338717UL;
        }
    }

    private sealed class ElectionSafetyOracle : IRaftTraceSink
    {
        private readonly Dictionary<ulong, ulong> leadersByTerm = [];

        public void Trace(RaftTraceEvent traceEvent)
        {
            if (traceEvent.Type == RaftTraceEventType.BecameLeader)
            {
                BasicStatus status = traceEvent.Status;
                Assert.True(leadersByTerm.TryAdd(status.Term, status.Id) || leadersByTerm[status.Term] == status.Id,
                    $"Term {status.Term}: node {status.Id} became leader after node {leadersByTerm[status.Term]}.");
            }
        }
    }
}
