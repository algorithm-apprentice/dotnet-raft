using System.Buffers.Binary;
using System.Text;

using DotnetRaft.Core;
using DotnetRaft.Tests.Interaction;

using ProtocolMessage = DotnetRaft.Protocol.Message;
using TrackerProgress = DotnetRaft.Tracker.Progress;

namespace DotnetRaft.Tests.Release;

public sealed class RandomizedInteractionPropertyTests
{
    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 1)]
    [InlineData(false, 2)]
    [InlineData(false, 3)]
    [InlineData(false, 4)]
    [InlineData(false, 5)]
    [InlineData(false, 6)]
    [InlineData(false, 7)]
    [InlineData(false, 8)]
    [InlineData(false, 9)]
    [InlineData(false, 10)]
    [InlineData(false, 11)]
    [InlineData(false, 12)]
    [InlineData(false, 13)]
    [InlineData(false, 14)]
    [InlineData(false, 15)]
    [InlineData(true, 16)]
    [InlineData(true, 17)]
    [InlineData(true, 18)]
    [InlineData(true, 19)]
    [InlineData(true, 20)]
    [InlineData(true, 21)]
    [InlineData(true, 22)]
    [InlineData(true, 23)]
    [InlineData(true, 24)]
    [InlineData(true, 25)]
    [InlineData(true, 26)]
    [InlineData(true, 27)]
    [InlineData(true, 28)]
    [InlineData(true, 29)]
    [InlineData(true, 30)]
    [InlineData(true, 31)]
    public void FixedSeedClusterMaintainsInvariants(
        bool asyncStorageWrites,
        ulong seed)
    {
        var environment = new InteractionEnvironment();
        environment.Handle(
            "log-level none",
            string.Empty);
        environment.AddNodes(
            3,
            new InteractionNodeOptions
            {
                Voters = [1, 2, 3],
                SnapshotIndex = 10,
                ElectionTick = 3,
                HeartbeatTick = 1,
                MaxInflightMessages = 4,
                MaxCommittedSizePerReady = 256,
                PreVote = (seed & 1) != 0,
                CheckQuorum = (seed & 2) != 0,
                AsyncStorageWrites =
                    asyncStorageWrites,
            });
        var random = new XorShift64Star(seed);

        for (var actionNumber = 0;
             actionNumber < 200;
             actionNumber++)
        {
            ulong value = random.Next();
            try
            {
                Act(
                    environment,
                    asyncStorageWrites,
                    seed,
                    actionNumber,
                    value);
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
                        value),
                    exception);
            }
        }
    }

    private static void Act(
        InteractionEnvironment environment,
        bool asyncStorageWrites,
        ulong seed,
        int actionNumber,
        ulong value)
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
                environment.Campaign(nodeIndex);
                break;
            case <= 63:
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
                        leaders[0].RawNode.Propose(data);
                    }
                    catch (ProposalDroppedException)
                    {
                    }
                }

                break;
            case <= 143:
                if (environment.Nodes[nodeIndex]
                    .RawNode.HasReady())
                {
                    environment.ProcessReady(nodeIndex);
                }

                break;
            case <= 191:
                HandleMessages(environment, value);
                break;
            case <= 207:
                environment.ReportUnreachable(
                    nodeIndex,
                    peerId);
                break;
            case <= 223:
                if (asyncStorageWrites
                    && environment.Nodes[nodeIndex]
                        .AppendWorkCount > 0)
                {
                    environment.ProcessAppendThread(
                        nodeIndex);
                }

                break;
            case <= 239:
                if (asyncStorageWrites
                    && environment.Nodes[nodeIndex]
                        .ApplyWorkCount > 0)
                {
                    environment.ProcessApplyThread(
                        nodeIndex);
                }

                break;
            default:
                if (((value >> 40) & 1) == 0)
                {
                    environment.Stabilize();
                }
                else
                {
                    environment.Stabilize(nodeIndex);
                }

                break;
        }
    }

    private static void HandleMessages(
        InteractionEnvironment environment,
        ulong value)
    {
        IReadOnlyList<ProtocolMessage> queued =
            environment.QueuedMessages;
        if (queued.Count == 0)
        {
            return;
        }

        ProtocolMessage selected =
            queued[(int)((value >> 24)
                         % (ulong)queued.Count)];
        bool local =
            RaftLocalMessageTargets.IsLocal(
                selected.From)
            || RaftLocalMessageTargets.IsLocal(
                selected.To)
            || selected.From == selected.To;
        bool drop = !local
            && ((value >> 32) & 1) != 0;
        environment.DeliverMessages(
            selected.Type,
            new InteractionRecipient(
                selected.To,
                drop));
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

        foreach (IGrouping<ulong, InteractionNode> leaders
                 in environment.Nodes
                     .Where(node =>
                         node.RawNode.GetBasicStatus()
                             .Role
                         == RaftRole.Leader)
                     .GroupBy(node =>
                         node.RawNode.GetBasicStatus()
                             .Term))
        {
            Assert.Single(leaders);
        }
    }

    private static string DescribeFailure(
        InteractionEnvironment environment,
        bool asyncStorageWrites,
        ulong seed,
        int action,
        ulong value)
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
                    System.Globalization.CultureInfo.InvariantCulture))
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
        return builder.ToString();
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
}
