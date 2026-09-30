using DotnetRaft.Core;
using DotnetRaft.Protocol;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreReplicationInteractionTests
{
    [Fact]
    public void ThreeNodeElectionAndProposalConvergeDeterministically()
    {
        var network = new DeterministicNetwork(
            Create(voters: [1, 2, 3], id: 1).Core,
            Create(voters: [1, 2, 3], id: 2).Core,
            Create(voters: [1, 2, 3], id: 3).Core);

        network.Deliver(Hup(1));

        Assert.Equal(RaftRole.Leader, network[1].Role);
        Assert.Equal(RaftRole.Follower, network[2].Role);
        Assert.Equal(RaftRole.Follower, network[3].Role);
        Assert.All(
            network.Nodes,
            core => Assert.Equal(1UL, core.Log.Committed));

        network.Deliver(Proposal(1, "command"));

        Assert.All(network.Nodes, core =>
        {
            Assert.Equal(2UL, core.Log.LastIndex);
            Assert.Equal(2UL, core.Log.Committed);
            Entry[] entries = [.. core.Log.GetAllEntries()];
            Assert.Equal([1UL, 2UL], entries.Select(entry => entry.Index));
            Assert.Equal([1UL, 1UL], entries.Select(entry => entry.Term));
            Assert.True(entries[0].Data.IsEmpty);
            Assert.Equal("command", entries[1].Data.ToStringUtf8());
        });
    }

    [Fact]
    public void NaiveOneIndexRetriesRepairDivergentFollowerSuffix()
    {
        RaftCore leader = Create(
            id: 1,
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
                EntryAt(3, 5),
            ],
            term: 5).Core;
        RaftCore follower = Create(
            id: 2,
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 4),
                EntryAt(3, 4),
                EntryAt(4, 4),
            ],
            term: 5).Core;
        var network = new DeterministicNetwork(leader, follower);

        network.Deliver(Hup(1));

        Assert.Equal(RaftRole.Leader, leader.Role);
        Assert.Equal(6UL, leader.Term);
        Assert.Equal(4UL, leader.Log.LastIndex);
        Assert.Equal(4UL, follower.Log.LastIndex);
        Assert.Equal(
            leader.Log.GetAllEntries(),
            follower.Log.GetAllEntries());
        Assert.Equal(4UL, leader.Log.Committed);
        Assert.Equal(4UL, follower.Log.Committed);
        Assert.True(network.RejectedAppendCount >= 2);
    }

    private static Message Proposal(ulong id, string data)
    {
        var message = new Message
        {
            From = id,
            To = id,
            Type = MessageType.MsgProp,
        };
        message.Entries.Add(new Entry
        {
            Data = ByteString.CopyFromUtf8(data),
        });
        return message;
    }

    private sealed class DeterministicNetwork
    {
        private readonly Dictionary<ulong, RaftCore> nodes;
        private readonly Queue<Message> messages = new();

        internal DeterministicNetwork(params RaftCore[] nodes)
        {
            this.nodes = nodes.ToDictionary(core => core.Id);
        }

        internal IEnumerable<RaftCore> Nodes =>
            nodes.Values.OrderBy(core => core.Id);

        internal int RejectedAppendCount { get; private set; }

        internal RaftCore this[ulong id] => nodes[id];

        internal void Deliver(Message message)
        {
            Step(message);
            while (messages.TryDequeue(out Message? next))
            {
                Step(next);
            }
        }

        private void Step(Message message)
        {
            if (message.Type == MessageType.MsgAppResp &&
                message.Reject)
            {
                RejectedAppendCount++;
            }

            RaftCore core = nodes[message.To];
            core.Step(message);
            CompleteReadyBatches(core);
        }

        private void CompleteReadyBatches(RaftCore core)
        {
            while (true)
            {
                Message[] immediate = core.TakeMessages();
                Message[] afterAppend =
                    core.TakeMessagesAfterAppend();
                if (immediate.Length == 0 &&
                    afterAppend.Length == 0)
                {
                    return;
                }

                foreach (Message message in immediate)
                {
                    messages.Enqueue(message);
                }

                foreach (Message message in afterAppend)
                {
                    if (message.To == core.Id)
                    {
                        core.Step(message);
                    }
                    else
                    {
                        messages.Enqueue(message);
                    }
                }
            }
        }
    }
}
