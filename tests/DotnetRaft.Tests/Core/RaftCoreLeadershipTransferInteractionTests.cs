using DotnetRaft.Core;
using DotnetRaft.Protocol;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;
using static DotnetRaft.Tests.Core.RaftCoreLeadershipTransferTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreLeadershipTransferInteractionTests
{
    [Fact]
    public void LeadershipTransfersToCaughtUpVoterAndBack()
    {
        var network = NewNetwork();
        network.Deliver(Hup(1));
        Assert.Equal(RaftRole.Leader, network[1].Role);

        network.Deliver(Transfer(2));

        Assert.Equal(RaftRole.Leader, network[2].Role);
        Assert.Equal(RaftRole.Follower, network[1].Role);
        Assert.Equal(2UL, network[1].LeaderId);

        network.Deliver(Proposal(
            2,
            NormalEntry("after-transfer")));
        network.Deliver(Transfer(
            transferee: 1,
            recipient: 2));

        Assert.Equal(RaftRole.Leader, network[1].Role);
        Assert.Equal(RaftRole.Follower, network[2].Role);
        Assert.Equal(1UL, network[2].LeaderId);
    }

    [Fact]
    public void TransferRequestSentToFollowerReachesLeader()
    {
        var network = NewNetwork();
        network.Deliver(Hup(1));

        network.Deliver(Transfer(
            transferee: 3,
            recipient: 2));

        Assert.Equal(RaftRole.Leader, network[3].Role);
        Assert.Equal(3UL, network[1].LeaderId);
        Assert.Equal(3UL, network[2].LeaderId);
    }

    [Fact]
    public void SlowFollowerCatchesUpBeforeTransfer()
    {
        var network = NewNetwork();
        network.Isolate(3);
        network.Deliver(Hup(1));
        network.Deliver(Proposal(
            1,
            NormalEntry("isolated")));
        Assert.True(
            network[3].Log.LastIndex
            < network[1].Log.LastIndex);

        network.Recover();
        network.Deliver(Transfer(3));
        Assert.Equal(3UL, network[1].LeaderTransferee);

        network.TickLeader(1);

        Assert.Equal(RaftRole.Leader, network[3].Role);
        Assert.Equal(
            network[1].Log.LastIndex,
            network[3].Log.LastIndex);
    }

    [Fact]
    public void IsolatedTransfereeRemainsPendingUntilTimeout()
    {
        var network = NewNetwork();
        network.Deliver(Hup(1));
        network.Isolate(3);

        network.Deliver(Transfer(3));

        Assert.Equal(3UL, network[1].LeaderTransferee);
        for (var tick = 0;
             tick < network[1]
                 .GetClockStateForTesting()
                 .ElectionTick - 1;
             tick++)
        {
            network.TickLeader(1);
        }

        Assert.Equal(3UL, network[1].LeaderTransferee);

        network.TickLeader(1);

        Assert.Equal(RaftRole.Leader, network[1].Role);
        Assert.Equal(0UL, network[1].LeaderTransferee);
    }

    [Fact]
    public void CheckQuorumTransferBypassesPreVoteAndLease()
    {
        var network = NewNetwork(
            checkQuorum: true,
            preVote: true);
        network.Deliver(Hup(1));
        Assert.Equal(RaftRole.Leader, network[1].Role);

        network.Deliver(Transfer(2));

        Assert.Equal(RaftRole.Leader, network[2].Role);
        Assert.Equal(2UL, network[2].Term);
        Assert.Equal(RaftRole.Follower, network[1].Role);
    }

    [Fact]
    public void DelayedStaleTimeoutCanDeposeLeaderWithoutSuccessor()
    {
        var network = NewNetwork();
        network.Deliver(Hup(1));
        network.HoldPredicate = message =>
            message.Type == MessageType.MsgTimeoutNow
            && message.To == 2;

        network.Deliver(Transfer(2));

        Assert.Single(network.HeldMessages);
        Assert.Equal(2UL, network[1].LeaderTransferee);
        for (var tick = 0;
             tick < network[1]
                 .GetClockStateForTesting()
                 .ElectionTick;
             tick++)
        {
            network.TickLeader(1);
        }

        Assert.Equal(0UL, network[1].LeaderTransferee);
        network.DropPredicate = message =>
            message.Type == MessageType.MsgApp
            && message.To == 2;
        network.Deliver(Proposal(
            1,
            NormalEntry("newer")));
        Assert.True(
            network[2].Log.LastIndex
            < network[1].Log.LastIndex);

        network.DropPredicate = null;
        network.HoldPredicate = null;
        network.DeliverHeld();

        Assert.Equal(RaftRole.Follower, network[1].Role);
        Assert.Equal(RaftRole.Follower, network[2].Role);
        Assert.Equal(RaftRole.Follower, network[3].Role);
        Assert.Equal(2UL, network[1].Term);
        Assert.Equal(0UL, network[1].LeaderId);
        Assert.Equal(0UL, network[2].LeaderId);
        Assert.Equal(0UL, network[3].LeaderId);
    }

    private static TransferNetwork NewNetwork(
        bool checkQuorum = false,
        bool preVote = false)
    {
        return new TransferNetwork(
            Create(
                id: 1,
                voters: [1, 2, 3],
                checkQuorum: checkQuorum,
                preVote: preVote).Core,
            Create(
                id: 2,
                voters: [1, 2, 3],
                checkQuorum: checkQuorum,
                preVote: preVote).Core,
            Create(
                id: 3,
                voters: [1, 2, 3],
                checkQuorum: checkQuorum,
                preVote: preVote).Core);
    }

    private sealed class TransferNetwork
    {
        private readonly Dictionary<ulong, RaftCore> nodes;
        private readonly Queue<PendingMessage> messages =
            new();
        private readonly List<PendingMessage> heldMessages =
            [];
        private ulong isolated;

        internal TransferNetwork(
            params RaftCore[] nodes)
        {
            this.nodes = nodes.ToDictionary(
                node => node.Id);
        }

        internal RaftCore this[ulong id] => nodes[id];

        internal Func<Message, bool>? DropPredicate
        {
            get;
            set;
        }

        internal Func<Message, bool>? HoldPredicate
        {
            get;
            set;
        }

        internal IReadOnlyList<Message> HeldMessages =>
            heldMessages
                .Select(pending => pending.Message)
                .ToArray();

        internal void Isolate(ulong id)
        {
            isolated = id;
        }

        internal void Recover()
        {
            isolated = 0;
        }

        internal void Deliver(Message message)
        {
            messages.Enqueue(new PendingMessage(
                message.Clone(),
                message.To,
                BypassNetwork: true));
            Pump();
        }

        internal void DeliverHeld()
        {
            foreach (PendingMessage pending in
                     heldMessages)
            {
                messages.Enqueue(pending);
            }

            heldMessages.Clear();
            Pump();
        }

        internal void TickLeader(ulong id)
        {
            nodes[id].TickLeader();
            EnqueueOutputs(nodes[id]);
            Pump();
        }

        private void Pump()
        {
            var delivered = 0;
            while (messages.TryDequeue(
                       out PendingMessage? pending))
            {
                delivered++;
                if (delivered > 5000)
                {
                    throw new InvalidOperationException(
                        "Transfer network did not quiesce.");
                }

                Message message = pending.Message;
                if (HoldPredicate?.Invoke(message) == true)
                {
                    heldMessages.Add(pending with
                    {
                        Message = message.Clone(),
                    });
                    continue;
                }

                bool crossesNetwork =
                    pending.Sender != message.To;
                if (!pending.BypassNetwork
                    && ((crossesNetwork
                         && isolated != 0
                         && (pending.Sender == isolated
                             || message.To == isolated))
                        || DropPredicate?.Invoke(message)
                        == true))
                {
                    continue;
                }

                RaftCore target = nodes[message.To];
                target.Step(message);
                EnqueueOutputs(target);
            }
        }

        private void EnqueueOutputs(RaftCore core)
        {
            foreach (Message message in
                     core.TakeMessagesAfterAppend())
            {
                messages.Enqueue(new PendingMessage(
                    message,
                    core.Id,
                    BypassNetwork: false));
            }

            foreach (Message message in
                     core.TakeMessages())
            {
                messages.Enqueue(new PendingMessage(
                    message,
                    core.Id,
                    BypassNetwork: false));
            }
        }

        private sealed record PendingMessage(
            Message Message,
            ulong Sender,
            bool BypassNetwork);
    }
}
