using DotnetRaft.Core;
using DotnetRaft.Protocol;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreAvailabilityInteractionTests
{
    [Fact]
    public void ForgetLeaderLetsPreCandidatesReplaceActiveLeader()
    {
        var network = new AvailabilityNetwork(
            CreateNode(1, term: 0, checkQuorum: true),
            CreateNode(2, term: 0, checkQuorum: true),
            CreateNode(3, term: 0, checkQuorum: true));
        network.Deliver(Hup(1));
        Assert.Equal(RaftRole.Leader, network[1].Role);
        Assert.Equal(1UL, network[2].LeaderId);
        Assert.Equal(1UL, network[3].LeaderId);

        network[2].Step(new Message
        {
            Type = MessageType.MsgForgetLeader,
        });
        network.Deliver(Hup(3));

        Assert.Equal(RaftRole.Follower, network[1].Role);
        Assert.Equal(RaftRole.Follower, network[2].Role);
        Assert.Equal(RaftRole.Leader, network[3].Role);
        Assert.Equal(2UL, network[3].Term);
    }

    [Fact]
    public void MixedPreVoteMigrationCanCompleteLaterElection()
    {
        var network = new AvailabilityNetwork(
            CreateNode(
                1,
                term: 1,
                voters: [1, 2],
                checkQuorum: false,
                preVote: true),
            CreateNode(
                2,
                term: 3,
                voters: [1, 2],
                checkQuorum: false,
                preVote: false));

        network.Deliver(Hup(1));

        Assert.Equal(RaftRole.Follower, network[1].Role);
        Assert.Equal(3UL, network[1].Term);

        network.Deliver(Hup(1));

        Assert.Equal(RaftRole.Leader, network[1].Role);
        Assert.Equal(4UL, network[1].Term);
        Assert.Equal(RaftRole.Follower, network[2].Role);
    }

    [Fact]
    public void LowerTermHeartbeatFreesStuckPreCandidate()
    {
        RaftCore leader = CreateNode(
            1,
            term: 1,
            checkQuorum: false);
        leader.BecomeCandidate();
        leader.BecomeLeader();
        leader.TakeMessages();
        leader.TakeMessagesAfterAppend();

        RaftCore follower = CreateNode(
            2,
            term: 2,
            checkQuorum: false);
        follower.BecomeFollower(2, leaderId: 1);

        RaftCore stuck = CreateNode(
            3,
            term: 3,
            checkQuorum: false);
        stuck.Step(Hup(3));
        stuck.TakeMessages();
        stuck.Step(Assert.Single(
            stuck.TakeMessagesAfterAppend()));
        Assert.Equal(RaftRole.PreCandidate, stuck.Role);
        var network = new AvailabilityNetwork(
            leader,
            follower,
            stuck);

        network.Deliver(new Message
        {
            From = 1,
            To = 3,
            Term = 2,
            Type = MessageType.MsgHeartbeat,
        });

        Assert.Equal(RaftRole.Follower, leader.Role);
        Assert.Equal(3UL, leader.Term);

        network.Deliver(Hup(3));

        Assert.Equal(RaftRole.Leader, stuck.Role);
        Assert.Equal(4UL, stuck.Term);
    }

    private static RaftCore CreateNode(
        ulong id,
        ulong term,
        IEnumerable<ulong>? voters = null,
        bool checkQuorum = true,
        bool preVote = true)
    {
        return Create(
            id: id,
            voters: voters ?? [1UL, 2UL, 3UL],
            term: term,
            checkQuorum: checkQuorum,
            preVote: preVote).Core;
    }

    private sealed class AvailabilityNetwork
    {
        private readonly Dictionary<ulong, RaftCore> nodes;
        private readonly Queue<Message> messages = new();

        internal AvailabilityNetwork(
            params RaftCore[] nodes)
        {
            this.nodes = nodes.ToDictionary(
                node => node.Id);
        }

        internal RaftCore this[ulong id] => nodes[id];

        internal void Deliver(Message message)
        {
            messages.Enqueue(message.Clone());
            var delivered = 0;
            while (messages.TryDequeue(
                       out Message? next))
            {
                delivered++;
                if (delivered > 1000)
                {
                    throw new InvalidOperationException(
                        "Availability network did not quiesce.");
                }

                if (!nodes.TryGetValue(
                        next.To,
                        out RaftCore? target))
                {
                    throw new InvalidOperationException(
                        $"Unknown target {next.To}.");
                }

                target.Step(next);
                foreach (Message afterAppend in
                         target.TakeMessagesAfterAppend())
                {
                    messages.Enqueue(afterAppend);
                }

                foreach (Message outbound in
                         target.TakeMessages())
                {
                    messages.Enqueue(outbound);
                }
            }
        }
    }
}
