using DotnetRaft.Core;
using DotnetRaft.Protocol;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreCheckQuorumTests
{
    [Fact]
    public void InactiveLeaderStepsDownAfterElectionWindow()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);

        TickElectionWindow(core);

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(0UL, core.LeaderId);
        Assert.Equal(1UL, core.Term);
    }

    [Fact]
    public void RecentVoterResponsePreservesOneWindowOnly()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);
        core.Step(HeartbeatResponse(core, from: 2));
        core.TakeMessages();

        TickElectionWindow(core);

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.True(core.Tracker.Progress[1].RecentActive);
        Assert.False(core.Tracker.Progress[2].RecentActive);
        Assert.False(core.Tracker.Progress[3].RecentActive);
        core.TakeMessages();

        TickElectionWindow(core);

        Assert.Equal(RaftRole.Follower, core.Role);
    }

    [Fact]
    public void RejectedAppendResponseCountsAsActivity()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Reject = true,
        });
        TickElectionWindow(core);

        Assert.Equal(RaftRole.Leader, core.Role);
    }

    [Fact]
    public void LearnerActivityCannotPreserveLeadership()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            learners: [4]);
        core.Step(HeartbeatResponse(core, from: 4));
        core.TakeMessages();

        TickElectionWindow(core);

        Assert.Equal(RaftRole.Follower, core.Role);
    }

    [Fact]
    public void JointConfigurationRequiresRecentActivityInBothMajorities()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            outgoingVoters: [1, 4, 5]);
        core.Step(HeartbeatResponse(core, from: 2));
        core.Step(HeartbeatResponse(core, from: 4));
        core.TakeMessages();

        TickElectionWindow(core);

        Assert.Equal(RaftRole.Leader, core.Role);
        core.TakeMessages();

        core.Step(HeartbeatResponse(core, from: 2));
        core.TakeMessages();
        TickElectionWindow(core);

        Assert.Equal(RaftRole.Follower, core.Role);
    }

    [Fact]
    public void LocalSingletonAlwaysHasActiveQuorum()
    {
        RaftCore core = NewLeader(voters: [1]);

        TickElectionWindow(core);
        TickElectionWindow(core);

        Assert.Equal(RaftRole.Leader, core.Role);
    }

    [Fact]
    public void NewlyAddedVoterReceivesOneFullActivityWindow()
    {
        RaftCore core = NewLeader(voters: [1]);
        core.ApplyConfigurationChange(V2(AddVoter(2)));
        core.TakeMessages();

        TickElectionWindow(core);

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.False(core.Tracker.Progress[2].RecentActive);
        core.TakeMessages();

        TickElectionWindow(core);

        Assert.Equal(RaftRole.Follower, core.Role);
    }

    [Fact]
    public void QuorumLossSuppressesHeartbeatDueOnSameTick()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            electionTick: 2,
            heartbeatTick: 1);
        core.TickLeader();
        Assert.NotEmpty(core.TakeMessages());

        core.TickLeader();

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Empty(core.TakeMessages());
    }

    private static RaftCore NewLeader(
        IEnumerable<ulong> voters,
        IEnumerable<ulong>? outgoingVoters = null,
        IEnumerable<ulong>? learners = null,
        int electionTick = 3,
        int heartbeatTick = 1)
    {
        RaftCore core = Create(
            voters: voters,
            outgoingVoters: outgoingVoters,
            learners: learners,
            electionTick: electionTick,
            heartbeatTick: heartbeatTick,
            checkQuorum: true).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        Message selfAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(selfAck);
        core.TakeMessages();
        return core;
    }

    private static void TickElectionWindow(
        RaftCore core)
    {
        for (var tick = 0; tick < core.GetClockStateForTesting().ElectionTick; tick++)
        {
            core.TickLeader();
        }
    }

    private static Message HeartbeatResponse(
        RaftCore core,
        ulong from)
    {
        return new Message
        {
            From = from,
            To = core.Id,
            Term = core.Term,
            Type = MessageType.MsgHeartbeatResp,
        };
    }

    private static ConfChangeV2 V2(
        ConfChangeSingle change)
    {
        var result = new ConfChangeV2();
        result.Changes.Add(change);
        return result;
    }

    private static ConfChangeSingle AddVoter(ulong id)
    {
        return new ConfChangeSingle
        {
            Type = ConfChangeType.ConfChangeAddNode,
            NodeId = id,
        };
    }
}
