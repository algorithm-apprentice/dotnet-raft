using DotnetRaft.Core;
using DotnetRaft.Protocol;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;
using static DotnetRaft.Tests.Core.RaftCoreLeadershipTransferTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreLeadershipTransferMembershipTests
{
    [Fact]
    public void LearnersNextOutgoingVoterRemainsEligible()
    {
        RaftCore core = CreateLeader(
            voters: [1, 2],
            outgoingVoters: [1, 2, 3],
            learnersNext: [3]).Core;
        MakeUpToDate(core, 3);

        core.Step(Transfer(3));

        Assert.Equal(3UL, core.LeaderTransferee);
        Message timeout = Assert.Single(core.TakeMessages());
        Assert.Equal(MessageType.MsgTimeoutNow, timeout.Type);
        Assert.Equal(3UL, timeout.To);
    }

    [Fact]
    public void JointDemotionClearsTargetOnlyAfterLeave()
    {
        RaftCore core = CreateLeader().Core;
        core.Step(Transfer(3));
        core.TakeMessages();
        var enter = new ConfChangeV2();
        enter.Changes.Add(Remove(3));
        enter.Changes.Add(AddLearner(3));

        core.ApplyConfigurationChange(enter);

        Assert.Equal(3UL, core.LeaderTransferee);
        Assert.Contains(
            3UL,
            core.Tracker.Config.LearnersNext);

        core.ApplyConfigurationChange(
            new ConfChangeV2());

        Assert.Equal(0UL, core.LeaderTransferee);
        Assert.Contains(
            3UL,
            core.Tracker.Config.Learners);
    }

    [Fact]
    public void RemovingTargetFromSimpleConfigCancelsTransfer()
    {
        RaftCore core = CreateLeader().Core;
        core.Step(Transfer(3));
        core.TakeMessages();
        var remove = new ConfChangeV2();
        remove.Changes.Add(Remove(3));

        core.ApplyConfigurationChange(remove);

        Assert.Equal(0UL, core.LeaderTransferee);
    }

    [Fact]
    public void RetainedRemovedLeaderPreservesStaleTargetUntilReplacement()
    {
        RaftCore core = CreateLeader(
            voters: [1, 2, 3, 4],
            stepDownOnRemoval: false).Core;
        core.Step(Transfer(3));
        core.TakeMessages();
        var enter = new ConfChangeV2();
        enter.Changes.Add(Remove(1));
        enter.Changes.Add(Remove(3));
        core.ApplyConfigurationChange(enter);
        core.TakeMessages();

        core.ApplyConfigurationChange(
            new ConfChangeV2());

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.False(
            core.Tracker.Progress.ContainsKey(1));
        Assert.False(
            core.Tracker.Progress.ContainsKey(3));
        Assert.Equal(3UL, core.LeaderTransferee);

        ProposalDroppedException dropped =
            Assert.Throws<ProposalDroppedException>(
                () => core.Step(Proposal(
                    core.Id,
                    NormalEntry())));
        Assert.Contains(
            "local replication progress",
            dropped.Message,
            StringComparison.Ordinal);

        core.Step(Transfer(1));
        Assert.Equal(3UL, core.LeaderTransferee);

        core.Step(Transfer(2));
        Assert.Equal(2UL, core.LeaderTransferee);
        Assert.Equal(0, core.GetClockStateForTesting().ElectionElapsed);
    }

    [Fact]
    public void AutoLeaveRetriesImmediatelyAfterTransferTimeout()
    {
        RaftCore core = CreateLeader(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 1,
            commit: 1,
            applied: 1,
            electionTick: 3).Core;
        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = core.Log.LastIndex,
        });
        core.TakeMessages();
        Assert.Equal(2UL, core.Log.Committed);

        var enter = new ConfChangeV2();
        enter.Changes.Add(AddLearner(4));
        enter.Changes.Add(AddLearner(5));
        core.ApplyConfigurationChange(enter);
        core.TakeMessages();
        core.SetPendingConfigurationIndexForTesting(2);
        core.Step(Transfer(3));
        core.TakeMessages();

        core.AppliedTo(2, 0);

        Assert.Equal(2UL, core.Log.LastIndex);
        Assert.Equal(2UL, core.PendingConfigurationIndex);

        for (var tick = 0;
             tick < core.GetClockStateForTesting().ElectionTick;
             tick++)
        {
            core.TickLeader();
            core.TakeMessages();
        }

        Assert.Equal(0UL, core.LeaderTransferee);
        Assert.Equal(3UL, core.Log.LastIndex);
        Assert.Equal(3UL, core.PendingConfigurationIndex);
        Entry exit = Assert.Single(
            core.Log.GetEntries(3));
        Assert.Equal(
            EntryType.EntryConfChangeV2,
            exit.Type);
        Assert.Equal(
            new ConfChangeV2(),
            ConfChangeV2.Parser.ParseFrom(
                exit.Data));

        core.AppliedTo(2, 0);
        for (var tick = 0;
             tick < core.GetClockStateForTesting().ElectionTick;
             tick++)
        {
            core.TickLeader();
            core.TakeMessages();
        }

        Assert.Equal(3UL, core.Log.LastIndex);
        Assert.Equal(3UL, core.PendingConfigurationIndex);
    }

    [Fact]
    public void CheckQuorumStepDownSuppressesTimeoutAutoLeaveRetry()
    {
        RaftCore core = CreateLeader(
            voters: [1, 2, 3],
            outgoingVoters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 1,
            commit: 1,
            applied: 1,
            electionTick: 2,
            checkQuorum: true).Core;
        core.Tracker.Config.AutoLeave = true;
        core.SetPendingConfigurationIndexForTesting(1);
        core.SetLeaderTransfereeForTesting(2);
        ulong lastIndex = core.Log.LastIndex;

        for (var tick = 0;
             tick < core.GetClockStateForTesting().ElectionTick;
             tick++)
        {
            core.TickLeader();
            core.TakeMessages();
        }

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(0UL, core.LeaderTransferee);
        Assert.Equal(lastIndex, core.Log.LastIndex);
    }

    private static ConfChangeSingle Remove(ulong id)
    {
        return new ConfChangeSingle
        {
            Type = ConfChangeType.ConfChangeRemoveNode,
            NodeId = id,
        };
    }

    private static ConfChangeSingle AddLearner(ulong id)
    {
        return new ConfChangeSingle
        {
            Type =
                ConfChangeType.ConfChangeAddLearnerNode,
            NodeId = id,
        };
    }
}
