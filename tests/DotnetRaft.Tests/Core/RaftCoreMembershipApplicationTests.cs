using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Tracker;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

using ConfigurationChangeException =
    DotnetRaft.ConfChange.ConfigurationChangeException;
using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreMembershipApplicationTests
{
    [Fact]
    public void V1AddVoterInstallsProgressAndProbesImmediately()
    {
        RaftCore core = NewLeader(voters: [1]);
        var change = new ProtocolConfChange
        {
            Type = ConfChangeType.ConfChangeAddNode,
            NodeId = 2,
            Context = ByteString.CopyFromUtf8("context"),
        };
        ProtocolConfChange original = change.Clone();

        ConfState state = core.ApplyConfigurationChange(change);

        Assert.Equal(original, change);
        Assert.Equal([1UL, 2UL], state.Voters);
        Assert.Equal([1UL, 2UL], core.Tracker.VoterNodes());
        Progress added = core.Tracker.Progress[2];
        Assert.Equal(0UL, added.Match);
        Assert.Equal(1UL, added.Next);
        Assert.True(added.RecentActive);
        Assert.False(added.IsLearner);
        Message probe = Assert.Single(core.TakeMessages());
        Assert.Equal(MessageType.MsgApp, probe.Type);
        Assert.Equal(2UL, probe.To);
        Assert.Equal([1UL], probe.Entries.Select(
            entry => entry.Index));
    }

    [Fact]
    public void LearnerCanBeAddedPromotedAndLocalNodeDemoted()
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2]).Core;

        ConfState withLearner = core.ApplyConfigurationChange(
            V2(AddLearner(3)));
        Assert.Equal([3UL], withLearner.Learners);
        Assert.True(core.Tracker.Progress[3].IsLearner);

        ConfState promoted = core.ApplyConfigurationChange(
            V2(AddVoter(3)));
        Assert.Equal([1UL, 2UL, 3UL], promoted.Voters);
        Assert.Empty(promoted.Learners);
        Assert.False(core.Tracker.Progress[3].IsLearner);

        ConfState demoted = core.ApplyConfigurationChange(
            V2(AddLearner(1)));
        Assert.Equal([2UL, 3UL], demoted.Voters);
        Assert.Equal([1UL], demoted.Learners);
        Assert.True(core.IsLearner);
        Assert.True(core.Tracker.Progress[1].IsLearner);

        core.ApplyConfigurationChange(V2(AddVoter(1)));

        Assert.False(core.IsLearner);
        Assert.False(core.Tracker.Progress[1].IsLearner);
    }

    [Fact]
    public void ExplicitJointChangeAndLeavePreserveStagedLearner()
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2]).Core;
        var enter = new ConfChangeV2
        {
            Transition = ConfChangeTransition.JointExplicit,
        };
        enter.Changes.Add(AddVoter(3));
        enter.Changes.Add(AddLearner(1));

        ConfState joint = core.ApplyConfigurationChange(enter);

        Assert.Equal([2UL, 3UL], joint.Voters);
        Assert.Equal([1UL, 2UL], joint.VotersOutgoing);
        Assert.Equal([1UL], joint.LearnersNext);
        Assert.Empty(joint.Learners);
        Assert.False(core.IsLearner);
        Assert.False(core.Tracker.Progress[1].IsLearner);

        ConfState final = core.ApplyConfigurationChange(
            new ConfChangeV2());

        Assert.Equal([2UL, 3UL], final.Voters);
        Assert.Empty(final.VotersOutgoing);
        Assert.Equal([1UL], final.Learners);
        Assert.Empty(final.LearnersNext);
        Assert.True(core.IsLearner);
        Assert.True(core.Tracker.Progress[1].IsLearner);
    }

    [Fact]
    public void InvalidApplicationLeavesLiveStateUntouched()
    {
        RaftCore core = NewLeader(voters: [1]);
        ConfState before = core.Tracker.ToConfState();
        Progress local = core.Tracker.Progress[1];
        SoftState softState = core.SoftState;
        ulong committed = core.Log.Committed;

        Assert.Throws<ConfigurationChangeException>(
            () => core.ApplyConfigurationChange(
                V2(Remove(1))));

        Assert.True(before.IsEquivalentTo(
            core.Tracker.ToConfState()));
        Assert.Same(local, core.Tracker.Progress[1]);
        Assert.Equal(softState, core.SoftState);
        Assert.Equal(committed, core.Log.Committed);
        Assert.False(core.IsLearner);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void ExplicitEmptyEntryAcceptedInJointFailsApplicationAtomically()
    {
        RaftCore core = Create(
            voters: [1, 2]).Core;
        var enter = new ConfChangeV2
        {
            Transition = ConfChangeTransition.JointExplicit,
        };
        enter.Changes.Add(AddLearner(3));
        core.ApplyConfigurationChange(enter);
        ConfState before = core.Tracker.ToConfState();
        ProgressMap progress = core.Tracker.Progress;

        Assert.Throws<ConfigurationChangeException>(
            () => core.ApplyConfigurationChange(
                new ConfChangeV2
                {
                    Transition =
                        ConfChangeTransition.JointExplicit,
                }));

        Assert.True(before.IsEquivalentTo(
            core.Tracker.ToConfState()));
        Assert.Same(progress, core.Tracker.Progress);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void ReducedQuorumCanCommitAlreadyReplicatedEntry()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3, 4]);
        core.Step(Proposal("command"));
        Message localAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(localAck);
        core.TakeMessages();
        Assert.True(
            core.Tracker.Progress[2].MaybeUpdate(2));
        Assert.Equal(0UL, core.Log.Committed);

        core.ApplyConfigurationChange(V2(Remove(4)));

        Assert.Equal(2UL, core.Log.Committed);
        Assert.DoesNotContain(
            core.TakeMessages(),
            message => message.To == 4);
        Assert.Equal([1UL, 2UL, 3UL], core.Tracker.VoterNodes());
    }

    [Fact]
    public void RemovedLeaderStaysActiveByDefaultButDropsProposals()
    {
        RaftCore core = NewLeader(voters: [1, 2]);
        ulong term = core.Term;

        core.ApplyConfigurationChange(V2(Remove(1)));

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.Equal(term, core.Term);
        Assert.False(core.IsLearner);
        Assert.False(core.Promotable);
        Assert.False(core.Tracker.Progress.ContainsKey(1));
        Assert.Throws<ProposalDroppedException>(
            () => core.Step(Proposal("rejected")));
    }

    [Fact]
    public void DemotedLeaderStaysActiveAndAcceptsProposalsByDefault()
    {
        RaftCore core = NewLeader(voters: [1, 2]);

        core.ApplyConfigurationChange(V2(AddLearner(1)));
        core.TakeMessages();
        ulong lastIndex = core.Log.LastIndex;

        core.Step(Proposal("accepted"));

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.True(core.IsLearner);
        Assert.False(core.Promotable);
        Assert.True(core.Tracker.Progress.ContainsKey(1));
        Assert.Equal(lastIndex + 1, core.Log.LastIndex);
        Assert.Single(core.TakeMessagesAfterAppend());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void StepDownOnRemovalHandlesRemovalAndDemotion(
        bool demote)
    {
        RaftCore core = NewLeader(
            voters: [1, 2],
            stepDownOnRemoval: true);
        ulong term = core.Term;

        core.ApplyConfigurationChange(
            demote
                ? V2(AddLearner(1))
                : V2(Remove(1)));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(term, core.Term);
        Assert.Equal(0UL, core.LeaderId);
        Assert.Equal(
            demote,
            core.IsLearner);
    }

    [Fact]
    public void StagedLocalLearnerStepsDownOnlyAfterLeavingJoint()
    {
        RaftCore core = NewLeader(
            voters: [1, 2],
            stepDownOnRemoval: true);
        var enter = new ConfChangeV2
        {
            Transition = ConfChangeTransition.JointExplicit,
        };
        enter.Changes.Add(AddLearner(1));

        core.ApplyConfigurationChange(enter);

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.False(core.IsLearner);
        Assert.Contains(1UL, core.Tracker.Config.LearnersNext);

        core.ApplyConfigurationChange(new ConfChangeV2());

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.True(core.IsLearner);
        Assert.Empty(core.Tracker.Config.LearnersNext);
    }

    [Fact]
    public void TransferTargetClearsOnlyAfterJointDemotionCompletes()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);
        core.LeaderTransferee = 3;
        var enter = new ConfChangeV2();
        enter.Changes.Add(Remove(3));
        enter.Changes.Add(AddLearner(3));

        core.ApplyConfigurationChange(enter);

        Assert.Equal(3UL, core.LeaderTransferee);
        Assert.Contains(
            3UL,
            core.Tracker.Config.Voters.Outgoing);
        Assert.Contains(3UL, core.Tracker.Config.LearnersNext);

        core.ApplyConfigurationChange(new ConfChangeV2());

        Assert.Equal(0UL, core.LeaderTransferee);
        Assert.Contains(3UL, core.Tracker.Config.Learners);
        Assert.False(
            core.Tracker.Config.Voters.Ids().Contains(3));
    }

    [Fact]
    public void RemovedFollowerCannotCampaign()
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2]).Core;

        core.ApplyConfigurationChange(V2(Remove(1)));
        core.Step(Hup(1));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.False(core.Promotable);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    private static RaftCore NewLeader(
        IEnumerable<ulong> voters,
        bool stepDownOnRemoval = false)
    {
        RaftCore core = Create(
            voters: voters,
            stepDownOnRemoval: stepDownOnRemoval).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        Message noOpAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(noOpAck);
        core.TakeMessages();
        return core;
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

    private static ConfChangeSingle AddLearner(ulong id)
    {
        return new ConfChangeSingle
        {
            Type = ConfChangeType.ConfChangeAddLearnerNode,
            NodeId = id,
        };
    }

    private static ConfChangeSingle Remove(ulong id)
    {
        return new ConfChangeSingle
        {
            Type = ConfChangeType.ConfChangeRemoveNode,
            NodeId = id,
        };
    }

    private static Message Proposal(string data)
    {
        var message = new Message
        {
            From = 1,
            To = 1,
            Type = MessageType.MsgProp,
        };
        message.Entries.Add(new Entry
        {
            Data = ByteString.CopyFromUtf8(data),
        });
        return message;
    }
}
