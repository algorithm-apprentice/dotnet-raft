using DotnetRaft.Core;
using DotnetRaft.Protocol;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreMembershipAutoLeaveTests
{
    [Fact]
    public void ApplyingJointEntryAppendsOneExitAfterExistingTail()
    {
        RaftCore core = NewSingletonLeader();
        ConfChangeV2 change = AutoJointChange();
        core.Step(Proposal(
            ConfigurationEntry(change),
            NormalEntry("one"),
            NormalEntry("two")));
        Message proposalAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(proposalAck);
        core.TakeMessages();
        Assert.Equal(4UL, core.Log.Committed);
        core.AppliedTo(1, 0);

        core.ApplyConfigurationChange(change);
        core.TakeMessages();
        core.AppliedTo(2, 0);

        Assert.Equal(5UL, core.Log.LastIndex);
        Assert.Equal(5UL, core.PendingConfigurationIndex);
        Entry exit = Assert.Single(core.Log.GetEntries(5));
        Assert.Equal(EntryType.EntryConfChangeV2, exit.Type);
        Assert.Equal(new ConfChangeV2(), ConfChangeV2.Parser
            .ParseFrom(exit.Data));

        core.AppliedTo(3, 0);
        core.AppliedTo(4, 0);

        Assert.Equal(5UL, core.Log.LastIndex);
        Assert.Single(
            core.Log.GetEntries(2),
            entry =>
                entry.Type == EntryType.EntryConfChangeV2
                && entry.Index > 2);
    }

    [Fact]
    public void FollowerDefersAutoLeaveUntilLaterLeaderAppliesEntry()
    {
        RaftCore core = NewSingletonLeader();
        var change = new ConfChangeV2
        {
            Transition =
                ConfChangeTransition.JointImplicit,
        };
        change.Changes.Add(AddLearner(2));
        core.Step(Proposal(ConfigurationEntry(change)));
        core.Step(Assert.Single(
            core.TakeMessagesAfterAppend()));
        core.TakeMessages();
        core.AppliedTo(1, 0);
        core.BecomeFollower(core.Term, 2);

        core.ApplyConfigurationChange(change);
        core.AppliedTo(2, 0);

        Assert.Equal(2UL, core.Log.LastIndex);
        Assert.True(core.Tracker.Config.AutoLeave);

        core.BecomeCandidate();
        core.BecomeLeader();
        core.Step(Assert.Single(
            core.TakeMessagesAfterAppend()));
        core.TakeMessages();
        core.AppliedTo(3, 0);

        Entry exit = Assert.Single(core.Log.GetEntries(4));
        Assert.Equal(EntryType.EntryConfChangeV2, exit.Type);
        Assert.Equal(new ConfChangeV2(), ConfChangeV2.Parser
            .ParseFrom(exit.Data));
        Assert.Equal(4UL, core.PendingConfigurationIndex);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RestoredSnapshotAutoLeavesOnlyAfterLeaderApplication(
        bool autoLeave)
    {
        ElectionCoreFixture fixture = Create(
            id: 1,
            voters: [1],
            term: 1);
        RaftCore core = fixture.Core;
        Snapshot snapshot = JointSnapshot(autoLeave);

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 2,
            Type = MessageType.MsgSnap,
            Snapshot = snapshot,
        });

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(5UL, core.Log.LastIndex);
        Assert.Equal(autoLeave, core.Tracker.Config.AutoLeave);
        Assert.Single(core.TakeMessagesAfterAppend());
        Assert.Equal(5UL, core.Log.LastIndex);
        Snapshot pending = core.Log.GetNextUnstableSnapshot()!;
        core.Log.AcceptUnstable();
        fixture.Storage.LogStorage.ApplySnapshot(pending);
        core.Log.AcknowledgeSnapshot(5);
        Assert.Equal(5UL, core.Log.LastIndex);

        core.BecomeCandidate();
        core.BecomeLeader();
        core.Step(Assert.Single(
            core.TakeMessagesAfterAppend()));
        core.TakeMessages();
        core.AppliedTo(6, 0);

        Entry[] exits = [.. core.Log.GetEntries(7)
            .Where(entry =>
                entry.Type == EntryType.EntryConfChangeV2)];
        if (autoLeave)
        {
            Entry exit = Assert.Single(exits);
            Assert.Equal(new ConfChangeV2(), ConfChangeV2.Parser
                .ParseFrom(exit.Data));
            Assert.Equal(7UL, core.PendingConfigurationIndex);
        }
        else
        {
            Assert.Empty(exits);
            Assert.Equal(5UL, core.PendingConfigurationIndex);
        }
    }

    private static RaftCore NewSingletonLeader()
    {
        RaftCore core = Create(voters: [1]).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        core.Step(Assert.Single(
            core.TakeMessagesAfterAppend()));
        core.TakeMessages();
        return core;
    }

    private static ConfChangeV2 AutoJointChange()
    {
        var change = new ConfChangeV2();
        change.Changes.Add(AddVoter(2));
        change.Changes.Add(AddLearner(3));
        return change;
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

    private static Message Proposal(params Entry[] entries)
    {
        var message = new Message
        {
            From = 1,
            To = 1,
            Type = MessageType.MsgProp,
        };
        message.Entries.Add(entries);
        return message;
    }

    private static Entry ConfigurationEntry(
        ConfChangeV2 change)
    {
        return new Entry
        {
            Type = EntryType.EntryConfChangeV2,
            Data = change.ToByteString(),
        };
    }

    private static Entry NormalEntry(string data)
    {
        return new Entry
        {
            Data = ByteString.CopyFromUtf8(data),
        };
    }

    private static Snapshot JointSnapshot(bool autoLeave)
    {
        return new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = 5,
                Term = 2,
                ConfState = new ConfState
                {
                    Voters = { 1 },
                    VotersOutgoing = { 1 },
                    AutoLeave = autoLeave,
                },
            },
        };
    }
}
