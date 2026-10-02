using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreMembershipProposalTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AcceptedConfigurationEntryPreservesEncodingAndCaller(
        bool useV2)
    {
        RaftCore core = NewLeader();
        Message proposal = useV2
            ? Proposal(V2Entry(AddVoter(2)))
            : Proposal(V1Entry(new ProtocolConfChange
            {
                Type = ConfChangeType.ConfChangeAddNode,
                NodeId = 2,
                Context = ByteString.CopyFromUtf8("v1"),
            }));
        Message original = proposal.Clone();

        core.Step(proposal);

        Assert.Equal(original, proposal);
        Entry appended = Assert.Single(core.Log.GetEntries(2));
        Assert.Equal(
            useV2
                ? EntryType.EntryConfChangeV2
                : EntryType.EntryConfChange,
            appended.Type);
        Assert.Equal(
            original.Entries[0].Data,
            appended.Data);
        Assert.Equal(2UL, core.PendingConfigurationIndex);
    }

    [Fact]
    public void OnlyFirstConfigurationEntryInProposalRemainsPending()
    {
        RaftCore core = NewLeader();
        Message proposal = Proposal(
            V1Entry(new ProtocolConfChange
            {
                Type = ConfChangeType.ConfChangeAddNode,
                NodeId = 2,
            }),
            V2Entry(AddVoter(3)));
        Message original = proposal.Clone();

        core.Step(proposal);

        Assert.Equal(original, proposal);
        Entry[] appended = [.. core.Log.GetEntries(2)];
        Assert.Equal(2, appended.Length);
        Assert.Equal(EntryType.EntryConfChange, appended[0].Type);
        Assert.Equal(EntryType.EntryNormal, appended[1].Type);
        Assert.True(appended[1].Data.IsEmpty);
        Assert.Equal(2UL, core.PendingConfigurationIndex);
    }

    [Fact]
    public void RejectedConfigurationDoesNotBlockLaterEntryInSameProposal()
    {
        RaftCore core = NewLeader();

        core.Step(Proposal(
            V2Entry(new ConfChangeV2()),
            V2Entry(AddVoter(2))));

        Entry[] appended = [.. core.Log.GetEntries(2)];
        Assert.Equal(2, appended.Length);
        Assert.Equal(EntryType.EntryNormal, appended[0].Type);
        Assert.Equal(EntryType.EntryConfChangeV2, appended[1].Type);
        Assert.Equal(3UL, core.PendingConfigurationIndex);
    }

    [Fact]
    public void LaterConfigurationProposalBecomesNormalWhilePending()
    {
        RaftCore core = NewLeader();
        core.Step(Proposal(V2Entry(AddVoter(2))));
        core.TakeMessages();
        core.TakeMessagesAfterAppend();

        core.Step(Proposal(V2Entry(AddVoter(3))));

        Entry appended = Assert.Single(core.Log.GetEntries(3));
        Assert.Equal(EntryType.EntryNormal, appended.Type);
        Assert.True(appended.Data.IsEmpty);
        Assert.Equal(2UL, core.PendingConfigurationIndex);
    }

    [Fact]
    public void AppliedPendingChangeAllowsNextConfigurationProposal()
    {
        RaftCore core = NewLeader();
        var first = new ConfChangeV2();
        first.Changes.Add(AddVoter(2));
        core.Step(Proposal(V2Entry(first)));
        Message firstAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(firstAck);
        core.TakeMessages();

        core.ApplyConfigurationChange(first);
        core.AppliedTo(2, 0);
        core.Step(Proposal(V2Entry(AddLearner(3))));

        Entry appended = Assert.Single(core.Log.GetEntries(3));
        Assert.Equal(EntryType.EntryConfChangeV2, appended.Type);
        Assert.Equal(3UL, core.PendingConfigurationIndex);
    }

    [Fact]
    public void NonemptyChangeWhileJointBecomesNormal()
    {
        RaftCore core = NewJointLeader();
        ulong previousPending = core.PendingConfigurationIndex;

        core.Step(Proposal(V2Entry(AddVoter(3))));

        Entry appended = Assert.Single(
            core.Log.GetEntries(core.Log.LastIndex));
        Assert.Equal(EntryType.EntryNormal, appended.Type);
        Assert.True(appended.Data.IsEmpty);
        Assert.Equal(
            previousPending,
            core.PendingConfigurationIndex);
    }

    [Fact]
    public void EmptyV1IsAcceptedOutsideJoint()
    {
        RaftCore core = NewLeader();
        var empty = new ProtocolConfChange();

        core.Step(Proposal(V1Entry(empty)));

        Entry appended = Assert.Single(core.Log.GetEntries(2));
        Assert.Equal(EntryType.EntryConfChange, appended.Type);
        Assert.Equal(empty.ToByteString(), appended.Data);
        Assert.Equal(2UL, core.PendingConfigurationIndex);
    }

    [Fact]
    public void EmptyV1BecomesNormalInsideJoint()
    {
        RaftCore core = NewJointLeader();
        ulong previousPending = core.PendingConfigurationIndex;

        core.Step(Proposal(V1Entry(
            new ProtocolConfChange())));

        Entry appended = Assert.Single(
            core.Log.GetEntries(core.Log.LastIndex));
        Assert.Equal(EntryType.EntryNormal, appended.Type);
        Assert.True(appended.Data.IsEmpty);
        Assert.Equal(
            previousPending,
            core.PendingConfigurationIndex);
    }

    [Theory]
    [InlineData(ConfChangeTransition.Auto)]
    [InlineData(ConfChangeTransition.JointImplicit)]
    [InlineData(ConfChangeTransition.JointExplicit)]
    public void EmptyV2BecomesNormalOutsideJoint(
        ConfChangeTransition transition)
    {
        RaftCore core = NewLeader();

        core.Step(Proposal(V2Entry(new ConfChangeV2
        {
            Transition = transition,
        })));

        Entry appended = Assert.Single(core.Log.GetEntries(2));
        Assert.Equal(EntryType.EntryNormal, appended.Type);
        Assert.True(appended.Data.IsEmpty);
        Assert.Equal(0UL, core.PendingConfigurationIndex);
    }

    [Theory]
    [InlineData(ConfChangeTransition.Auto)]
    [InlineData(ConfChangeTransition.JointImplicit)]
    [InlineData(ConfChangeTransition.JointExplicit)]
    public void EmptyV2IsAcceptedInsideJoint(
        ConfChangeTransition transition)
    {
        RaftCore core = NewJointLeader();
        ulong expectedIndex = core.Log.LastIndex + 1;

        core.Step(Proposal(V2Entry(new ConfChangeV2
        {
            Transition = transition,
        })));

        Entry appended = Assert.Single(
            core.Log.GetEntries(expectedIndex));
        Assert.Equal(EntryType.EntryConfChangeV2, appended.Type);
        Assert.Equal(expectedIndex, core.PendingConfigurationIndex);
    }

    [Theory]
    [InlineData(EntryType.EntryConfChange)]
    [InlineData(EntryType.EntryConfChangeV2)]
    public void MalformedConfigurationFailsAtomically(
        EntryType type)
    {
        foreach (bool disableValidation in new[] { false, true })
        {
            RaftCore core = NewLeader(
                disableValidation: disableValidation);
            ulong lastIndex = core.Log.LastIndex;
            ulong pending = core.PendingConfigurationIndex;
            var proposal = Proposal(new Entry
            {
                Type = type,
                Data = ByteString.CopyFrom(0xff),
            });

            Assert.Throws<InvalidProtocolBufferException>(
                () => core.Step(proposal));

            Assert.Equal(lastIndex, core.Log.LastIndex);
            Assert.Equal(
                pending,
                core.PendingConfigurationIndex);
            Assert.Empty(core.TakeMessages());
            Assert.Empty(core.TakeMessagesAfterAppend());
        }
    }

    [Fact]
    public void DisabledValidationAcceptsOverlappingChanges()
    {
        RaftCore core = NewLeader(disableValidation: true);
        var first = new ConfChangeV2
        {
            Transition = ConfChangeTransition.JointExplicit,
        };
        first.Changes.Add(AddLearner(2));
        core.ApplyConfigurationChange(first);
        core.TakeMessages();
        ulong firstIndex = core.Log.LastIndex + 1;

        core.Step(Proposal(
            V2Entry(AddVoter(3)),
            V2Entry(AddVoter(4))));

        Entry[] appended = [.. core.Log.GetEntries(firstIndex)];
        Assert.Equal(2, appended.Length);
        Assert.All(
            appended,
            entry => Assert.Equal(
                EntryType.EntryConfChangeV2,
                entry.Type));
        Assert.Equal(
            firstIndex + 1,
            core.PendingConfigurationIndex);
    }

    [Fact]
    public void QuotaRejectionDoesNotPublishPendingIndex()
    {
        RaftCore core = NewLeader(
            maxUncommittedEntriesSize: 1);
        core.Step(Proposal(new Entry
        {
            Data = ByteString.CopyFromUtf8("x"),
        }));
        core.TakeMessages();
        core.TakeMessagesAfterAppend();
        ulong lastIndex = core.Log.LastIndex;

        Assert.Throws<ProposalDroppedException>(
            () => core.Step(Proposal(V2Entry(AddVoter(2)))));

        Assert.Equal(lastIndex, core.Log.LastIndex);
        Assert.Equal(0UL, core.PendingConfigurationIndex);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void IndexFailureDoesNotPublishPendingIndex()
    {
        var storage = new CoreTestStorage();
        storage.LogStorage.ApplySnapshot(new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = ulong.MaxValue - 2,
                Term = 1,
                ConfState = new ConfState
                {
                    Voters = { 1 },
                },
            },
        });
        storage.InitialState = new StorageState(
            new HardState
            {
                Term = 1,
                Commit = ulong.MaxValue - 2,
            },
            new ConfState
            {
                Voters = { 1 },
            });
        var core = new RaftCore(
            new RaftConfig
            {
                Id = 1,
                Storage = storage,
                Applied = ulong.MaxValue - 2,
            },
            _ => 0);
        core.BecomeCandidate();
        core.BecomeLeader();
        ulong pending = core.PendingConfigurationIndex;

        Assert.Throws<RaftInvariantException>(
            () => core.Step(
                Proposal(V2Entry(AddVoter(2)))));

        Assert.Equal(
            ulong.MaxValue - 1,
            core.Log.LastIndex);
        Assert.Equal(pending, core.PendingConfigurationIndex);
        Assert.Single(core.TakeMessagesAfterAppend());
    }

    private static RaftCore NewJointLeader()
    {
        RaftCore core = NewLeader();
        var change = new ConfChangeV2
        {
            Transition = ConfChangeTransition.JointExplicit,
        };
        change.Changes.Add(AddLearner(2));
        core.ApplyConfigurationChange(change);
        core.TakeMessages();
        return core;
    }

    private static RaftCore NewLeader(
        bool disableValidation = false,
        ulong maxUncommittedEntriesSize = 0)
    {
        RaftCore core = Create(
            voters: [1],
            disableConfChangeValidation: disableValidation,
            maxUncommittedEntriesSize:
                maxUncommittedEntriesSize).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        Message noOpAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(noOpAck);
        core.TakeMessages();
        return core;
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

    private static Entry V1Entry(
        ProtocolConfChange change)
    {
        return new Entry
        {
            Type = EntryType.EntryConfChange,
            Data = change.ToByteString(),
        };
    }

    private static Entry V2Entry(
        ConfChangeSingle change)
    {
        var changes = new ConfChangeV2();
        changes.Changes.Add(change);
        return V2Entry(changes);
    }

    private static Entry V2Entry(ConfChangeV2 change)
    {
        return new Entry
        {
            Type = EntryType.EntryConfChangeV2,
            Data = change.ToByteString(),
        };
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
}
