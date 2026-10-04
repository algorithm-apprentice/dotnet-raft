using DotnetRaft.ConfChange;
using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using static DotnetRaft.Tests.RawNode.RawNodeTestSupport;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.RawNode;

public sealed class RawNodeConfigurationTests
{
    [Fact]
    public void V1ProposalPreservesExactEncodingAndInput()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(storage);
        BecomeSingletonLeader(node, storage);
        var change = new ProtocolConfChange
        {
            Type = ConfChangeType.ConfChangeAddNode,
            NodeId = 2,
            Context = ByteString.CopyFromUtf8("v1"),
        };
        ProtocolConfChange original = change.Clone();

        node.ProposeConfChange(change);
        change.NodeId = 99;

        Ready ready = node.Ready();
        Entry entry = Assert.Single(ready.Entries);
        Assert.Equal(EntryType.EntryConfChange, entry.Type);
        Assert.Equal(original.ToByteString(), entry.Data);
    }

    [Fact]
    public void V2ProposalPreservesExactEncodingAndInput()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(storage);
        BecomeSingletonLeader(node, storage);
        var change = new ConfChangeV2
        {
            Transition =
                ConfChangeTransition.JointExplicit,
            Context = ByteString.CopyFromUtf8("v2"),
        };
        change.Changes.Add(new ConfChangeSingle
        {
            Type = ConfChangeType.ConfChangeAddLearnerNode,
            NodeId = 2,
        });
        ConfChangeV2 original = change.Clone();

        node.ProposeConfChange(change);
        change.Changes[0].NodeId = 99;

        Ready ready = node.Ready();
        Entry entry = Assert.Single(ready.Entries);
        Assert.Equal(EntryType.EntryConfChangeV2, entry.Type);
        Assert.Equal(original.ToByteString(), entry.Data);
    }

    [Fact]
    public void ProposalRejectsStructuralErrorsBeforeLogMutation()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(
            storage,
            disableConfChangeValidation: true);
        BecomeSingletonLeader(node, storage);
        ulong lastIndex = node.Core.Log.LastIndex;

        Assert.Throws<ArgumentException>(
            () => node.ProposeConfChange(
                new ProtocolConfChange
                {
                    Type = (ConfChangeType)99,
                    NodeId = 2,
                }));
        Assert.Throws<ArgumentException>(
            () => node.ProposeConfChange(
                new ProtocolConfChange
                {
                    NodeId =
                        RaftLocalMessageTargets
                            .AppendThread,
                }));
        Assert.Throws<ArgumentException>(
            () => node.ProposeConfChange(
                new ConfChangeV2
                {
                    Transition =
                        (ConfChangeTransition)99,
                }));
        var unknownType = new ConfChangeV2();
        unknownType.Changes.Add(
            new ConfChangeSingle
            {
                Type = (ConfChangeType)99,
                NodeId = 2,
            });
        Assert.Throws<ArgumentException>(
            () => node.ProposeConfChange(
                unknownType));

        Assert.Equal(
            lastIndex,
            node.Core.Log.LastIndex);
        Assert.False(node.HasReady());

        node.Propose("valid"u8);
        Assert.Single(node.Ready().Entries);
    }

    [Fact]
    public void ZeroNodeIdRemainsAConfigurationNoOp()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(storage);
        BecomeSingletonLeader(node, storage);

        node.ProposeConfChange(
            new ProtocolConfChange
            {
                NodeId = 0,
            });

        Entry entry = Assert.Single(
            node.Ready().Entries);
        ConfState state = node.ApplyConfChange(
            ProtocolConfChange.Parser.ParseFrom(
                entry.Data));
        Assert.Equal(
            [1UL],
            state.Voters);
    }

    [Theory]
    [InlineData(ulong.MaxValue)]
    [InlineData(ulong.MaxValue - 1)]
    public void RestoredConfigurationRejectsReservedIds(
        ulong id)
    {
        MemoryStorage storage =
            CreateStorage(voters: [1, id]);

        Assert.Throws<
            ConfigurationChangeException>(
            () => CreateNode(storage));
    }

    [Fact]
    public void ApplyConfChangeReturnsDetachedState()
    {
        var node = CreateNode(CreateStorage());
        var change = new ProtocolConfChange
        {
            Type = ConfChangeType.ConfChangeAddNode,
            NodeId = 2,
        };

        ConfState returned = node.ApplyConfChange(change);
        returned.Voters.Add(99);

        Assert.True(
            node.Core.Tracker.ToConfState().IsEquivalentTo(
                new ConfState
                {
                    Voters = { 1, 2 },
                }));
        Assert.Equal(2UL, change.NodeId);
    }

    [Fact]
    public void AcceptedConfigurationIsInstalledBeforeAdvance()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(storage);
        BecomeSingletonLeader(node, storage);
        node.ProposeConfChange(new ProtocolConfChange
        {
            Type = ConfChangeType.ConfChangeAddNode,
            NodeId = 2,
        });
        Ready append = node.Ready();
        PersistAndAdvance(node, storage, append);
        Ready apply = node.Ready();
        Entry entry = Assert.Single(apply.CommittedEntries);
        var application = new OrderedApplicationHarness();
        Persist(storage, apply);

        Assert.Throws<InvalidOperationException>(
            () => application.Advance(node, apply));

        application.AcceptConfiguration(node, entry);
        application.Advance(node, apply);

        Assert.Contains(
            2UL,
            application.LatestConfState!.Voters);
        Assert.Equal(entry.Index, application.PhysicalApplied);
        Snapshot snapshot = application.CreateSnapshot(
            storage,
            entry.Index,
            ByteString.CopyFromUtf8("state"));
        Assert.Contains(
            2UL,
            snapshot.Metadata.ConfState.Voters);
    }

    [Fact]
    public void RejectedConfigurationAdvancesAsNoOp()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(storage);
        BecomeSingletonLeader(node, storage);
        node.ProposeConfChange(new ProtocolConfChange
        {
            Type = ConfChangeType.ConfChangeAddNode,
            NodeId = 2,
        });
        Ready append = node.Ready();
        PersistAndAdvance(node, storage, append);
        Ready apply = node.Ready();
        Entry entry = Assert.Single(apply.CommittedEntries);
        var application = new OrderedApplicationHarness();
        Persist(storage, apply);

        application.RejectConfiguration(entry);
        application.Advance(node, apply);

        Assert.False(
            node.Core.Tracker.Progress.ContainsKey(2));
        Assert.Null(application.LatestConfState);
        Assert.Equal(entry.Index, application.PhysicalApplied);
    }

    [Fact]
    public void ApplyConfChangeFailureFaultsFacade()
    {
        var node = CreateNode(CreateStorage());
        var invalid = new ConfChangeV2
        {
            Transition = (ConfChangeTransition)999,
        };
        invalid.Changes.Add(new ConfChangeSingle
        {
            Type = ConfChangeType.ConfChangeAddNode,
            NodeId = 2,
        });

        Assert.ThrowsAny<InvalidOperationException>(
            () => node.ApplyConfChange(invalid));

        Assert.Throws<InvalidOperationException>(
            node.Tick);
        Assert.Throws<InvalidOperationException>(
            () => node.HasReady());
        Assert.Throws<InvalidOperationException>(
            () => node.ApplyConfChange(invalid));
    }

    [Fact]
    public void ArgumentValidationDoesNotFaultFacade()
    {
        var node = CreateNode(CreateStorage());

        Assert.Throws<ArgumentNullException>(
            () => node.ApplyConfChange(
                (ConfChangeV2)null!));

        Assert.False(node.HasReady());
        node.Tick();
    }

    [Fact]
    public void PostInstallationFailureFaultsFacade()
    {
        MemoryStorage storage = CreateStorage();
        var faultingStorage = new FaultingStorage(storage);
        var node = CreateNode(faultingStorage);
        BecomeSingletonLeader(node, storage);
        var change = new ProtocolConfChange
        {
            Type = ConfChangeType.ConfChangeAddNode,
            NodeId = 2,
        };
        node.ProposeConfChange(change);
        Ready append = node.Ready();
        PersistAndAdvance(node, storage, append);
        Ready apply = node.Ready();
        Persist(storage, apply);
        faultingStorage.ThrowOnGetTerm = true;

        Assert.Throws<StorageException>(
            () => node.ApplyConfChange(change));

        Assert.True(
            node.Core.Tracker.Progress.ContainsKey(2));
        Assert.Throws<InvalidOperationException>(
            node.Tick);
        Assert.Throws<InvalidOperationException>(
            () => node.Advance(apply));
    }

    [Fact]
    public void AutoJointChangeProposesOrderedExit()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(storage);
        BecomeSingletonLeader(node, storage);
        var change = new ConfChangeV2
        {
            Transition =
                ConfChangeTransition.JointImplicit,
        };
        change.Changes.Add(new ConfChangeSingle
        {
            Type = ConfChangeType.ConfChangeAddLearnerNode,
            NodeId = 2,
        });

        node.ProposeConfChange(change);
        Ready append = node.Ready();
        PersistAndAdvance(node, storage, append);
        Ready apply = node.Ready();
        Persist(storage, apply);
        Entry committed = Assert.Single(
            apply.CommittedEntries);
        ConfState joint = node.ApplyConfChange(change);
        Assert.True(joint.AutoLeave);
        node.Advance(apply);

        Ready leave = node.Ready();
        Entry leaveEntry = Assert.Single(leave.Entries);
        Assert.Equal(
            EntryType.EntryConfChangeV2,
            leaveEntry.Type);
        Assert.Empty(
            ConfChangeV2.Parser
                .ParseFrom(leaveEntry.Data)
                .Changes);
        Assert.True(leaveEntry.Index > committed.Index);
        PersistAndAdvance(node, storage, leave);

        Ready leaveApply = node.Ready();
        Persist(storage, leaveApply);
        ConfChangeV2 leaveChange =
            ConfChangeV2.Parser.ParseFrom(
                Assert.Single(
                    leaveApply.CommittedEntries).Data);
        ConfState final =
            node.ApplyConfChange(leaveChange);
        node.Advance(leaveApply);

        Assert.False(final.AutoLeave);
        Assert.Empty(final.VotersOutgoing);
        Assert.Equal([1UL], final.Voters);
        Assert.Equal([2UL], final.Learners);
    }
}
