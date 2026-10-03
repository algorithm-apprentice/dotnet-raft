using System.Buffers.Binary;

using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;
using static DotnetRaft.Tests.Core.RaftCoreReadIndexTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreReadIndexQuorumTests
{
    [Fact]
    public void PeriodicHeartbeatsCarryLatestReadContext()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);
        core.Step(ReadRequest("periodic"));
        ByteString initial = LatestHeartbeatContext(
            core.TakeMessages());

        core.TickLeader();

        Message[] periodic = core.TakeMessages();
        Assert.Equal(2, periodic.Length);
        Assert.All(
            periodic,
            message =>
            {
                Assert.Equal(
                    MessageType.MsgHeartbeat,
                    message.Type);
                Assert.Equal(initial, message.Context);
            });
    }

    [Fact]
    public void UnknownSenderCannotAcknowledgeRead()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);
        core.Step(ReadRequest("unknown"));
        core.TakeMessages();

        core.Step(HeartbeatResponse(
            core,
            from: 99,
            ByteString.CopyFrom([1])));

        Assert.Empty(core.TakeReadStates());
        Assert.Equal(1, core.GetReadOnlyPendingCountForTesting());
    }

    [Theory]
    [InlineData("short")]
    [InlineData("zero")]
    [InlineData("future")]
    public void InvalidKnownSenderContextFailsExplicitly(
        string kind)
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);
        core.Step(ReadRequest("invalid"));
        core.TakeMessages();
        ByteString context = kind switch
        {
            "short" => ByteString.CopyFrom([1]),
            "zero" => ByteString.CopyFrom(new byte[sizeof(ulong)]),
            "future" => EncodePosition(2),
            _ => throw new ArgumentOutOfRangeException(
                nameof(kind),
                kind,
                null),
        };

        Assert.Throws<RaftInvariantException>(
            () => core.Step(HeartbeatResponse(
                core,
                from: 2,
                context)));

        Assert.Empty(core.TakeReadStates());
        Assert.Equal(1, core.GetReadOnlyPendingCountForTesting());
    }

    [Fact]
    public void LearnerAcknowledgementCannotSatisfyVoterQuorum()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            learners: [4]);
        core.Step(ReadRequest("learner"));
        ByteString context = LatestHeartbeatContext(
            core.TakeMessages());

        core.Step(HeartbeatResponse(
            core,
            from: 4,
            context));

        Assert.Empty(core.TakeReadStates());
        Assert.Equal(1, core.GetReadOnlyPendingCountForTesting());

        core.Step(HeartbeatResponse(
            core,
            from: 2,
            context));

        Assert.Single(core.TakeReadStates());
        Assert.Equal(0, core.GetReadOnlyPendingCountForTesting());
    }

    [Fact]
    public void JointConfigurationRequiresBothMajorities()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            outgoingVoters: [1, 4, 5]);
        core.Step(ReadRequest("joint"));
        ByteString context = LatestHeartbeatContext(
            core.TakeMessages());

        core.Step(HeartbeatResponse(
            core,
            from: 2,
            context));

        Assert.Empty(core.TakeReadStates());

        core.Step(HeartbeatResponse(
            core,
            from: 4,
            context));

        ReadState state = Assert.Single(
            core.TakeReadStates());
        Assert.Equal(1UL, state.Index);
        Assert.Equal(
            "joint",
            state.RequestContext.ToStringUtf8());
    }

    [Fact]
    public void LocalOnlyJointConfigurationConfirmsImmediately()
    {
        RaftCore core = NewLeader(
            voters: [1],
            outgoingVoters: [1]);

        core.Step(ReadRequest("local-joint"));

        ReadState state = Assert.Single(
            core.TakeReadStates());
        Assert.Equal(core.Log.Committed, state.Index);
        Assert.Equal(
            "local-joint",
            state.RequestContext.ToStringUtf8());
        Assert.Empty(core.TakeMessages());
        Assert.Equal(0, core.GetReadOnlyPendingCountForTesting());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RetainedNonVoterLeaderDoesNotUseSingletonFastPath(
        bool demote)
    {
        RaftCore core = NewLeader(voters: [1, 2]);
        core.ApplyConfigurationChange(
            demote
                ? V2(AddLearner(1))
                : V2(Remove(1)));
        core.TakeMessages();

        core.Step(ReadRequest("retained"));

        Assert.Empty(core.TakeReadStates());
        Message heartbeat = Assert.Single(
            core.TakeMessages(),
            message =>
                message.Type ==
                    MessageType.MsgHeartbeat);
        Assert.Equal(2UL, heartbeat.To);
        Assert.False(heartbeat.Context.IsEmpty);

        core.Step(HeartbeatResponse(
            core,
            from: 2,
            heartbeat.Context));

        Assert.Single(core.TakeReadStates());
    }

    [Fact]
    public void RemovingVoterReevaluatesActiveReadImmediately()
    {
        RaftCore core = NewLeader(voters: [1, 2]);
        core.Step(ReadRequest("reconfigure"));
        core.TakeMessages();
        Assert.Equal(1, core.GetReadOnlyPendingCountForTesting());

        core.ApplyConfigurationChange(V2(Remove(2)));

        ReadState state = Assert.Single(
            core.TakeReadStates());
        Assert.Equal(1UL, state.Index);
        Assert.Equal(
            "reconfigure",
            state.RequestContext.ToStringUtf8());
        Assert.Equal(0, core.GetReadOnlyPendingCountForTesting());
    }

    [Fact]
    public void MembershipCommitReleasesGatedReadThroughSingletonPath()
    {
        RaftCore core = NewLeader(
            voters: [1, 2],
            commitCurrentTerm: false);
        core.Step(ReadRequest("membership"));
        Assert.Empty(core.TakeMessages());

        core.ApplyConfigurationChange(V2(Remove(2)));

        ReadState state = Assert.Single(
            core.TakeReadStates());
        Assert.Equal(1UL, state.Index);
        Assert.Equal(
            "membership",
            state.RequestContext.ToStringUtf8());
        Assert.Equal(1UL, core.Log.Committed);
        Assert.Empty(core.TakeMessages());
    }

    [Fact]
    public void RoleResetDiscardsActiveHeartbeatConfirmation()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);
        core.Step(ReadRequest("active"));
        core.TakeMessages();
        Assert.Equal(1, core.GetReadOnlyPendingCountForTesting());

        core.BecomeFollower(core.Term, leaderId: 2);

        Assert.Equal(0, core.GetReadOnlyPendingCountForTesting());
        Assert.Empty(core.TakeReadStates());
    }

    [Fact]
    public void GatedReadSurvivesResetUntilNewTermCommit()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            commitCurrentTerm: false);
        core.Step(ReadRequest("survives"));
        core.BecomeFollower(core.Term, leaderId: 2);
        core.BecomeCandidate();
        core.BecomeLeader();
        Message selfAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(selfAck);
        core.TakeMessages();

        core.Step(AppResponse(
            core,
            from: 2,
            index: core.Log.LastIndex));
        ByteString context = LatestHeartbeatContext(
            core.TakeMessages());
        core.Step(HeartbeatResponse(
            core,
            from: 2,
            context));

        ReadState state = Assert.Single(
            core.TakeReadStates());
        Assert.Equal(2UL, state.Index);
        Assert.Equal(
            "survives",
            state.RequestContext.ToStringUtf8());
    }

    [Fact]
    public void LowerTermDelayedAcknowledgementCannotConfirmNewRead()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);
        ulong oldTerm = core.Term;
        core.Step(ReadRequest("old"));
        ByteString oldContext = LatestHeartbeatContext(
            core.TakeMessages());

        core.BecomeFollower(core.Term + 1, leaderId: 2);
        core.BecomeCandidate();
        core.BecomeLeader();
        Message selfAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(selfAck);
        core.TakeMessages();
        core.Step(AppResponse(
            core,
            from: 2,
            index: core.Log.LastIndex));
        core.TakeMessages();

        core.Step(ReadRequest("new"));
        ByteString newContext = LatestHeartbeatContext(
            core.TakeMessages());
        Assert.Equal(oldContext, newContext);

        core.Step(HeartbeatResponse(
            core,
            from: 2,
            oldContext,
            term: oldTerm));

        Assert.Empty(core.TakeReadStates());
        Assert.Equal(1, core.GetReadOnlyPendingCountForTesting());

        core.Step(HeartbeatResponse(
            core,
            from: 2,
            newContext));

        ReadState state = Assert.Single(
            core.TakeReadStates());
        Assert.Equal(
            "new",
            state.RequestContext.ToStringUtf8());
    }

    [Theory]
    [InlineData(StorageError.Compacted)]
    [InlineData(StorageError.Unavailable)]
    public void UnreadableCommittedTermKeepsReadGated(
        StorageError error)
    {
        ElectionCoreFixture fixture = Create(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 1,
            commit: 1);
        RaftCore core = fixture.Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        Message selfAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(selfAck);
        core.TakeMessages();
        fixture.Storage.GetTermOverride =
            _ => throw new StorageException(
                error,
                "unreadable");

        core.Step(ReadRequest("gated"));

        Assert.Empty(core.TakeReadStates());
        Assert.Empty(core.TakeMessages());
        Assert.Equal(0, core.GetReadOnlyPendingCountForTesting());
    }

    [Fact]
    public void UnexpectedCommittedTermStorageErrorPropagates()
    {
        ElectionCoreFixture fixture = Create(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 1,
            commit: 1);
        RaftCore core = fixture.Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        Message selfAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(selfAck);
        core.TakeMessages();
        fixture.Storage.GetTermOverride =
            _ => throw new StorageException(
                StorageError.Unknown,
                "failed");

        StorageException exception =
            Assert.Throws<StorageException>(
                () => core.Step(
                    ReadRequest("failure")));

        Assert.Equal(StorageError.Unknown, exception.Error);
        Assert.Empty(core.TakeReadStates());
        Assert.Empty(core.TakeMessages());
    }

    private static ConfChangeV2 V2(
        ConfChangeSingle change)
    {
        var result = new ConfChangeV2();
        result.Changes.Add(change);
        return result;
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
            Type = ConfChangeType.ConfChangeAddLearnerNode,
            NodeId = id,
        };
    }

    private static ByteString EncodePosition(ulong position)
    {
        var bytes = new byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64LittleEndian(
            bytes,
            position);
        return ByteString.CopyFrom(bytes);
    }
}
