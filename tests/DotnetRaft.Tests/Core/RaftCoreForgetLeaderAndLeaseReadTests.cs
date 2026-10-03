using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Quorum;
using DotnetRaft.Read;
using DotnetRaft.Tracker;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;
using static DotnetRaft.Tests.Core.RaftCoreReadIndexTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreForgetLeaderAndLeaseReadTests
{
    [Fact]
    public void SafeFollowerForgetsOnlyLeaderIdentity()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2, 3],
            term: 5,
            vote: 3,
            checkQuorum: true,
            preVote: true).Core;
        core.BecomeFollower(5, leaderId: 1);
        core.SetClockElapsedForTesting(4);
        HardState hardState = core.HardState;
        RaftRole role = core.Role;
        int timeout = core.GetClockStateForTesting().RandomizedElectionTimeout;
        Progress progress = core.Tracker.Progress[2];

        core.Step(new Message
        {
            Type = MessageType.MsgForgetLeader,
        });

        Assert.Equal(0UL, core.LeaderId);
        Assert.Equal(hardState, core.HardState);
        Assert.Equal(role, core.Role);
        Assert.Equal(4, core.GetClockStateForTesting().ElectionElapsed);
        Assert.Equal(timeout, core.GetClockStateForTesting().RandomizedElectionTimeout);
        Assert.Same(progress, core.Tracker.Progress[2]);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void ForgetLeaderAllowsRecentPreVote()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2, 3],
            term: 5,
            checkQuorum: true,
            preVote: true).Core;
        core.BecomeFollower(5, leaderId: 1);
        Message request = FuturePreVote(core, from: 3);

        core.Step(request);

        Assert.Empty(core.TakeMessagesAfterAppend());

        core.Step(new Message
        {
            Type = MessageType.MsgForgetLeader,
        });
        core.Step(request);

        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgPreVoteResp, response.Type);
        Assert.False(response.Reject);
    }

    [Fact]
    public void ForgetLeaderDoesNotBypassLogFreshness()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2, 3],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
            ],
            term: 2,
            checkQuorum: true,
            preVote: true).Core;
        core.BecomeFollower(2, leaderId: 1);
        core.Step(new Message
        {
            Type = MessageType.MsgForgetLeader,
        });

        core.Step(new Message
        {
            From = 3,
            To = 2,
            Term = 3,
            Type = MessageType.MsgPreVote,
            Index = 1,
            LogTerm = 1,
        });

        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.True(response.Reject);
        Assert.Equal(2UL, response.Term);
    }

    [Fact]
    public void LeaseFollowerRejectsForgetLeader()
    {
        var logger = new RecordingLogger();
        RaftCore core = Create(
            id: 2,
            voters: [1, 2, 3],
            term: 5,
            checkQuorum: true,
            readOnlyOption: ReadOnlyOption.LeaseBased,
            logger: logger).Core;
        core.BecomeFollower(5, leaderId: 1);

        core.Step(new Message
        {
            Type = MessageType.MsgForgetLeader,
        });

        Assert.Equal(1UL, core.LeaderId);
        Assert.Contains(
            logger.Entries,
            entry =>
                entry.Level == RaftLogLevel.Error
                && entry.Message.Contains(
                    "lease-based",
                    StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData(ElectionTestRole.PreCandidate)]
    [InlineData(ElectionTestRole.Candidate)]
    [InlineData(ElectionTestRole.Leader)]
    public void NonFollowerForgetLeaderIsNoOp(
        ElectionTestRole role)
    {
        RaftCore core = Create(voters: [1, 2, 3]).Core;
        EnterRole(core, role);
        core.TakeMessages();
        core.TakeMessagesAfterAppend();
        SoftState softState = core.SoftState;
        HardState hardState = core.HardState;

        core.Step(new Message
        {
            Type = MessageType.MsgForgetLeader,
        });

        Assert.Equal(softState, core.SoftState);
        Assert.Equal(hardState, core.HardState);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void EligibleLeaseReadAnswersWithoutHeartbeat()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            readOnlyOption: ReadOnlyOption.LeaseBased,
            checkQuorum: true);

        core.Step(ReadRequest("lease"));

        ReadState state = Assert.Single(
            core.TakeReadStates());
        Assert.Equal(1UL, state.Index);
        Assert.Equal(
            "lease",
            state.RequestContext.ToStringUtf8());
        Assert.DoesNotContain(
            core.TakeMessages(),
            message =>
                message.Type ==
                    MessageType.MsgHeartbeat);
        Assert.Equal(0, core.GetReadOnlyPendingCountForTesting());
    }

    [Fact]
    public void RemoteLeaseReadReceivesImmediateResponse()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            readOnlyOption: ReadOnlyOption.LeaseBased,
            checkQuorum: true);

        core.Step(ReadRequest(
            "remote",
            from: 2,
            to: 1));

        Message response = Assert.Single(
            core.TakeMessages());
        Assert.Equal(MessageType.MsgReadIndexResp, response.Type);
        Assert.Equal(2UL, response.To);
        Assert.Equal(1UL, response.Index);
        Assert.Equal(
            "remote",
            Assert.Single(response.Entries)
                .Data.ToStringUtf8());
    }

    [Fact]
    public void GatedLeaseReadsReleaseInFifoOrder()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            commitCurrentTerm: false,
            readOnlyOption: ReadOnlyOption.LeaseBased,
            checkQuorum: true);
        core.Step(ReadRequest("first"));
        core.Step(ReadRequest("second"));

        Assert.Empty(core.TakeReadStates());
        Assert.Empty(core.TakeMessages());

        core.Step(AppResponse(
            core,
            from: 2,
            index: core.Log.LastIndex));

        ReadState[] states = core.TakeReadStates();
        Assert.Equal(
            ["first", "second"],
            states.Select(
                state =>
                    state.RequestContext.ToStringUtf8()));
        Assert.All(
            states,
            state => Assert.Equal(1UL, state.Index));
        Assert.Equal(0, core.GetReadOnlyPendingCountForTesting());
        Assert.DoesNotContain(
            core.TakeMessages(),
            message =>
                message.Type ==
                    MessageType.MsgHeartbeat);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RetainedNonVoterLeaseLeaderUsesSafeRead(
        bool demote)
    {
        RaftCore core = NewLeader(
            voters: [1, 2],
            readOnlyOption: ReadOnlyOption.LeaseBased,
            checkQuorum: true);
        core.ApplyConfigurationChange(
            demote
                ? V2(AddLearner(1))
                : V2(Remove(1)));
        core.TakeMessages();

        core.Step(ReadRequest("safe-fallback"));

        Assert.Empty(core.TakeReadStates());
        Message heartbeat = Assert.Single(
            core.TakeMessages(),
            message =>
                message.Type ==
                    MessageType.MsgHeartbeat);

        core.Step(HeartbeatResponse(
            core,
            from: 2,
            heartbeat.Context));

        Assert.Single(core.TakeReadStates());
    }

    [Fact]
    public void TrackedUnconfiguredLeaderUsesSafeRead()
    {
        RaftCore core = NewLeader(
            voters: [1, 2],
            readOnlyOption: ReadOnlyOption.LeaseBased,
            checkQuorum: true);
        core.Tracker.Install(
            new TrackerConfig(
                new JointConfig(
                    new MajorityConfig([2]))),
            core.Tracker.Progress);

        core.Step(ReadRequest("unconfigured"));

        Assert.Empty(core.TakeReadStates());
        Message heartbeat = Assert.Single(
            core.TakeMessages(),
            message =>
                message.Type ==
                    MessageType.MsgHeartbeat);
        Assert.Equal(2UL, heartbeat.To);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void OutgoingLocalVoterRemainsLeaseEligible(
        bool stagedLearner)
    {
        RaftCore core = NewLeader(
            voters: [2],
            outgoingVoters: [1, 2],
            learnersNext:
                stagedLearner ? [1UL] : null,
            readOnlyOption: ReadOnlyOption.LeaseBased,
            checkQuorum: true);

        core.Step(ReadRequest("outgoing"));

        ReadState state = Assert.Single(
            core.TakeReadStates());
        Assert.Equal(
            "outgoing",
            state.RequestContext.ToStringUtf8());
        Assert.Equal(0, core.GetReadOnlyPendingCountForTesting());
        Assert.DoesNotContain(
            core.TakeMessages(),
            message =>
                message.Type ==
                    MessageType.MsgHeartbeat);
    }

    private static Message FuturePreVote(
        RaftCore core,
        ulong from)
    {
        EntryId last = core.Log.LastEntryId;
        return new Message
        {
            From = from,
            To = core.Id,
            Term = core.Term + 1,
            Type = MessageType.MsgPreVote,
            Index = last.Index,
            LogTerm = last.Term,
        };
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

    private sealed class RecordingLogger : IRaftLogger
    {
        internal List<(RaftLogLevel Level, string Message)> Entries
        {
            get;
        } = [];

        public bool IsEnabled(RaftLogLevel level)
        {
            return true;
        }

        public void Log(
            RaftLogLevel level,
            string message)
        {
            Entries.Add((level, message));
        }
    }
}
