using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreSafetyGapTests
{
    [Fact]
    public void DefaultRandomizedConstructorUsesValidRange()
    {
        var core = new RaftCore(
            CreateConfig(electionTick: 5));

        Assert.InRange(
            core.RandomizedElectionTimeout,
            5,
            9);
    }

    [Fact]
    public void CampaignWrapperStartsOrdinaryElection()
    {
        RaftCore core = CreateCore();

        core.Campaign();

        Assert.Equal(RaftRole.Candidate, core.Role);
        Assert.Equal(2UL, core.Term);
        Assert.Equal(1UL, core.Vote);
    }

    [Fact]
    public void DeterministicTimeoutHookRejectsNonpositiveValue()
    {
        RaftCore core = CreateCore();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => core
                .SetRandomizedElectionTimeoutForTesting(0));
    }

    [Fact]
    public void ElectionClockRejectsLeaderRole()
    {
        RaftCore core = CreateLeader();

        Assert.Throws<RaftInvariantException>(
            () => core.TickElectionClock());
    }

    [Fact]
    public void MembershipReevaluationRebroadcastsPendingRead()
    {
        RaftCore core = CreateLeader();
        AcknowledgeLeaderNoOp(core);
        core.TakeMessages();
        core.TakeMessagesAfterAppend();

        var request = new Message
        {
            From = 1,
            To = 1,
            Type = MessageType.MsgReadIndex,
        };
        request.Entries.Add(
            new Entry
            {
                Data =
                    ByteString.CopyFromUtf8("read"),
            });
        core.Step(request);
        core.TakeMessages();
        Assert.Equal(1, core.ReadOnly.PendingCount);

        core.ApplyConfigurationChange(
            new ProtocolConfChange
            {
                Type =
                    ConfChangeType.ConfChangeAddLearnerNode,
                NodeId = 4,
            });

        Message[] messages = core.TakeMessages();
        Assert.Equal(
            [2UL, 3UL, 4UL],
            messages
                .Where(message =>
                    message.Type
                    == MessageType.MsgHeartbeat)
                .Select(message => message.To)
                .Order());
    }

    [Fact]
    public void V2ConfigurationProposalEmitsTraceDetail()
    {
        var trace = new RecordingSink();
        RaftCore core = CreateLeader(trace: trace);
        trace.Events.Clear();
        var change = new ConfChangeV2();
        change.Changes.Add(
            new ConfChangeSingle
            {
                Type =
                    ConfChangeType.ConfChangeAddLearnerNode,
                NodeId = 4,
            });
        var proposal = new Message
        {
            From = 1,
            To = 1,
            Type = MessageType.MsgProp,
        };
        proposal.Entries.Add(
            new Entry
            {
                Type =
                    EntryType.EntryConfChangeV2,
                Data = change.ToByteString(),
            });

        core.Step(proposal);

        RaftTraceEvent traceEvent =
            Assert.Single(
                trace.Events,
                item =>
                    item.Type
                    == RaftTraceEventType.ConfigurationProposed);
        Assert.Contains(
            "ConfChangeAddLearnerNode",
            traceEvent.Detail,
            StringComparison.Ordinal);
    }

    [Fact]
    public void AutoLeaveDropUsesDebugLogger()
    {
        var logger = new RecordingLogger();
        RaftCore core = CreateLeader(logger: logger);
        core.Tracker.Config.AutoLeave = true;
        core.PendingConfigurationIndex =
            core.Log.Applied;
        core.LeaderTransferee = 2;

        core.AppliedTo(core.Log.Applied, 0);

        Assert.Contains(
            logger.Events,
            item =>
                item.Level == RaftLogLevel.Debug
                && item.Message.Contains(
                    "Automatic joint exit remains pending",
                    StringComparison.Ordinal));
    }

    [Fact]
    public void EmptyStorageApplyResponseIsNoOp()
    {
        RaftCore core = CreateCore();
        BasicStatus before = core.GetBasicStatus();

        core.Step(
            new Message
            {
                From =
                    RaftLocalMessageTargets.ApplyThread,
                To = 1,
                Type =
                    MessageType.MsgStorageApplyResp,
                Term = 0,
            });

        Assert.Equal(before, core.GetBasicStatus());
    }

    private static RaftCore CreateLeader(
        IRaftTraceSink? trace = null,
        IRaftLogger? logger = null)
    {
        RaftCore core = CreateCore(trace, logger);
        core.BecomeCandidate();
        core.BecomeLeader();
        return core;
    }

    private static void AcknowledgeLeaderNoOp(
        RaftCore core)
    {
        Message self = Assert.Single(
            core.TakeMessagesAfterAppend(),
            message =>
                message.Type == MessageType.MsgAppResp
                && message.From == 1);
        core.Step(self);
        core.Step(
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgAppResp,
                Term = core.Term,
                Index = core.Log.LastIndex,
            });
    }

    private static RaftCore CreateCore(
        IRaftTraceSink? trace = null,
        IRaftLogger? logger = null)
    {
        return new RaftCore(
            CreateConfig(trace: trace, logger: logger),
            _ => 0);
    }

    private static RaftConfig CreateConfig(
        int electionTick = 10,
        IRaftTraceSink? trace = null,
        IRaftLogger? logger = null)
    {
        var state = new ConfState();
        state.Voters.Add([1UL, 2UL, 3UL]);
        var storage = new MemoryStorage();
        storage.ApplySnapshot(
            new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = 2,
                    Term = 1,
                    ConfState = state,
                },
            });
        storage.SetHardState(
            new HardState
            {
                Term = 1,
                Commit = 2,
            });
        return new RaftConfig
        {
            Id = 1,
            ElectionTick = electionTick,
            HeartbeatTick = 1,
            Storage = storage,
            Applied = 2,
            TraceSink = trace,
            Logger = logger,
        };
    }

    private sealed class RecordingSink : IRaftTraceSink
    {
        internal List<RaftTraceEvent> Events { get; } = [];

        public void Trace(RaftTraceEvent traceEvent)
        {
            Events.Add(traceEvent);
        }
    }

    private sealed class RecordingLogger : IRaftLogger
    {
        internal List<(
            RaftLogLevel Level,
            string Message)> Events
        { get; } = [];

        public bool IsEnabled(RaftLogLevel level)
        {
            return true;
        }

        public void Log(
            RaftLogLevel level,
            string message)
        {
            Events.Add((level, message));
        }
    }
}
