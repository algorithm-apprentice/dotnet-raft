using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using static DotnetRaft.Tests.RawNode.RawNodeTestSupport;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.RawNode;

public sealed class RawNodeTracingTests
{
    [Fact]
    public void ConstructionEmitsOneDetachedInitializationEvent()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateStorage(voters: [1, 2]),
            traceSink: sink);

        RaftTraceEvent traceEvent =
            Assert.Single(sink.Events);
        Assert.Equal(
            RaftTraceEventType.Initialized,
            traceEvent.Type);
        Assert.Equal(node.GetBasicStatus(), traceEvent.Status);
        Assert.Equal([1UL, 2UL], traceEvent.Configuration.Voters);
        Assert.Equal(0UL, traceEvent.LastLogIndex);
        Assert.Null(traceEvent.Message);
        Assert.Null(traceEvent.Detail);

        node.ApplyConfChange(new ProtocolConfChange
        {
            Type = ConfChangeType.ConfChangeRemoveNode,
            NodeId = 2,
        });
        Assert.Equal([1UL, 2UL], traceEvent.Configuration.Voters);
    }

    [Fact]
    public void BootstrapTraceOrderIsExact()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateEmptyStorage(),
            traceSink: sink);
        sink.Events.Clear();

        node.Bootstrap([new Peer(2), new Peer(1)]);

        Assert.Equal(
        [
            RaftTraceEventType.BecameFollower,
            RaftTraceEventType.EntriesAppended,
            RaftTraceEventType.CommitAdvanced,
            RaftTraceEventType.ConfigurationApplied,
            RaftTraceEventType.ConfigurationApplied,
        ],
            sink.Events.Select(traceEvent => traceEvent.Type));
        Assert.Equal(
            "count:2 first:1 last:2",
            sink.Events[1].Detail);
        Assert.Equal(
            "Voters:[2] VotersOutgoing:[] Learners:[] LearnersNext:[] AutoLeave:false",
            sink.Events[3].Detail);
        Assert.Equal(
            "Voters:[1 2] VotersOutgoing:[] Learners:[] LearnersNext:[] AutoLeave:false",
            sink.Events[4].Detail);
    }

    [Fact]
    public void CampaignTraceOrderIncludesReceiveTransitionAndSend()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateStorage(),
            traceSink: sink);
        sink.Events.Clear();

        node.Campaign();

        Assert.Equal(
        [
            RaftTraceEventType.MessageReceived,
            RaftTraceEventType.BecameCandidate,
            RaftTraceEventType.MessageSent,
        ],
            sink.Events.Select(traceEvent => traceEvent.Type));
        Assert.Equal(
            MessageType.MsgHup,
            sink.Events[0].Message?.Type);
        Assert.Equal(
            MessageType.MsgVoteResp,
            sink.Events[2].Message?.Type);
    }

    [Fact]
    public void PreVoteTraceIncludesPreCandidateTransition()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateStorage(),
            preVote: true,
            traceSink: sink);
        sink.Events.Clear();

        node.Campaign();

        Assert.Equal(
        [
            RaftTraceEventType.MessageReceived,
            RaftTraceEventType.BecamePreCandidate,
            RaftTraceEventType.MessageSent,
        ],
            sink.Events.Select(traceEvent => traceEvent.Type));
        Assert.Equal(
            MessageType.MsgPreVoteResp,
            sink.Events[^1].Message?.Type);
    }

    [Fact]
    public void ReadyAcceptedIsEmittedAfterBatchInstallation()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateStorage(),
            traceSink: sink);
        node.Campaign();
        sink.Events.Clear();

        Ready ready = node.Ready();

        RaftTraceEvent traceEvent =
            Assert.Single(sink.Events);
        Assert.Equal(
            RaftTraceEventType.ReadyAccepted,
            traceEvent.Type);
        Assert.Equal(
            RaftRole.Candidate,
            traceEvent.Status.Role);
        Assert.False(node.HasReady());
        node.Advance(ready);
    }

    [Fact]
    public void MessageTraceOwnsDetachedScalars()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateStorage(voters: [1, 2]),
            traceSink: sink);
        var message = new Message
        {
            From = 2,
            To = 1,
            Type = MessageType.MsgVote,
            Term = 1,
            Commit = 3,
        };

        node.Step(message);
        RaftTraceMessage received =
            sink.Events.Single(
                traceEvent =>
                    traceEvent.Type
                    == RaftTraceEventType.MessageReceived)
                .Message!.Value;
        message.From = 99;
        message.Commit = 0;

        Assert.Equal(2UL, received.From);
        Assert.Equal(3UL, received.Commit);
        Assert.Equal(
            MessageType.MsgVote,
            received.Type);
    }

    [Fact]
    public void TraceCallbackCannotReplaceInboundMessage()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateStorage(voters: [1, 2]),
            traceSink: sink);
        var message = new Message
        {
            From = 2,
            To = 1,
            Type = MessageType.MsgHeartbeat,
            Term = 1,
        };
        sink.OnTrace = traceEvent =>
        {
            if (traceEvent.Type
                == RaftTraceEventType.MessageReceived)
            {
                message.From = 1;
                message.Term = 0;
                message.Type = MessageType.MsgHup;
            }
        };

        node.Step(message);

        BasicStatus status = node.GetBasicStatus();
        Assert.Equal(RaftRole.Follower, status.Role);
        Assert.Equal(1UL, status.Term);
        Assert.Equal(2UL, status.LeaderId);
    }

    [Fact]
    public void AcceptedConfigurationProposalHasExactTraceOrder()
    {
        var sink = new RecordingTraceSink();
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(
            storage,
            traceSink: sink);
        BecomeSingletonLeader(node, storage);
        sink.Events.Clear();

        node.ProposeConfChange(new ProtocolConfChange
        {
            Type =
                ConfChangeType.ConfChangeAddLearnerNode,
            NodeId = 2,
        });

        Assert.Equal(
        [
            RaftTraceEventType.MessageReceived,
            RaftTraceEventType.ConfigurationProposed,
            RaftTraceEventType.EntriesAppended,
            RaftTraceEventType.MessageSent,
        ],
            sink.Events.Select(traceEvent => traceEvent.Type));
        Assert.Equal(
            "transition:ConfChangeTransitionAuto changes:{type:ConfChangeAddLearnerNode node_id:2}",
            sink.Events[1].Detail);
        Assert.Equal(
            "count:1 first:2 last:2",
            sink.Events[2].Detail);
    }

    [Fact]
    public void DroppedConfigurationProposalEmitsNoProposalTrace()
    {
        var sink = new RecordingTraceSink();
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(
            storage,
            maxUncommittedEntriesSize: 1,
            traceSink: sink);
        BecomeSingletonLeader(node, storage);
        node.Propose("x"u8);
        sink.Events.Clear();

        Assert.Throws<ProposalDroppedException>(
            () => node.ProposeConfChange(
                new ProtocolConfChange
                {
                    Type =
                        ConfChangeType.ConfChangeAddLearnerNode,
                    NodeId = 2,
                }));

        Assert.DoesNotContain(
            sink.Events,
            traceEvent =>
                traceEvent.Type
                == RaftTraceEventType.ConfigurationProposed);
        Assert.DoesNotContain(
            sink.Events,
            traceEvent =>
                traceEvent.Type
                == RaftTraceEventType.EntriesAppended);
        Assert.True(node.HasReady());
    }

    [Fact]
    public void FollowerAppendTracesAppendBeforeCommit()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateStorage(voters: [1, 2]),
            traceSink: sink);
        sink.Events.Clear();
        var append = new Message
        {
            From = 2,
            To = 1,
            Type = MessageType.MsgApp,
            Term = 1,
            Commit = 1,
        };
        append.Entries.Add(new Entry
        {
            Term = 1,
            Index = 1,
        });

        node.Step(append);

        RaftTraceEvent appended =
            sink.Events.Single(
                traceEvent =>
                    traceEvent.Type
                    == RaftTraceEventType.EntriesAppended);
        RaftTraceEvent committed =
            sink.Events.Single(
                traceEvent =>
                    traceEvent.Type
                    == RaftTraceEventType.CommitAdvanced);
        Assert.True(
            sink.Events.IndexOf(appended)
            < sink.Events.IndexOf(committed));
        Assert.Equal(0UL, appended.Status.Commit);
        Assert.Equal(1UL, committed.Status.Commit);
    }

    [Fact]
    public void SnapshotRestoreTracesConfigurationBeforeCommit()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateStorage(voters: [1, 2]),
            traceSink: sink);
        sink.Events.Clear();
        var state = new ConfState();
        state.Voters.Add([1UL, 2UL]);

        node.Step(new Message
        {
            From = 2,
            To = 1,
            Type = MessageType.MsgSnap,
            Term = 1,
            Snapshot = new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = 5,
                    Term = 1,
                    ConfState = state,
                },
            },
        });

        RaftTraceEvent configuration =
            sink.Events.Single(
                traceEvent =>
                    traceEvent.Type
                    == RaftTraceEventType.ConfigurationApplied);
        RaftTraceEvent committed =
            sink.Events.Single(
                traceEvent =>
                    traceEvent.Type
                    == RaftTraceEventType.CommitAdvanced);
        Assert.True(
            sink.Events.IndexOf(configuration)
            < sink.Events.IndexOf(committed));
        Assert.Equal(0UL, configuration.Status.Commit);
        Assert.Equal(5UL, committed.Status.Commit);
    }

    [Fact]
    public void ApplyingConfigurationTracesInstalledState()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateStorage(),
            traceSink: sink);
        sink.Events.Clear();

        node.ApplyConfChange(new ProtocolConfChange
        {
            Type =
                ConfChangeType.ConfChangeAddLearnerNode,
            NodeId = 2,
        });

        RaftTraceEvent traceEvent =
            Assert.Single(sink.Events);
        Assert.Equal(
            RaftTraceEventType.ConfigurationApplied,
            traceEvent.Type);
        Assert.Equal(
            "Voters:[1] VotersOutgoing:[] Learners:[2] LearnersNext:[] AutoLeave:false",
            traceEvent.Detail);
    }

    [Fact]
    public void ConstructorCallbackFailureIsExplicit()
    {
        var sink = new RecordingTraceSink
        {
            OnTrace = _ =>
                throw new InvalidOperationException(
                    "trace failed"),
        };

        RaftTracingException exception =
            Assert.Throws<RaftTracingException>(
                () => CreateNode(
                    CreateStorage(),
                    traceSink: sink));

        Assert.IsType<InvalidOperationException>(
            exception.InnerException);
    }

    [Fact]
    public void ReceiveFailureFaultsBeforeTermDispatch()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateStorage(voters: [1, 2]),
            traceSink: sink);
        sink.OnTrace = traceEvent =>
        {
            if (traceEvent.Type
                == RaftTraceEventType.MessageReceived)
            {
                throw new InvalidOperationException(
                    "receive failed");
            }
        };

        Assert.Throws<RaftTracingException>(
            () => node.Step(new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgHeartbeat,
                Term = 5,
            }));

        Assert.Equal(0UL, node.GetBasicStatus().Term);
        Assert.Throws<InvalidOperationException>(
            () => node.HasReady());
    }

    [Theory]
    [InlineData(RaftTraceEventType.BecameCandidate)]
    [InlineData(RaftTraceEventType.MessageSent)]
    public void TransitionOrSendFailureFaultsAfterMutation(
        RaftTraceEventType failureType)
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateStorage(),
            traceSink: sink);
        sink.OnTrace = traceEvent =>
        {
            if (traceEvent.Type == failureType)
            {
                throw new InvalidOperationException(
                    "campaign trace failed");
            }
        };

        Assert.Throws<RaftTracingException>(
            () => node.Campaign());

        BasicStatus status = node.GetBasicStatus();
        Assert.Equal(1UL, status.Term);
        Assert.Equal(RaftRole.Candidate, status.Role);
        Assert.Throws<InvalidOperationException>(
            () => node.HasReady());
    }

    [Fact]
    public void AppendFailureFaultsAfterLogMutation()
    {
        var sink = new RecordingTraceSink();
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(
            storage,
            traceSink: sink);
        BecomeSingletonLeader(node, storage);
        sink.OnTrace = traceEvent =>
        {
            if (traceEvent.Type
                == RaftTraceEventType.EntriesAppended)
            {
                throw new InvalidOperationException(
                    "append trace failed");
            }
        };

        Assert.Throws<RaftTracingException>(
            () => node.Propose("value"u8));

        Assert.Equal(2UL, node.Core.Log.LastIndex);
        Assert.Throws<InvalidOperationException>(
            () => node.HasReady());
    }

    [Fact]
    public void ReadyAcceptedFailureFaultsAndInvalidatesBatch()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateStorage(),
            traceSink: sink);
        node.Campaign();
        sink.OnTrace = traceEvent =>
        {
            if (traceEvent.Type
                == RaftTraceEventType.ReadyAccepted)
            {
                throw new InvalidOperationException(
                    "ready trace failed");
            }
        };

        Assert.Throws<RaftTracingException>(
            () => node.Ready());

        Assert.Equal(
            RaftRole.Candidate,
            node.GetBasicStatus().Role);
        Assert.Throws<InvalidOperationException>(
            () => node.HasReady());
    }

    [Fact]
    public void AdvanceTraceFailureFaultsAndInvalidatesOutstandingBatch()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateStorage(),
            traceSink: sink);
        node.Campaign();
        Ready ready = node.Ready();
        sink.OnTrace = traceEvent =>
        {
            if (traceEvent.Type
                == RaftTraceEventType.MessageReceived)
            {
                throw new InvalidOperationException(
                    "advance trace failed");
            }
        };

        Assert.Throws<RaftTracingException>(
            () => node.Advance(ready));
        Assert.Throws<InvalidOperationException>(
            () => node.Advance(ready));
        _ = node.GetStatus();
    }

    [Fact]
    public void BootstrapTraceFailureFaultsPartialFacade()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateEmptyStorage(),
            traceSink: sink);
        sink.OnTrace = traceEvent =>
        {
            if (traceEvent.Type
                == RaftTraceEventType.EntriesAppended)
            {
                throw new InvalidOperationException(
                    "bootstrap trace failed");
            }
        };

        Assert.Throws<RaftTracingException>(
            () => node.Bootstrap([new Peer(1)]));

        Assert.Equal(1UL, node.GetBasicStatus().Term);
        Assert.Equal(1UL, node.Core.Log.LastIndex);
        Assert.Throws<InvalidOperationException>(
            () => node.HasReady());
    }

    [Fact]
    public void TraceFailureInvalidatesOutstandingReady()
    {
        var sink = new RecordingTraceSink();
        var node = CreateNode(
            CreateStorage(),
            traceSink: sink);
        node.Campaign();
        Ready ready = node.Ready();
        sink.OnTrace = traceEvent =>
        {
            if (traceEvent.Type
                == RaftTraceEventType.MessageReceived)
            {
                throw new InvalidOperationException(
                    "receive failed");
            }
        };

        Assert.Throws<RaftTracingException>(
            () => node.Campaign());
        Assert.Throws<InvalidOperationException>(
            () => node.Advance(ready));
        _ = node.GetStatus();
    }

    [Fact]
    public void CaughtTraceReentryLeavesOuterOperationUsable()
    {
        var sink = new RecordingTraceSink();
        DotnetRaft.RawNode? node = null;
        node = CreateNode(
            CreateStorage(),
            traceSink: sink);
        InvalidOperationException? rejection = null;
        sink.OnTrace = traceEvent =>
        {
            if (traceEvent.Type
                == RaftTraceEventType.MessageReceived)
            {
                rejection =
                    Assert.Throws<InvalidOperationException>(
                        () => node.GetStatus());
            }
        };

        node.Campaign();

        Assert.NotNull(rejection);
        Assert.True(node.HasReady());
    }

    [Fact]
    public void EscapingTraceReentryFaultsFacade()
    {
        var sink = new RecordingTraceSink();
        DotnetRaft.RawNode? node = null;
        node = CreateNode(
            CreateStorage(),
            traceSink: sink);
        sink.OnTrace = traceEvent =>
        {
            if (traceEvent.Type
                == RaftTraceEventType.MessageReceived)
            {
                _ = node.GetStatus();
            }
        };

        Assert.Throws<RaftTracingException>(
            () => node.Campaign());

        Assert.Throws<InvalidOperationException>(
            () => node.HasReady());
        _ = node.GetBasicStatus();
    }

    [Fact]
    public void TraceSinkDoesNotChangeConsensusOutput()
    {
        MemoryStorage tracedStorage = CreateStorage();
        MemoryStorage plainStorage = CreateStorage();
        var traced = CreateNode(
            tracedStorage,
            traceSink: new RecordingTraceSink());
        var plain = CreateNode(plainStorage);

        traced.Campaign();
        plain.Campaign();
        Ready tracedReady = traced.Ready();
        Ready plainReady = plain.Ready();

        Assert.Equal(
            plainReady.SoftState,
            tracedReady.SoftState);
        Assert.Equal(
            plainReady.HardState,
            tracedReady.HardState);
        Assert.Equal(
            plainReady.Entries,
            tracedReady.Entries);
        Assert.Equal(
            plainReady.CommittedEntries,
            tracedReady.CommittedEntries);
        Assert.Equal(
            plainReady.Messages,
            tracedReady.Messages);
        Assert.Equal(
            plainReady.MustSync,
            tracedReady.MustSync);
    }
}
