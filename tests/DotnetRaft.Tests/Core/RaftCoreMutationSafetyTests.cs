using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Tracker;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreMutationSafetyTests
{
    [Fact]
    public void CandidateTermOverflowLeavesStateUntouched()
    {
        RaftCore core = Create(
            voters: [1],
            term: ulong.MaxValue).Core;
        BasicStatus before = core.GetBasicStatus();

        RaftInvariantException exception =
            Assert.Throws<RaftInvariantException>(
                core.BecomeCandidate);

        Assert.Equal(
            "Raft term overflow while becoming candidate.",
            exception.Message);
        Assert.Equal(before, core.GetBasicStatus());
    }

    [Fact]
    public void RoleTransitionsEmitTraceBeforeLeaderOutput()
    {
        var trace = new RecordingTraceSink();
        RaftCore core = CreateCoreWithTrace(trace);
        trace.Events.Clear();

        core.BecomeCandidate();
        core.BecomeLeader();

        Assert.Equal(
            RaftTraceEventType.BecameCandidate,
            trace.Events[0].Type);
        Assert.Equal(
            RaftTraceEventType.BecameLeader,
            trace.Events[1].Type);
    }

    [Fact]
    public void DuplicateAppendDoesNotEmitEntriesAppendedTrace()
    {
        var trace = new RecordingTraceSink();
        RaftCore core = CreateCoreWithTrace(trace);
        trace.Events.Clear();

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 1,
            Type = MessageType.MsgApp,
            Index = 0,
            LogTerm = 0,
        });

        Assert.DoesNotContain(
            trace.Events,
            traceEvent =>
                traceEvent.Type
                == RaftTraceEventType.EntriesAppended);
    }

    [Fact]
    public void AppendResponseCommitEmitsCommitTrace()
    {
        var trace = new RecordingTraceSink();
        RaftCore core = CreateCoreWithTrace(trace);
        core.BecomeCandidate();
        core.BecomeLeader();
        Message selfAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(selfAck);
        core.TakeMessages();
        trace.Events.Clear();

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = core.Log.LastIndex,
        });

        Assert.Contains(
            trace.Events,
            traceEvent =>
                traceEvent.Type
                == RaftTraceEventType.CommitAdvanced);
    }

    [Fact]
    public void HeartbeatCommitEmitsCommitTrace()
    {
        var trace = new RecordingTraceSink();
        RaftCore core = CreateFollowerWithTrace(
            trace,
            entries: [EntryAt(1, 1)],
            term: 1,
            commit: 0,
            applied: 0);
        trace.Events.Clear();

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 1,
            Type = MessageType.MsgHeartbeat,
            Commit = 1,
        });

        Assert.Contains(
            trace.Events,
            traceEvent =>
                traceEvent.Type
                == RaftTraceEventType.CommitAdvanced);
    }

    [Fact]
    public void MatchingSnapshotCommitEmitsCommitTrace()
    {
        var trace = new RecordingTraceSink();
        RaftCore core = CreateFollowerWithTrace(
            trace,
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 1),
                EntryAt(3, 1),
                EntryAt(4, 2),
                EntryAt(5, 2),
            ],
            term: 2,
            commit: 3,
            applied: 3);
        var state = new ConfState();
        state.Voters.Add([1UL, 2UL]);
        trace.Events.Clear();

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 2,
            Type = MessageType.MsgSnap,
            Snapshot = new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = 5,
                    Term = 2,
                    ConfState = state,
                },
            },
        });

        Assert.Contains(
            trace.Events,
            traceEvent =>
                traceEvent.Type
                == RaftTraceEventType.CommitAdvanced);
    }

    [Fact]
    public void ImmediateSelfMessageIsRejectedBeforeQueueMutation()
    {
        RaftCore core = Create(voters: [1]).Core;

        Assert.Throws<RaftInvariantException>(
            () => core.Send(new Message
            {
                To = 1,
                Type = MessageType.MsgHeartbeat,
            }));

        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void ExactUncommittedReductionClearsQuota()
    {
        RaftCore core = Create(voters: [1]).Core;
        core.SetUncommittedSizeForTesting(7);

        core.ReduceUncommittedSize(7);

        Assert.Equal(0UL, core.UncommittedSize);
    }

    [Fact]
    public void ZeroIndexRejectionPreservesSentCommit()
    {
        RaftCore core = NewLeader(voters: [1, 2]);
        Progress remote = core.Tracker.Progress[2];
        remote.RecordSentCommit(5);

        core.Step(Rejected(
            core,
            index: 0,
            rejectHint: 0,
            logTerm: 0));

        Assert.Equal(5UL, remote.LastSentCommit);
        Assert.Empty(core.TakeMessages());
    }

    [Fact]
    public void ZeroTermRejectionUsesRawHint()
    {
        RaftCore core = NewLeader(
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 1),
                EntryAt(3, 1),
            ],
            term: 1);
        Progress remote = core.Tracker.Progress[2];

        core.Step(Rejected(
            core,
            index: 3,
            rejectHint: 2,
            logTerm: 0));

        Assert.Equal(3UL, remote.Next);
        Message retry = Assert.Single(core.TakeMessages());
        Assert.Equal(2UL, retry.Index);
        Assert.Equal(
            [3UL, 4UL],
            retry.Entries.Select(entry => entry.Index));
    }

    [Fact]
    public void ReplicateRejectionSendsEmptyProbe()
    {
        RaftCore core = NewLeader(voters: [1, 2]);
        Progress remote = core.Tracker.Progress[2];
        Assert.True(
            remote.MaybeUpdate(core.Log.LastIndex));
        remote.BecomeReplicate();

        core.Step(Rejected(
            core,
            index: core.Log.LastIndex + 1,
            rejectHint: core.Log.LastIndex,
            logTerm: core.Term));

        Assert.Equal(ProgressState.Probe, remote.State);
        Message probe = Assert.Single(core.TakeMessages());
        Assert.Equal(core.Log.LastIndex, probe.Index);
        Assert.Empty(probe.Entries);
    }

    [Fact]
    public void DuplicateReplicateAcknowledgementDoesNotFreeInflight()
    {
        RaftCore core = NewLeader(voters: [1, 2]);
        Progress remote = core.Tracker.Progress[2];
        Assert.True(
            remote.MaybeUpdate(core.Log.LastIndex));
        remote.BecomeReplicate();
        remote.Inflights.Add(
            core.Log.LastIndex,
            bytes: 1);

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = core.Log.LastIndex,
        });

        Assert.Equal(1, remote.Inflights.Count);
        Assert.Empty(core.TakeMessages());
    }

    [Fact]
    public void SnapshotAcknowledgementBoundsSentCommit()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3, 4],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 1),
                EntryAt(3, 1),
                EntryAt(4, 1),
                EntryAt(5, 1),
            ],
            term: 1);
        Progress remote = core.Tracker.Progress[2];
        remote.BecomeSnapshot(10);
        remote.RecordSentCommit(20);

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = core.Log.LastIndex,
        });

        Assert.Equal(ProgressState.Replicate, remote.State);
        Assert.Equal(10UL, remote.LastSentCommit);
    }

    [Fact]
    public void HeartbeatRecordsCommitBoundedByRemoteMatch()
    {
        RaftCore core = NewLeader(
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 1),
            ],
            term: 1);
        core.Log.CommitTo(core.Log.LastIndex);
        Progress remote = core.Tracker.Progress[2];
        Assert.True(remote.MaybeUpdate(1));

        core.Step(new Message
        {
            From = 1,
            To = 1,
            Type = MessageType.MsgBeat,
        });

        Message heartbeat = Assert.Single(core.TakeMessages());
        Assert.Equal(1UL, heartbeat.Commit);
        Assert.Equal(1UL, remote.LastSentCommit);
    }

    [Fact]
    public void TemporarySnapshotUnavailabilityStopsRetryLoop()
    {
        var state = new ConfState();
        state.Voters.Add([1UL, 2UL]);
        var storage = new CoreTestStorage();
        storage.LogStorage.ApplySnapshot(
            new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = 5,
                    Term = 1,
                    ConfState = state,
                },
            });
        storage.LogStorage.Append([EntryAt(6, 1)]);
        storage.InitialState = new StorageState(
            new HardState
            {
                Term = 1,
                Commit = 5,
            },
            state);
        storage.GetSnapshotOverride = () =>
            throw new StorageException(
                StorageError.SnapshotTemporarilyUnavailable,
                "retry later");
        var core = new RaftCore(
            new RaftConfig
            {
                Id = 1,
                Storage = storage,
                Applied = 5,
            },
            _ => 0);
        core.BecomeCandidate();
        core.BecomeLeader();
        Message selfAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(selfAck);
        core.TakeMessages();

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = 0,
        });

        Assert.Empty(core.TakeMessages());
        Assert.Equal(
            ProgressState.Replicate,
            core.Tracker.Progress[2].State);
    }

    [Fact]
    public void SnapshotRestoreResetsRecordedVotes()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2],
            term: 1).Core;
        core.Tracker.RecordVote(1, granted: true);
        var state = new ConfState();
        state.Voters.Add([1UL, 2UL]);

        core.Step(new Message
        {
            From = 1,
            To = 2,
            Term = 1,
            Type = MessageType.MsgSnap,
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

        Assert.Empty(core.Tracker.Votes);
    }

    [Fact]
    public void StorageApplyResponseReleasesUncommittedQuota()
    {
        Entry applied = EntryAt(3, 1);
        applied.Data = ByteString.CopyFromUtf8("applied");
        RaftCore core = Create(
            voters: [1],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 1),
                applied,
            ],
            term: 1,
            commit: 3,
            applied: 2).Core;
        core.SetUncommittedSizeForTesting(
            EntrySizing.PayloadSize([applied]));
        var response = new Message
        {
            From = RaftLocalMessageTargets.ApplyThread,
            To = 1,
            Type = MessageType.MsgStorageApplyResp,
        };
        response.Entries.Add(applied);

        core.Step(response);

        Assert.Equal(0UL, core.UncommittedSize);
        Assert.Equal(3UL, core.Log.Applied);
    }

    private static RaftCore NewLeader(
        IEnumerable<ulong> voters,
        IEnumerable<Entry>? entries = null,
        ulong term = 0)
    {
        RaftCore core = Create(
            voters: voters,
            entries: entries,
            term: term).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        Message selfAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(selfAck);
        core.TakeMessages();
        return core;
    }

    private static Message Rejected(
        RaftCore core,
        ulong index,
        ulong rejectHint,
        ulong logTerm)
    {
        return new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = index,
            Reject = true,
            RejectHint = rejectHint,
            LogTerm = logTerm,
        };
    }

    private static RaftCore CreateCoreWithTrace(
        IRaftTraceSink trace)
    {
        return CreateFollowerWithTrace(
            trace,
            entries: [],
            term: 0,
            commit: 0,
            applied: 0);
    }

    private static RaftCore CreateFollowerWithTrace(
        IRaftTraceSink trace,
        IEnumerable<Entry> entries,
        ulong term,
        ulong commit,
        ulong applied)
    {
        var state = new ConfState();
        state.Voters.Add([1UL, 2UL]);
        var storage = new CoreTestStorage
        {
            InitialState =
                new StorageState(
                    new HardState
                    {
                        Term = term,
                        Commit = commit,
                    },
                    state),
        };
        storage.LogStorage.Append(entries);
        return new RaftCore(
            new RaftConfig
            {
                Id = 1,
                Storage = storage,
                Applied = applied,
                TraceSink = trace,
            },
            _ => 0);
    }

    private sealed class RecordingTraceSink : IRaftTraceSink
    {
        internal List<RaftTraceEvent> Events { get; } = [];

        public void Trace(RaftTraceEvent traceEvent)
        {
            Events.Add(traceEvent);
        }
    }
}
