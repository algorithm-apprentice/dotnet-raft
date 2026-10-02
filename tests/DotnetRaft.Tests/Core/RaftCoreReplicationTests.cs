using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Tracker;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreReplicationTests
{
    [Fact]
    public void LeaderActivationAppendsNoOpAndWaitsForDurableSelfAck()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
            ],
            term: 2).Core;
        core.BecomeCandidate();
        ulong oldLastIndex = core.Log.LastIndex;

        core.BecomeLeader();

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.Equal(oldLastIndex + 1, core.Log.LastIndex);
        Assert.Equal(oldLastIndex, core.PendingConfigurationIndex);
        Entry noOp = Assert.Single(
            core.Log.GetEntries(oldLastIndex + 1));
        Assert.Equal(core.Term, noOp.Term);
        Assert.Equal(oldLastIndex + 1, noOp.Index);
        Assert.Equal(EntryType.EntryNormal, noOp.Type);
        Assert.True(noOp.Data.IsEmpty);

        Progress local = core.Tracker.Progress[core.Id];
        Assert.Equal(oldLastIndex, local.Match);
        Assert.Equal(oldLastIndex + 1, local.Next);
        Assert.Equal(0UL, core.Log.Committed);
        Assert.Empty(core.TakeMessages());

        Message durableAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgAppResp, durableAck.Type);
        Assert.Equal(core.Id, durableAck.To);
        Assert.Equal(oldLastIndex + 1, durableAck.Index);

        core.Step(durableAck);

        Assert.Equal(oldLastIndex + 1, local.Match);
        Assert.Equal(oldLastIndex + 2, local.Next);
        Assert.Equal(0UL, core.Log.Committed);
    }

    [Fact]
    public void SingletonCommitsNoOpOnlyAfterItsLaterPersistenceBatch()
    {
        RaftCore core = Create(voters: [1]).Core;
        core.BecomeCandidate();

        core.BecomeLeader();

        Assert.Equal(0UL, core.Log.Committed);
        Message noOpAck = Assert.Single(
            core.TakeMessagesAfterAppend());

        core.Step(noOpAck);

        Assert.Equal(1UL, core.Log.Committed);
        Assert.Equal(1UL, core.Tracker.Progress[1].Match);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void ElectionVictoryBroadcastsNoOpToEveryRemoteProgress()
    {
        RaftCore core = Create(
            voters: [1, 3],
            learners: [2]).Core;
        core.Step(Hup(core.Id));
        core.TakeMessages();
        Message selfVote = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(selfVote);

        core.Step(VoteResponse(core, 3, granted: true));

        Assert.Equal(RaftRole.Leader, core.Role);
        Message[] appends = core.TakeMessages();
        Assert.Equal(
            [2UL, 3UL],
            appends.Select(message => message.To));
        Assert.All(appends, message =>
        {
            Assert.Equal(MessageType.MsgApp, message.Type);
            Assert.Equal(0UL, message.Index);
            Assert.Equal(0UL, message.LogTerm);
            Assert.Equal(0UL, message.Commit);
            Entry noOp = Assert.Single(message.Entries);
            Assert.Equal(1UL, noOp.Index);
            Assert.Equal(1UL, noOp.Term);
            Assert.True(noOp.Data.IsEmpty);
        });

        Message noOpAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(1UL, noOpAck.Index);
    }

    [Fact]
    public void LeaderTransitionIndexExhaustionFailsAtomically()
    {
        var storage = new CoreTestStorage();
        storage.LogStorage.ApplySnapshot(new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = ulong.MaxValue - 1,
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
                Commit = ulong.MaxValue - 1,
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
                Applied = ulong.MaxValue - 1,
            },
            _ => 0);
        core.BecomeCandidate();
        SoftState beforeSoftState = core.SoftState;
        HardState beforeHardState = core.HardState;
        Progress beforeProgress = core.Tracker.Progress[1];

        Assert.Throws<RaftInvariantException>(
            core.BecomeLeader);

        Assert.Equal(beforeSoftState, core.SoftState);
        Assert.Equal(beforeHardState, core.HardState);
        Assert.Equal(ulong.MaxValue - 1, core.Log.LastIndex);
        Assert.Same(beforeProgress, core.Tracker.Progress[1]);
        Assert.Equal(ulong.MaxValue - 1, beforeProgress.Match);
        Assert.Equal(ulong.MaxValue, beforeProgress.Next);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void LeaderProposalClonesEntriesAndHonorsMessageLimit()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 1,
            maxSizePerMessage: 1,
            maxUncommittedEntriesSize: 1);
        var proposal = new Message
        {
            From = 1,
            To = 1,
            Type = MessageType.MsgProp,
        };
        proposal.Entries.Add(new Entry
        {
            Data = ByteString.CopyFromUtf8("first"),
        });
        proposal.Entries.Add(new Entry
        {
            Data = ByteString.CopyFromUtf8("second"),
        });
        Message original = proposal.Clone();

        core.Step(proposal);

        Assert.Equal(original, proposal);
        Entry[] appended = [.. core.Log.GetEntries(3)];
        Assert.Equal(2, appended.Length);
        Assert.Equal([3UL, 4UL], appended.Select(entry => entry.Index));
        Assert.All(
            appended,
            entry => Assert.Equal(core.Term, entry.Term));
        Assert.Equal(EntryType.EntryNormal, appended[0].Type);
        Assert.Equal(EntryType.EntryNormal, appended[1].Type);
        Assert.Equal("first", appended[0].Data.ToStringUtf8());
        Assert.Equal("second", appended[1].Data.ToStringUtf8());
        Assert.Equal(11UL, core.UncommittedSize);

        Message selfAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(4UL, selfAck.Index);

        Message[] appends = core.TakeMessages();
        Assert.Equal([2UL, 3UL], appends.Select(message => message.To));
        Assert.All(appends, message =>
        {
            Assert.Equal(MessageType.MsgApp, message.Type);
            Assert.Equal(1UL, message.Index);
            Assert.Equal(1UL, message.LogTerm);
            Assert.Equal(0UL, message.Commit);
            Assert.Equal(
                [2UL],
                message.Entries.Select(entry => entry.Index));
        });

        proposal.Entries[0].Data = ByteString.CopyFromUtf8("changed");
        Assert.Equal(
            "first",
            core.Log.GetEntries(3, 1)[0].Data.ToStringUtf8());
    }

    [Fact]
    public void EmptyProposalFailsBeforeLogOrQueueMutation()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);
        ulong lastIndex = core.Log.LastIndex;
        HardState hardState = core.HardState;

        Assert.Throws<RaftInvariantException>(
            () => core.Step(new Message
            {
                From = 1,
                To = 1,
                Type = MessageType.MsgProp,
            }));

        Assert.Equal(lastIndex, core.Log.LastIndex);
        Assert.Equal(hardState, core.HardState);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void FollowerForwardsProposalWithoutMutatingCaller()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            term: 5).Core;
        core.BecomeFollower(5, 2);
        var proposal = new Message
        {
            From = 1,
            To = 1,
            Type = MessageType.MsgProp,
        };
        proposal.Entries.Add(new Entry
        {
            Data = ByteString.CopyFromUtf8("forward"),
        });
        Message original = proposal.Clone();

        core.Step(proposal);

        Assert.Equal(original, proposal);
        Message forwarded = Assert.Single(core.TakeMessages());
        Assert.Equal(1UL, forwarded.From);
        Assert.Equal(2UL, forwarded.To);
        Assert.Equal(MessageType.MsgProp, forwarded.Type);
        Assert.Equal(0UL, forwarded.Term);
        Assert.False(forwarded.HasTerm);
        Assert.Equal("forward", forwarded.Entries[0].Data.ToStringUtf8());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Theory]
    [InlineData("follower-no-leader")]
    [InlineData("follower-disabled")]
    [InlineData("candidate")]
    [InlineData("pre-candidate")]
    [InlineData("removed-leader")]
    public void UnavailableProposalPathFailsExplicitly(string scenario)
    {
        RaftCore core = scenario switch
        {
            "follower-no-leader" => Create(
                voters: [1, 2, 3],
                term: 5).Core,
            "follower-disabled" => Create(
                voters: [1, 2, 3],
                term: 5,
                disableProposalForwarding: true).Core,
            "candidate" => Create(
                voters: [1, 2, 3],
                term: 4).Core,
            "pre-candidate" => Create(
                voters: [1, 2, 3],
                term: 5).Core,
            "removed-leader" => NewLeader(
                voters: [1, 2, 3]),
            _ => throw new ArgumentOutOfRangeException(
                nameof(scenario),
                scenario,
                null),
        };
        switch (scenario)
        {
            case "follower-disabled":
                core.BecomeFollower(core.Term, 2);
                break;
            case "candidate":
                core.BecomeCandidate();
                break;
            case "pre-candidate":
                core.BecomePreCandidate();
                break;
            case "removed-leader":
                core.Tracker.Progress.Remove(core.Id);
                break;
        }

        ulong lastIndex = core.Log.LastIndex;

        Assert.Throws<ProposalDroppedException>(
            () => core.Step(Proposal("drop")));

        Assert.Equal(lastIndex, core.Log.LastIndex);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void AcceptedResponseAdvancesProgressCommitsAndBroadcasts()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);
        Progress remote = core.Tracker.Progress[2];

        core.Step(AppResponse(core, 2, index: 1));

        Assert.Equal(1UL, remote.Match);
        Assert.Equal(2UL, remote.Next);
        Assert.True(remote.RecentActive);
        Assert.Equal(1UL, core.Log.Committed);

        Message[] appends = core.TakeMessages();
        Assert.Equal([2UL, 3UL], appends.Select(message => message.To));
        Assert.All(
            appends,
            message => Assert.Equal(1UL, message.Commit));
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void FutureAcknowledgementFailsBeforeProgressMutation()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);
        Progress remote = core.Tracker.Progress[2];
        Assert.False(remote.RecentActive);

        Assert.Throws<RaftInvariantException>(
            () => core.Step(AppResponse(
                core,
                2,
                index: core.Log.LastIndex + 1)));

        Assert.Equal(0UL, remote.Match);
        Assert.Equal(1UL, remote.Next);
        Assert.False(remote.RecentActive);
        Assert.Equal(0UL, core.Log.Committed);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void UnknownAndStaleAcknowledgementsDoNothing()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);
        Progress remote = core.Tracker.Progress[2];

        core.Step(AppResponse(core, 99, index: 1));

        Assert.Equal(0UL, remote.Match);
        Assert.False(remote.RecentActive);
        Assert.Empty(core.TakeMessages());

        core.Step(AppResponse(core, 2, index: 1));
        core.TakeMessages();
        remote.RecentActive = false;
        ulong committed = core.Log.Committed;

        core.Step(AppResponse(core, 2, index: 1));

        Assert.Equal(1UL, remote.Match);
        Assert.True(remote.RecentActive);
        Assert.Equal(committed, core.Log.Committed);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void EarlierSelfAckNeverAttemptsToAppendToSelf()
    {
        RaftCore core = Create(voters: [1, 2, 3]).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        core.Step(Proposal("second-batch"));
        core.TakeMessages();
        Message[] durable = core.TakeMessagesAfterAppend();
        Assert.Equal([1UL, 2UL], durable.Select(message => message.Index));

        core.Step(durable[0]);

        Assert.Equal(1UL, core.Tracker.Progress[1].Match);
        Assert.Equal(2UL, core.Log.LastIndex);
        Assert.Equal(0UL, core.Log.Committed);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());

        core.Step(durable[1]);

        Assert.Equal(2UL, core.Tracker.Progress[1].Match);
        Assert.Empty(core.TakeMessages());
    }

    [Fact]
    public void CurrentTermRuleBlocksOlderEntriesUntilNoOpIsAcknowledged()
    {
        RaftCore core = NewLeader(
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
            ],
            term: 2);

        core.Step(AppResponse(core, 2, index: 1));
        core.TakeMessages();
        Assert.Equal(0UL, core.Log.Committed);

        core.Step(AppResponse(core, 2, index: 2));
        core.TakeMessages();
        Assert.Equal(0UL, core.Log.Committed);

        core.Step(AppResponse(core, 2, index: 3));

        Assert.Equal(3UL, core.Log.Committed);
    }

    [Fact]
    public void JointCommitRequiresBothConstituentMajorities()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            outgoingVoters: [1, 4, 5]);

        core.Step(AppResponse(core, 2, index: 1));
        core.TakeMessages();
        core.Step(AppResponse(core, 3, index: 1));
        core.TakeMessages();

        Assert.Equal(0UL, core.Log.Committed);

        core.Step(AppResponse(core, 4, index: 1));

        Assert.Equal(1UL, core.Log.Committed);
    }

    [Fact]
    public void RejectedProbeBacksUpOneIndexAndStaleOrZeroRejectDoesNothing()
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
        Assert.Equal(4UL, remote.Next);

        core.Step(AppResponse(
            core,
            from: 2,
            index: 3,
            reject: true,
            rejectHint: 2,
            logTerm: 1));

        Assert.Equal(3UL, remote.Next);
        Message retry = Assert.Single(core.TakeMessages());
        Assert.Equal(MessageType.MsgApp, retry.Type);
        Assert.Equal(2UL, retry.Index);
        Assert.Equal(1UL, retry.LogTerm);
        Assert.Equal([3UL, 4UL], retry.Entries.Select(entry => entry.Index));

        core.Step(AppResponse(
            core,
            from: 2,
            index: 3,
            reject: true,
            rejectHint: 2,
            logTerm: 1));
        Assert.Equal(3UL, remote.Next);
        Assert.Empty(core.TakeMessages());

        core.Step(AppResponse(
            core,
            from: 2,
            index: 0,
            reject: true,
            rejectHint: 0,
            logTerm: 0));
        Assert.Equal(3UL, remote.Next);
        Assert.Empty(core.TakeMessages());
    }

    [Fact]
    public void HeartbeatResponseMarksActiveAndSendsCatchUpAppend()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);
        Progress remote = core.Tracker.Progress[2];

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgHeartbeatResp,
        });

        Assert.True(remote.RecentActive);
        Message append = Assert.Single(core.TakeMessages());
        Assert.Equal(MessageType.MsgApp, append.Type);
        Assert.Equal(2UL, append.To);
        Assert.Equal(0UL, append.Index);
        Assert.Single(append.Entries);
    }

    [Fact]
    public void HeartbeatBroadcastBoundsCommitByRemoteMatch()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
            ],
            term: 2);
        core.Log.CommitTo(3);
        core.Tracker.Progress[2].MaybeUpdate(1);

        core.Step(new Message
        {
            From = 1,
            To = 1,
            Type = MessageType.MsgBeat,
        });

        Message[] heartbeats = core.TakeMessages();
        Assert.Equal([2UL, 3UL], heartbeats.Select(message => message.To));
        Assert.Equal([1UL, 0UL], heartbeats.Select(message => message.Commit));
        Assert.All(heartbeats, message =>
        {
            Assert.Equal(MessageType.MsgHeartbeat, message.Type);
            Assert.Equal(0UL, message.Index);
            Assert.Equal(0UL, message.LogTerm);
            Assert.Empty(message.Entries);
            Assert.True(message.Context.IsEmpty);
        });
    }

    [Theory]
    [InlineData(ElectionTestRole.Follower)]
    [InlineData(ElectionTestRole.Candidate)]
    public void MsgBeatIsIgnoredOutsideLeader(ElectionTestRole role)
    {
        RaftCore core = Create(voters: [1, 2, 3]).Core;
        EnterRole(core, role);

        core.Step(new Message
        {
            From = 1,
            To = 1,
            Type = MessageType.MsgBeat,
        });

        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void LeaderHeartbeatClockBroadcastsAtExactCadence()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            electionTick: 5,
            heartbeatTick: 2);

        core.TickLeader();

        Assert.Empty(core.TakeMessages());

        core.TickLeader();

        Assert.Equal(
            [2UL, 3UL],
            core.TakeMessages().Select(message => message.To));
        Assert.Equal(RaftRole.Leader, core.Role);

        core.TickLeader();
        core.TickLeader();
        core.TickLeader();

        Assert.Equal(RaftRole.Leader, core.Role);
    }

    [Fact]
    public void CompactedProgressSendsSnapshot()
    {
        var storage = new CoreTestStorage();
        storage.LogStorage.ApplySnapshot(new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = 5,
                Term = 1,
                ConfState = new ConfState
                {
                    Voters = { 1, 2 },
                },
            },
        });
        storage.LogStorage.Append(
        [
            EntryAt(6, 1),
            EntryAt(7, 1),
        ]);
        storage.InitialState = new StorageState(
            new HardState
            {
                Term = 1,
                Commit = 5,
            },
            new ConfState
            {
                Voters = { 1, 2 },
            });
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
        core.TakeMessagesAfterAppend();
        Progress remote = core.Tracker.Progress[2];
        Assert.True(remote.MaybeDecrementTo(7, 6));
        Assert.True(remote.MaybeDecrementTo(6, 5));
        Assert.True(remote.MaybeDecrementTo(5, 4));
        Assert.Equal(5UL, remote.Next);

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgHeartbeatResp,
        });

        Assert.True(remote.RecentActive);
        Message snapshot = Assert.Single(core.TakeMessages());
        Assert.Equal(MessageType.MsgSnap, snapshot.Type);
        Assert.Equal(2UL, snapshot.To);
        Assert.Equal(5UL, snapshot.Snapshot.Metadata.Index);
        Assert.Equal(ProgressState.Snapshot, remote.State);
        Assert.Equal(5UL, remote.PendingSnapshot);
    }

    [Fact]
    public void UnavailableRetainedPreviousTermPropagates()
    {
        ElectionCoreFixture fixture = Create(
            voters: [1, 2],
            entries: [EntryAt(1, 1)],
            term: 1);
        RaftCore core = fixture.Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        core.TakeMessagesAfterAppend();
        fixture.Storage.GetTermOverride = index =>
            throw new StorageException(
                StorageError.Unavailable,
                $"term {index} unavailable");

        StorageException exception = Assert.Throws<StorageException>(
            () => core.Step(new Message
            {
                From = 2,
                To = 1,
                Term = core.Term,
                Type = MessageType.MsgHeartbeatResp,
            }));

        Assert.Equal(StorageError.Unavailable, exception.Error);
        Assert.Empty(core.TakeMessages());
    }

    [Fact]
    public void ProgressBeyondLastIndexPropagatesUnavailableTerm()
    {
        RaftCore core = NewLeader(
            voters: [1, 2],
            entries: [EntryAt(1, 1)],
            term: 1);
        core.Tracker.Progress[2] = new Progress(
            match: 0,
            next: core.Log.LastIndex + 2,
            maxInflightMessages:
                core.Tracker.MaxInflightMessages,
            maxInflightBytes:
                core.Tracker.MaxInflightBytes);

        StorageException exception = Assert.Throws<StorageException>(
            () => core.Step(new Message
            {
                From = 2,
                To = 1,
                Term = core.Term,
                Type = MessageType.MsgHeartbeatResp,
            }));

        Assert.Equal(StorageError.Unavailable, exception.Error);
        Assert.Empty(core.TakeMessages());
    }

    private static RaftCore NewLeader(
        IEnumerable<ulong> voters,
        IEnumerable<ulong>? outgoingVoters = null,
        IEnumerable<ulong>? learners = null,
        IEnumerable<Entry>? entries = null,
        ulong term = 0,
        int electionTick = 10,
        int heartbeatTick = 1,
        ulong maxSizePerMessage = ulong.MaxValue,
        ulong maxUncommittedEntriesSize = 0,
        bool checkQuorum = false)
    {
        RaftCore core = Create(
            voters: voters,
            outgoingVoters: outgoingVoters,
            learners: learners,
            entries: entries,
            term: term,
            electionTick: electionTick,
            heartbeatTick: heartbeatTick,
            maxSizePerMessage: maxSizePerMessage,
            maxUncommittedEntriesSize:
                maxUncommittedEntriesSize,
            checkQuorum: checkQuorum).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        Message noOpAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(noOpAck);
        core.TakeMessages();
        return core;
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

    private static Message AppResponse(
        RaftCore core,
        ulong from,
        ulong index,
        bool reject = false,
        ulong rejectHint = 0,
        ulong logTerm = 0)
    {
        return new Message
        {
            From = from,
            To = core.Id,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = index,
            Reject = reject,
            RejectHint = rejectHint,
            LogTerm = logTerm,
        };
    }
}
