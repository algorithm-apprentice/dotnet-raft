using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Tracker;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreSnapshotSendingTests
{
    [Fact]
    public void CompactedPeerReceivesOwnedSnapshotAndProgressPauses()
    {
        ElectionCoreFixture fixture = NewSnapshotLeader();
        RaftCore core = fixture.Core;
        Progress progress = core.Tracker.Progress[2];

        core.Step(CompactedRejection(core));

        Message snapshotMessage = Assert.Single(
            core.TakeMessages());
        Assert.Equal(MessageType.MsgSnap, snapshotMessage.Type);
        Assert.Equal(2UL, snapshotMessage.To);
        Assert.Equal(core.Term, snapshotMessage.Term);
        Assert.Equal(5UL, snapshotMessage.Snapshot.Metadata.Index);
        Assert.Equal(1UL, snapshotMessage.Snapshot.Metadata.Term);
        Assert.Equal("snapshot", snapshotMessage.Snapshot.Data.ToStringUtf8());
        Assert.Equal(ProgressState.Snapshot, progress.State);
        Assert.Equal(5UL, progress.PendingSnapshot);
        Assert.Equal(6UL, progress.Next);
        Assert.True(progress.IsPaused);

        snapshotMessage.Snapshot.Metadata.Index = 99;
        snapshotMessage.Snapshot.Data =
            ByteString.CopyFromUtf8("changed");
        Snapshot stored = fixture.Storage.LogStorage.GetSnapshot();
        Assert.Equal(5UL, stored.Metadata.Index);
        Assert.Equal("snapshot", stored.Data.ToStringUtf8());
        Assert.Equal(5UL, progress.PendingSnapshot);
    }

    [Fact]
    public void SnapshotStateSuppressesProposalAndHeartbeatAppends()
    {
        ElectionCoreFixture fixture = NewSnapshotLeader();
        RaftCore core = fixture.Core;
        core.Step(CompactedRejection(core));
        core.TakeMessages();

        core.Step(Proposal("pending"));
        Assert.Empty(core.TakeMessages());
        core.TakeMessagesAfterAppend();

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgHeartbeatResp,
        });

        Assert.Empty(core.TakeMessages());
        Assert.Equal(
            ProgressState.Snapshot,
            core.Tracker.Progress[2].State);
    }

    [Fact]
    public void InactiveCompactedPeerDoesNotReceiveSnapshot()
    {
        ElectionCoreFixture fixture = NewSnapshotLeader();
        RaftCore core = fixture.Core;
        Progress progress = core.Tracker.Progress[2];
        Assert.True(progress.MaybeDecrementTo(7, 4));
        Assert.False(progress.RecentActive);

        core.Step(Proposal("pending"));

        Assert.Empty(core.TakeMessages());
        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.Equal(5UL, progress.Next);
    }

    [Fact]
    public void TemporarilyUnavailableSnapshotLeavesProgressRetryable()
    {
        ElectionCoreFixture fixture = NewSnapshotLeader();
        fixture.Storage.GetSnapshotOverride = () =>
            throw new StorageException(
                StorageError.SnapshotTemporarilyUnavailable,
                "retry later");
        Progress progress = fixture.Core.Tracker.Progress[2];

        fixture.Core.Step(CompactedRejection(fixture.Core));

        Assert.True(progress.RecentActive);
        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.Equal(5UL, progress.Next);
        Assert.False(progress.AppendFlowPaused);
        Assert.Empty(fixture.Core.TakeMessages());
    }

    [Fact]
    public void EmptyStorageSnapshotFailsExplicitly()
    {
        ElectionCoreFixture fixture = NewCompactedLeaderWithoutSnapshot();
        RaftCore core = fixture.Core;

        Assert.Throws<RaftInvariantException>(
            () => core.Step(new Message
            {
                From = 2,
                To = 1,
                Term = core.Term,
                Type = MessageType.MsgAppResp,
                Index = 5,
                Reject = true,
                RejectHint = 4,
            }));

        Assert.Empty(core.TakeMessages());
        Assert.Equal(
            ProgressState.Probe,
            core.Tracker.Progress[2].State);
    }

    [Fact]
    public void SuccessfulStatusWaitsForHeartbeatBeforeRetry()
    {
        ElectionCoreFixture fixture = NewSnapshotLeader();
        RaftCore core = fixture.Core;
        Progress progress = core.Tracker.Progress[2];
        core.Step(CompactedRejection(core));
        core.TakeMessages();

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Type = MessageType.MsgSnapStatus,
        });

        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.Equal(6UL, progress.Next);
        Assert.Equal(0UL, progress.PendingSnapshot);
        Assert.True(progress.AppendFlowPaused);
        Assert.Empty(core.TakeMessages());

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgHeartbeatResp,
        });

        Message retry = Assert.Single(core.TakeMessages());
        Assert.Equal(MessageType.MsgApp, retry.Type);
        Assert.Equal(5UL, retry.Index);
        Assert.Equal([6UL, 7UL, 8UL], retry.Entries.Select(
            entry => entry.Index));
        Assert.True(progress.AppendFlowPaused);
    }

    [Fact]
    public void FailedStatusRetriesFromMatchAfterHeartbeat()
    {
        ElectionCoreFixture fixture = NewSnapshotLeader();
        RaftCore core = fixture.Core;
        Progress progress = core.Tracker.Progress[2];
        core.Step(CompactedRejection(core));
        core.TakeMessages();

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Type = MessageType.MsgSnapStatus,
            Reject = true,
        });

        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.Equal(1UL, progress.Next);
        Assert.Equal(0UL, progress.PendingSnapshot);
        Assert.True(progress.AppendFlowPaused);
        Assert.Empty(core.TakeMessages());

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgHeartbeatResp,
        });

        Message snapshot = Assert.Single(core.TakeMessages());
        Assert.Equal(MessageType.MsgSnap, snapshot.Type);
        Assert.Equal(ProgressState.Snapshot, progress.State);
    }

    [Fact]
    public void UnknownAndWrongStateSnapshotReportsAreIgnored()
    {
        ElectionCoreFixture fixture = NewSnapshotLeader();
        RaftCore core = fixture.Core;
        Progress progress = core.Tracker.Progress[2];

        core.Step(new Message
        {
            From = 99,
            To = 1,
            Type = MessageType.MsgSnapStatus,
        });
        core.Step(new Message
        {
            From = 2,
            To = 1,
            Type = MessageType.MsgSnapStatus,
        });

        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void RetainedBoundaryAckBelowPendingSnapshotAbortsSnapshot()
    {
        ElectionCoreFixture fixture = NewSnapshotLeader();
        RaftCore core = fixture.Core;
        Progress progress = core.Tracker.Progress[2];
        progress.BecomeSnapshot(10);

        core.Step(AppResponse(core, index: 5));

        Assert.Equal(ProgressState.Replicate, progress.State);
        Assert.Equal(5UL, progress.Match);
        Assert.Equal(0UL, progress.PendingSnapshot);
        Message append = Assert.Single(core.TakeMessages());
        Assert.Equal(5UL, append.Index);
        Assert.Equal([6UL, 7UL, 8UL], append.Entries.Select(
            entry => entry.Index));
        Assert.Equal(9UL, progress.Next);
        Assert.Equal(1, progress.Inflights.Count);
    }

    [Fact]
    public void AckBelowRetainedBoundaryLeavesSnapshotPaused()
    {
        ElectionCoreFixture fixture = NewSnapshotLeader();
        RaftCore core = fixture.Core;
        Progress progress = core.Tracker.Progress[2];
        progress.BecomeSnapshot(10);

        core.Step(AppResponse(core, index: 4));

        Assert.Equal(ProgressState.Snapshot, progress.State);
        Assert.Equal(4UL, progress.Match);
        Assert.Equal(10UL, progress.PendingSnapshot);
        Assert.True(progress.IsPaused);
        Assert.Empty(core.TakeMessages());
    }

    private static ElectionCoreFixture NewSnapshotLeader()
    {
        var storage = new CoreTestStorage();
        var confState = new ConfState
        {
            Voters = { 1, 2 },
        };
        storage.LogStorage.ApplySnapshot(new Snapshot
        {
            Data = ByteString.CopyFromUtf8("snapshot"),
            Metadata = new SnapshotMetadata
            {
                Index = 5,
                Term = 1,
                ConfState = confState.Clone(),
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
            confState);
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
        Message noOpAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(noOpAck);
        core.TakeMessages();
        return new ElectionCoreFixture(core, storage);
    }

    private static ElectionCoreFixture
        NewCompactedLeaderWithoutSnapshot()
    {
        var storage = new CoreTestStorage();
        storage.LogStorage.Append(
        [
            EntryAt(1, 1),
            EntryAt(2, 1),
            EntryAt(3, 1),
            EntryAt(4, 1),
            EntryAt(5, 1),
        ]);
        storage.LogStorage.Compact(5);
        var confState = new ConfState
        {
            Voters = { 1, 2 },
        };
        storage.InitialState = new StorageState(
            new HardState
            {
                Term = 1,
                Commit = 5,
            },
            confState);
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
        core.Step(Assert.Single(
            core.TakeMessagesAfterAppend()));
        core.TakeMessages();
        return new ElectionCoreFixture(core, storage);
    }

    private static Message CompactedRejection(
        RaftCore core)
    {
        return new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = 7,
            Reject = true,
            RejectHint = 4,
        };
    }

    private static Message AppResponse(
        RaftCore core,
        ulong index)
    {
        return new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = index,
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
