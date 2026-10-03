using DotnetRaft.ConfChange;
using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Tracker;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreSnapshotRestoreTests
{
    [Fact]
    public void ValidSnapshotRestoresOwnedLogConfigurationAndDurableResponse()
    {
        ElectionCoreFixture fixture = Create(
            id: 2,
            voters: [1, 2],
            entries: [EntryAt(1, 1)],
            term: 3);
        RaftCore core = fixture.Core;
        Snapshot snapshot = SnapshotAt(
            index: 5,
            term: 4,
            voters: [1, 2, 3],
            learners: [4],
            data: "state");
        Message message = SnapshotMessage(
            core,
            from: 1,
            messageTerm: 4,
            snapshot);
        Message original = message.Clone();

        core.Step(message);

        Assert.Equal(original, message);
        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(4UL, core.Term);
        Assert.Equal(1UL, core.LeaderId);
        Assert.Equal(5UL, core.Log.Committed);
        Assert.Equal(5UL, core.Log.LastIndex);
        Assert.Equal(6UL, core.Log.FirstIndex);
        Assert.True(core.Log.HasUnstableSnapshot);
        Assert.Equal([1UL, 2UL, 3UL], core.Tracker.VoterNodes());
        Assert.Equal([4UL], core.Tracker.LearnerNodes());
        Assert.False(core.Tracker.IsLearner(core.Id));
        Assert.False(core.Promotable);
        Assert.All(core.Tracker.Progress.Values, progress =>
        {
            Assert.Equal(0UL, progress.Match);
            Assert.Equal(5UL, progress.Next);
        });
        Assert.Empty(core.TakeMessages());
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgAppResp, response.Type);
        Assert.Equal(1UL, response.To);
        Assert.Equal(5UL, response.Index);

        message.Snapshot.Metadata.Index = 99;
        message.Snapshot.Metadata.ConfState.Voters.Clear();
        message.Snapshot.Data = ByteString.CopyFromUtf8("changed");
        Snapshot pending = core.Log.GetNextUnstableSnapshot()!;
        Assert.Equal(5UL, pending.Metadata.Index);
        Assert.Equal([1UL, 2UL, 3UL], pending.Metadata.ConfState.Voters);
        Assert.Equal("state", pending.Data.ToStringUtf8());

        RaftCore restarted = PersistSnapshotAndRestart(
            fixture.Storage,
            core,
            pending,
            applied: 5);

        Assert.Equal(4UL, restarted.Term);
        Assert.Equal(5UL, restarted.Log.Committed);
        Assert.Equal(5UL, restarted.Log.LastIndex);
        Assert.Equal([1UL, 2UL, 3UL], restarted.Tracker.VoterNodes());
        Assert.Equal([4UL], restarted.Tracker.LearnerNodes());
        Assert.True(core.Promotable);
    }

    [Fact]
    public void MatchingSnapshotFastForwardsCommitWithoutReplacingConfiguration()
    {
        ElectionCoreFixture fixture = Create(
            id: 2,
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 1),
                EntryAt(3, 1),
            ],
            term: 3,
            commit: 1,
            applied: 1);
        RaftCore core = fixture.Core;
        Snapshot snapshot = SnapshotAt(
            index: 2,
            term: 1,
            voters: [1, 2, 3]);

        core.Step(SnapshotMessage(
            core,
            from: 1,
            messageTerm: 3,
            snapshot));

        Assert.Equal(2UL, core.Log.Committed);
        Assert.Equal(3UL, core.Log.LastIndex);
        Assert.False(core.Log.HasUnstableSnapshot);
        Assert.Equal([1UL, 2UL], core.Tracker.VoterNodes());
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(2UL, response.Index);

        fixture.Storage.InitialState = new StorageState(
            core.HardState.Clone(),
            core.Tracker.ToConfState());
        var restarted = new RaftCore(
            new RaftConfig
            {
                Id = 2,
                Storage = fixture.Storage,
                Applied = 1,
            },
            _ => 0);

        Assert.Equal(3UL, restarted.Term);
        Assert.Equal(2UL, restarted.Log.Committed);
        Assert.Equal(3UL, restarted.Log.LastIndex);
        Assert.Equal([1UL, 2UL], restarted.Tracker.VoterNodes());
    }

    [Fact]
    public void HigherTermObsoleteSnapshotStillRequiresDurableTerm()
    {
        ElectionCoreFixture fixture = Create(
            id: 2,
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 1),
            ],
            term: 3,
            commit: 2,
            applied: 2);
        RaftCore core = fixture.Core;

        core.Step(SnapshotMessage(
            core,
            from: 1,
            messageTerm: 4,
            SnapshotAt(1, 1, voters: [1, 2])));

        Assert.Equal(4UL, core.Term);
        Assert.Equal(2UL, core.Log.Committed);
        Assert.False(core.Log.HasUnstableSnapshot);
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(2UL, response.Index);

        fixture.Storage.InitialState = new StorageState(
            core.HardState.Clone(),
            core.Tracker.ToConfState());
        var restarted = new RaftCore(
            new RaftConfig
            {
                Id = 2,
                Storage = fixture.Storage,
                Applied = 2,
            },
            _ => 0);

        Assert.Equal(4UL, restarted.Term);
        Assert.Equal(2UL, restarted.Log.Committed);
    }

    [Fact]
    public void SnapshotExcludingLocalNodeIsIgnored()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2],
            entries: [EntryAt(1, 1)],
            term: 3).Core;
        ConfState before = core.Tracker.ToConfState();

        core.Step(SnapshotMessage(
            core,
            from: 1,
            messageTerm: 3,
            SnapshotAt(5, 4, voters: [1, 3])));

        Assert.Equal(1UL, core.Log.LastIndex);
        Assert.Equal(0UL, core.Log.Committed);
        Assert.False(core.Log.HasUnstableSnapshot);
        Assert.True(before.IsEquivalentTo(core.Tracker.ToConfState()));
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(0UL, response.Index);
    }

    [Fact]
    public void InvalidConfigurationFailsBeforeLogOrTrackerMutation()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2],
            entries: [EntryAt(1, 1)],
            term: 3).Core;
        core.Tracker.RecordVote(1, granted: true);
        ConfState beforeConfig = core.Tracker.ToConfState();
        ProgressMap beforeProgress = core.Tracker.Progress.Clone();
        ulong beforeLastIndex = core.Log.LastIndex;
        bool beforeLearner = core.Tracker.IsLearner(core.Id);
        Snapshot snapshot = SnapshotAt(
            5,
            4,
            voters: [1, 2],
            learners: [2]);

        Assert.Throws<ConfigurationChangeException>(
            () => core.Step(SnapshotMessage(
                core,
                from: 1,
                messageTerm: 3,
                snapshot)));

        Assert.Equal(beforeLastIndex, core.Log.LastIndex);
        Assert.False(core.Log.HasUnstableSnapshot);
        Assert.True(beforeConfig.IsEquivalentTo(
            core.Tracker.ToConfState()));
        AssertProgressEqual(beforeProgress, core.Tracker.Progress);
        Assert.True(core.Tracker.Votes.TryGetValue(1, out bool vote));
        Assert.True(vote);
        Assert.Equal(beforeLearner, core.Tracker.IsLearner(core.Id));
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void LogRestoreFailureDoesNotInstallValidatedConfiguration()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2],
            entries: [EntryAt(1, 1)],
            term: 3).Core;
        core.Tracker.RecordVote(1, granted: true);
        ConfState beforeConfig = core.Tracker.ToConfState();
        ProgressMap beforeProgress = core.Tracker.Progress.Clone();
        ulong beforeLastIndex = core.Log.LastIndex;
        bool beforeLearner = core.Tracker.IsLearner(core.Id);
        Snapshot snapshot = SnapshotAt(
            ulong.MaxValue,
            4,
            voters: [1, 2, 3]);

        Assert.Throws<RaftInvariantException>(
            () => core.Step(SnapshotMessage(
                core,
                from: 1,
                messageTerm: 3,
                snapshot)));

        Assert.Equal(beforeLastIndex, core.Log.LastIndex);
        Assert.False(core.Log.HasUnstableSnapshot);
        Assert.True(beforeConfig.IsEquivalentTo(
            core.Tracker.ToConfState()));
        AssertProgressEqual(beforeProgress, core.Tracker.Progress);
        Assert.True(core.Tracker.Votes.TryGetValue(1, out bool vote));
        Assert.True(vote);
        Assert.Equal(beforeLearner, core.Tracker.IsLearner(core.Id));
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Theory]
    [MemberData(nameof(DefaultSnapshotMessages))]
    public void MissingSnapshotFieldsAreNormalizedOnOwnedClone(
        Message message)
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2],
            entries: [EntryAt(1, 1)],
            term: 3).Core;
        Message original = message.Clone();

        core.Step(message);

        Assert.Equal(original, message);
        Assert.Equal(1UL, core.Log.LastIndex);
        Assert.Equal(0UL, core.Log.Committed);
        Assert.False(core.Log.HasUnstableSnapshot);
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(0UL, response.Index);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void SnapshotCanChangeLocalLearnerStatus(bool becomeLearner)
    {
        RaftCore core = becomeLearner
            ? Create(
                id: 3,
                voters: [1, 2, 3],
                term: 3).Core
            : Create(
                id: 3,
                voters: [1, 2],
                learners: [3],
                term: 3).Core;
        Snapshot snapshot = becomeLearner
            ? SnapshotAt(
                5,
                4,
                voters: [1, 2],
                learners: [3])
            : SnapshotAt(
                5,
                4,
                voters: [1, 2, 3]);

        core.Step(SnapshotMessage(
            core,
            from: 1,
            messageTerm: 3,
            snapshot));

        Assert.Equal(becomeLearner, core.Tracker.IsLearner(core.Id));
        Assert.Equal(
            becomeLearner,
            core.Tracker.Progress[3].IsLearner);
    }

    [Fact]
    public void JointConfigurationRestoresEquivalently()
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2],
            term: 3).Core;
        var expected = new ConfState
        {
            Voters = { 2, 3, 4 },
            VotersOutgoing = { 1, 2, 3 },
            Learners = { 5 },
            LearnersNext = { 1 },
            AutoLeave = true,
        };

        core.Step(SnapshotMessage(
            core,
            from: 2,
            messageTerm: 3,
            SnapshotAt(11, 4, expected)));

        Assert.True(expected.IsEquivalentTo(
            core.Tracker.ToConfState()));
        Assert.Equal(
            [1UL, 2UL, 3UL, 4UL],
            core.Tracker.VoterNodes());
        Assert.Equal(11UL, core.Tracker.Progress[1].Next);
    }

    [Theory]
    [InlineData(ElectionTestRole.PreCandidate)]
    [InlineData(ElectionTestRole.Candidate)]
    public void CandidateRolesBecomeFollowerBeforeRestoring(
        ElectionTestRole role)
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2],
            term: 2).Core;
        EnterRole(core, role);

        core.Step(SnapshotMessage(
            core,
            from: 1,
            messageTerm: core.Term,
            SnapshotAt(5, 3, voters: [1, 2])));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(1UL, core.LeaderId);
        Assert.Equal(5UL, core.Log.LastIndex);
        Assert.True(core.Log.HasUnstableSnapshot);
    }

    [Fact]
    public void EqualTermLeaderIgnoresButHigherTermLeaderRestoresSnapshot()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2],
            term: 1).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        core.TakeMessagesAfterAppend();
        ulong leaderTerm = core.Term;
        ulong lastIndex = core.Log.LastIndex;
        Snapshot snapshot = SnapshotAt(
            5,
            2,
            voters: [1, 2]);

        core.Step(SnapshotMessage(
            core,
            from: 1,
            messageTerm: leaderTerm,
            snapshot));

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.Equal(lastIndex, core.Log.LastIndex);
        Assert.Empty(core.TakeMessagesAfterAppend());

        core.Step(SnapshotMessage(
            core,
            from: 1,
            messageTerm: leaderTerm + 1,
            snapshot));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(leaderTerm + 1, core.Term);
        Assert.Equal(5UL, core.Log.LastIndex);
        Assert.True(core.Log.HasUnstableSnapshot);
    }

    public static TheoryData<Message> DefaultSnapshotMessages()
    {
        return new TheoryData<Message>
        {
            new Message
            {
                From = 1,
                To = 2,
                Term = 3,
                Type = MessageType.MsgSnap,
            },
            new Message
            {
                From = 1,
                To = 2,
                Term = 3,
                Type = MessageType.MsgSnap,
                Snapshot = new Snapshot(),
            },
            new Message
            {
                From = 1,
                To = 2,
                Term = 3,
                Type = MessageType.MsgSnap,
                Snapshot = new Snapshot
                {
                    Metadata = new SnapshotMetadata
                    {
                        Index = 5,
                        Term = 4,
                    },
                },
            },
            new Message
            {
                From = 1,
                To = 2,
                Term = 3,
                Type = MessageType.MsgSnap,
                Snapshot = new Snapshot
                {
                    Metadata = new SnapshotMetadata
                    {
                        ConfState = new ConfState
                        {
                            Voters = { 2 },
                        },
                    },
                },
            },
        };
    }

    private static RaftCore PersistSnapshotAndRestart(
        CoreTestStorage storage,
        RaftCore core,
        Snapshot snapshot,
        ulong applied)
    {
        core.Log.AcceptUnstable();
        storage.LogStorage.ApplySnapshot(snapshot);
        storage.InitialState = new StorageState(
            core.HardState.Clone(),
            snapshot.Metadata.ConfState.Clone());
        core.Log.AcknowledgeSnapshot(snapshot.Metadata.Index);

        return new RaftCore(
            new RaftConfig
            {
                Id = core.Id,
                Storage = storage,
                Applied = applied,
            },
            _ => 0);
    }

    private static Snapshot SnapshotAt(
        ulong index,
        ulong term,
        IEnumerable<ulong>? voters = null,
        IEnumerable<ulong>? outgoingVoters = null,
        IEnumerable<ulong>? learners = null,
        IEnumerable<ulong>? learnersNext = null,
        string? data = null)
    {
        var state = new ConfState();
        state.Voters.Add(voters ?? [1UL, 2UL]);
        if (outgoingVoters is not null)
        {
            state.VotersOutgoing.Add(outgoingVoters);
        }

        if (learners is not null)
        {
            state.Learners.Add(learners);
        }

        if (learnersNext is not null)
        {
            state.LearnersNext.Add(learnersNext);
        }

        return SnapshotAt(
            index,
            term,
            state,
            data);
    }

    private static Snapshot SnapshotAt(
        ulong index,
        ulong term,
        ConfState state,
        string? data = null)
    {
        return new Snapshot
        {
            Data = data is null
                ? ByteString.Empty
                : ByteString.CopyFromUtf8(data),
            Metadata = new SnapshotMetadata
            {
                Index = index,
                Term = term,
                ConfState = state.Clone(),
            },
        };
    }

    private static Message SnapshotMessage(
        RaftCore core,
        ulong from,
        ulong messageTerm,
        Snapshot snapshot)
    {
        return new Message
        {
            From = from,
            To = core.Id,
            Term = messageTerm,
            Type = MessageType.MsgSnap,
            Snapshot = snapshot,
        };
    }

    private static void AssertProgressEqual(
        ProgressMap expected,
        ProgressMap actual)
    {
        Assert.Equal(expected.Keys.Order(), actual.Keys.Order());
        foreach ((ulong id, Progress progress) in expected)
        {
            Progress candidate = actual[id];
            Assert.Equal(progress.Match, candidate.Match);
            Assert.Equal(progress.Next, candidate.Next);
            Assert.Equal(progress.State, candidate.State);
            Assert.Equal(progress.PendingSnapshot, candidate.PendingSnapshot);
            Assert.Equal(progress.IsLearner, candidate.IsLearner);
            Assert.Equal(progress.RecentActive, candidate.RecentActive);
        }
    }
}
