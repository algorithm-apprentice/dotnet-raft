using DotnetRaft.ConfChange;
using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Tracker;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreInitializationTests
{
    [Fact]
    public void EmptyStorageStartsAsTermZeroFollower()
    {
        var storage = new CoreTestStorage();

        RaftCore core = NewCore(storage);

        Assert.Equal(1UL, core.Id);
        Assert.Equal(0UL, core.Term);
        Assert.Equal(0UL, core.Vote);
        Assert.Equal(0UL, core.LeaderId);
        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.False(core.IsLearner);
        Assert.False(core.Promotable);
        Assert.Equal(new SoftState(0, RaftRole.Follower), core.SoftState);
        Assert.Equal(0UL, core.HardState.Term);
        Assert.Equal(0UL, core.HardState.Vote);
        Assert.Equal(0UL, core.HardState.Commit);
        Assert.Equal(0UL, core.Log.Committed);
        Assert.Equal(0UL, core.Log.Applied);
        Assert.Empty(core.Tracker.Progress);
        Assert.Empty(core.Tracker.Config.Voters.Incoming);
    }

    [Fact]
    public void ExplicitZeroHardStateIsEmptyAboveSnapshotBoundary()
    {
        var storage = new CoreTestStorage();
        storage.LogStorage.ApplySnapshot(SnapshotAt(
            5,
            2,
            voters: [1]));
        storage.InitialState = new StorageState(
            new HardState
            {
                Term = 0,
                Vote = 0,
                Commit = 0,
            },
            ConfState(voters: [1]));

        RaftCore core = NewCore(storage);

        Assert.Equal(0UL, core.Term);
        Assert.Equal(0UL, core.Vote);
        Assert.Equal(5UL, core.Log.Committed);
        Assert.Equal(5UL, core.Log.Applied);
        Assert.Equal(RaftRole.Follower, core.Role);
    }

    [Fact]
    public void RestartRestoresHardStateLogMembershipAndAppliedIndex()
    {
        var storage = new CoreTestStorage();
        storage.LogStorage.ApplySnapshot(SnapshotAt(
            2,
            1,
            voters: [1, 2],
            learners: [3]));
        storage.LogStorage.Append(
        [
            EntryAt(3, 2),
            EntryAt(4, 3),
        ]);
        storage.InitialState = new StorageState(
            new HardState
            {
                Term = 4,
                Vote = 2,
                Commit = 4,
            },
            ConfState(
                voters: [1, 2],
                learners: [3]));

        RaftCore core = NewCore(storage, id: 3, applied: 3);

        Assert.Equal(4UL, core.Term);
        Assert.Equal(2UL, core.Vote);
        Assert.Equal(4UL, core.Log.Committed);
        Assert.Equal(3UL, core.Log.Applied);
        Assert.Equal(3UL, core.Log.FirstIndex);
        Assert.Equal(4UL, core.Log.LastIndex);
        Assert.True(core.IsLearner);
        Assert.False(core.Promotable);
        Assert.True(core.Tracker.Config.Voters.Incoming.SetEquals(
            [1UL, 2UL]));
        Assert.Equal([3UL], core.Tracker.Config.Learners);

        AssertProgress(core, 1, match: 0, next: 5, isLearner: false);
        AssertProgress(core, 2, match: 0, next: 5, isLearner: false);
        AssertProgress(core, 3, match: 4, next: 5, isLearner: true);
    }

    [Fact]
    public void AbsentLocalNodeIsNotLearnerOrPromotable()
    {
        var storage = new CoreTestStorage
        {
            InitialState = new StorageState(
                null,
                ConfState(voters: [2, 3])),
        };

        RaftCore core = NewCore(storage, id: 1);

        Assert.False(core.IsLearner);
        Assert.False(core.Promotable);
        Assert.False(core.Tracker.Progress.ContainsKey(1));
    }

    [Fact]
    public void HardStateCommitBelowSnapshotBoundaryFails()
    {
        var storage = new CoreTestStorage();
        storage.LogStorage.ApplySnapshot(SnapshotAt(
            5,
            2,
            voters: [1]));
        storage.InitialState = new StorageState(
            new HardState
            {
                Term = 3,
                Commit = 4,
            },
            ConfState(voters: [1]));

        Assert.Throws<RaftInvariantException>(
            () => NewCore(storage));
    }

    [Fact]
    public void HardStateCommitAboveLastIndexFails()
    {
        var storage = new CoreTestStorage();
        storage.LogStorage.Append([EntryAt(1, 1)]);
        storage.InitialState = new StorageState(
            new HardState
            {
                Term = 2,
                Commit = 2,
            },
            ConfState(voters: [1]));

        Assert.Throws<RaftInvariantException>(
            () => NewCore(storage));
    }

    [Theory]
    [InlineData(4UL, 5UL)]
    [InlineData(6UL, 5UL)]
    public void AppliedOutsideRetainedCommittedRangeFails(
        ulong applied,
        ulong commit)
    {
        var storage = new CoreTestStorage();
        storage.LogStorage.ApplySnapshot(SnapshotAt(
            5,
            2,
            voters: [1]));
        storage.LogStorage.Append([EntryAt(6, 3)]);
        storage.InitialState = new StorageState(
            new HardState
            {
                Term = 3,
                Commit = commit,
            },
            ConfState(voters: [1]));

        Assert.Throws<RaftInvariantException>(
            () => NewCore(storage, applied: applied));
    }

    [Fact]
    public void MalformedPersistedMembershipFails()
    {
        ConfState malformed = ConfState(
            voters: [1],
            learners: [1]);
        var storage = new CoreTestStorage
        {
            InitialState = new StorageState(null, malformed),
        };

        Assert.Throws<ConfigurationChangeException>(
            () => NewCore(storage));
    }

    [Fact]
    public void NullStorageStateFailsExplicitly()
    {
        var storage = new CoreTestStorage
        {
            InitialState = null,
        };

        Assert.Throws<RaftInvariantException>(
            () => NewCore(storage));
    }

    [Fact]
    public void NullPersistedConfStateFailsExplicitly()
    {
        var storage = new CoreTestStorage
        {
            InitialState = new StorageState(null, null!),
        };

        Assert.Throws<RaftInvariantException>(
            () => NewCore(storage));
    }

    [Fact]
    public void PersistedAndReturnedProtocolStateIsDefensivelyOwned()
    {
        var hardState = new HardState
        {
            Term = 2,
            Vote = 1,
            Commit = 1,
        };
        ConfState confState = ConfState(voters: [1]);
        var storage = new CoreTestStorage
        {
            InitialState = new StorageState(hardState, confState),
        };
        storage.LogStorage.Append([EntryAt(1, 1)]);

        RaftCore core = NewCore(storage);

        hardState.Term = 99;
        hardState.Vote = 99;
        hardState.Commit = 0;
        confState.Voters.Clear();
        confState.Voters.Add(2);

        Assert.Equal(2UL, core.Term);
        Assert.Equal(1UL, core.Vote);
        Assert.Equal(1UL, core.Log.Committed);
        Assert.True(core.Tracker.Config.Voters.Incoming.SetEquals([1UL]));

        HardState returned = core.HardState;
        returned.Term = 77;
        returned.Vote = 77;
        returned.Commit = 0;

        HardState second = core.HardState;
        Assert.Equal(2UL, second.Term);
        Assert.Equal(1UL, second.Vote);
        Assert.Equal(1UL, second.Commit);
    }

    private static RaftCore NewCore(
        CoreTestStorage storage,
        ulong id = 1,
        ulong applied = 0)
    {
        return new RaftCore(
            new RaftConfig
            {
                Id = id,
                Storage = storage,
                Applied = applied,
            },
            _ => 0);
    }

    private static void AssertProgress(
        RaftCore core,
        ulong id,
        ulong match,
        ulong next,
        bool isLearner)
    {
        Progress progress = core.Tracker.Progress[id];
        Assert.Equal(match, progress.Match);
        Assert.Equal(next, progress.Next);
        Assert.Equal(isLearner, progress.IsLearner);
        Assert.Equal(ProgressState.Probe, progress.State);
    }

    private static Entry EntryAt(ulong index, ulong term)
    {
        return new Entry
        {
            Index = index,
            Term = term,
        };
    }

    internal static Snapshot SnapshotAt(
        ulong index,
        ulong term,
        IEnumerable<ulong>? voters = null,
        IEnumerable<ulong>? learners = null)
    {
        return new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = index,
                Term = term,
                ConfState = ConfState(voters, learners),
            },
        };
    }

    internal static ConfState ConfState(
        IEnumerable<ulong>? voters = null,
        IEnumerable<ulong>? learners = null)
    {
        var state = new ConfState();
        if (voters is not null)
        {
            state.Voters.Add(voters);
        }

        if (learners is not null)
        {
            state.Learners.Add(learners);
        }

        return state;
    }
}
