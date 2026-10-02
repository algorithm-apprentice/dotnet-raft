using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using static DotnetRaft.Tests.RawNode.RawNodeTestSupport;

namespace DotnetRaft.Tests.RawNode;

public sealed class RawNodePersistenceTests
{
    [Fact]
    public void RestartEmitsCommittedButUnappliedEntries()
    {
        MemoryStorage storage = CreateStorage();
        storage.Append(
        [
            EntryAt(1, 1),
            EntryAt(2, 1, "later"),
        ]);
        storage.SetHardState(new HardState
        {
            Term = 1,
            Vote = 1,
            Commit = 2,
        });

        var node = CreateNode(storage, applied: 1);
        Ready ready = node.Ready();

        Entry entry = Assert.Single(ready.CommittedEntries);
        Assert.Equal(2UL, entry.Index);
        Assert.Null(ready.HardState);
        Assert.False(ready.MustSync);
    }

    [Fact]
    public void RestartFromSnapshotEmitsOnlyLaterCommittedEntries()
    {
        var storage = new MemoryStorage();
        storage.ApplySnapshot(new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = 2,
                Term = 1,
                ConfState = new ConfState
                {
                    Voters = { 1 },
                },
            },
        });
        storage.Append([EntryAt(3, 1, "later")]);
        storage.SetHardState(new HardState
        {
            Term = 1,
            Commit = 3,
        });

        var node = CreateNode(storage, applied: 2);
        Ready ready = node.Ready();

        Entry entry = Assert.Single(ready.CommittedEntries);
        Assert.Equal(3UL, entry.Index);
        Assert.Null(ready.Snapshot);
    }

    [Fact]
    public void EarlyAdvanceCrashRedeliversPhysicalSuffix()
    {
        MemoryStorage storage = CreateStorage();
        var original = CreateNode(storage);
        original.Campaign();
        Ready election = original.Ready();
        PersistAndAdvance(original, storage, election);
        Ready append = original.Ready();
        PersistAndAdvance(original, storage, append);

        Ready committed = original.Ready();
        Persist(storage, committed);
        original.Advance(committed);
        Assert.False(original.HasReady());

        var restarted = CreateNode(storage, applied: 0);
        Ready replay = restarted.Ready();

        Entry entry = Assert.Single(replay.CommittedEntries);
        Assert.Equal(1UL, entry.Index);
    }

    [Fact]
    public void RecoveryRepairsLostCommitBeforeConstruction()
    {
        MemoryStorage storage = CreateStorage();
        storage.Append(
        [
            EntryAt(1, 1),
            EntryAt(2, 1, "applied"),
        ]);
        storage.SetHardState(new HardState
        {
            Term = 1,
            Vote = 1,
            Commit = 1,
        });

        Assert.Throws<RaftInvariantException>(
            () => CreateNode(storage, applied: 2));

        storage.SetHardState(new HardState
        {
            Term = 1,
            Vote = 1,
            Commit = 2,
        });
        var repaired = CreateNode(storage, applied: 2);

        Assert.False(repaired.HasReady());
    }

    [Fact]
    public void CommittedPaginationHasNoGaps()
    {
        MemoryStorage storage = CreateStorage();
        Entry[] entries =
        [
            EntryAt(1, 1, "a"),
            EntryAt(2, 1, "b"),
            EntryAt(3, 1, "c"),
        ];
        storage.Append(entries);
        storage.SetHardState(new HardState
        {
            Term = 1,
            Commit = 3,
        });
        ulong pageSize = (ulong)entries[0].CalculateSize();
        var node = CreateNode(
            storage,
            maxCommittedSizePerReady: pageSize);
        var delivered = new List<ulong>();

        while (node.HasReady())
        {
            Ready ready = node.Ready();
            delivered.AddRange(
                ready.CommittedEntries.Select(
                    entry => entry.Index));
            node.Advance(ready);
        }

        Assert.Equal([1UL, 2UL, 3UL], delivered);
    }

    [Fact]
    public void PhysicalCursorGuardsSnapshotAndCompaction()
    {
        MemoryStorage storage = CreateStorage();
        storage.Append(
        [
            EntryAt(1, 1),
            EntryAt(2, 1),
        ]);
        storage.SetHardState(new HardState
        {
            Term = 1,
            Commit = 2,
        });
        var node = CreateNode(storage);
        Ready ready = node.Ready();
        var application = new OrderedApplicationHarness();

        Assert.Throws<InvalidOperationException>(
            () => application.CreateSnapshot(
                storage,
                1,
                ByteString.Empty));

        application.Advance(node, ready);
        Snapshot snapshot = application.CreateSnapshot(
            storage,
            2,
            ByteString.CopyFromUtf8("state"));

        Assert.Equal(2UL, snapshot.Metadata.Index);
        Assert.Throws<InvalidOperationException>(
            () => application.Compact(storage, 3));
        application.Compact(storage, 2);
    }

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    public void PersistenceForceDecisionIncludesSnapshots(
        bool mustSync,
        bool hasSnapshot,
        bool expectedForce)
    {
        bool force = mustSync || hasSnapshot;

        Assert.Equal(expectedForce, force);
    }

    [Fact]
    public void SameTermSnapshotDoesNotSetMustSync()
    {
        MemoryStorage storage = CreateStorage(voters: [1, 2]);
        storage.SetHardState(new HardState
        {
            Term = 1,
        });
        var node = CreateNode(storage);
        node.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 1,
            Type = MessageType.MsgSnap,
            Snapshot = new Snapshot
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
            },
        });

        Ready ready = node.Ready();

        Assert.NotNull(ready.Snapshot);
        Assert.NotNull(ready.HardState);
        Assert.False(ready.MustSync);
    }
}
