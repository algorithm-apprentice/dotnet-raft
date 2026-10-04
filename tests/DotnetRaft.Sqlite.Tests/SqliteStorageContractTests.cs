using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Storage.Sqlite;

using Google.Protobuf;

using static DotnetRaft.Sqlite.Tests.SqliteStorageTestSupport;

namespace DotnetRaft.Sqlite.Tests;

public sealed class SqliteStorageContractTests
{
    [Fact]
    public void EmptyStorageMatchesMemoryContract()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateStorage(directory);

        StorageState state =
            storage.GetInitialState();
        Snapshot snapshot = storage.GetSnapshot();

        Assert.Null(state.HardState);
        Assert.Empty(state.ConfState.Voters);
        Assert.True(state.ConfState.HasAutoLeave);
        Assert.Equal(1UL, storage.GetFirstIndex());
        Assert.Equal(0UL, storage.GetLastIndex());
        Assert.Equal(0UL, storage.GetTerm(0));
        Assert.Null(
            storage.GetPendingApplicationSnapshot());
        Assert.NotNull(snapshot.Metadata);
        Assert.NotNull(snapshot.Metadata.ConfState);
        AssertStorageError(
            StorageError.Unavailable,
            () => storage.GetTerm(1));
    }

    [Fact]
    public void TermAndEntryReadsMatchReferenceSemantics()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateRetainedStorage(
                directory,
                Entries(
                    (3, 3),
                    (4, 4),
                    (5, 5),
                    (6, 6)));

        AssertStorageError(
            StorageError.Compacted,
            () => storage.GetTerm(2));
        Assert.Equal(3UL, storage.GetTerm(3));
        Assert.Equal(6UL, storage.GetTerm(6));
        AssertStorageError(
            StorageError.Unavailable,
            () => storage.GetTerm(7));

        AssertStorageError(
            StorageError.Compacted,
            () => storage.GetEntries(
                3,
                4,
                ulong.MaxValue));
        Assert.Empty(
            storage.GetEntries(
                4,
                4,
                ulong.MaxValue));
        AssertEntries(
            Entries((4, 4), (5, 5), (6, 6)),
            storage.GetEntries(
                4,
                7,
                ulong.MaxValue));
        Assert.Single(
            storage.GetEntries(4, 7, 0));
        Assert.Throws<InvalidOperationException>(
            () => storage.GetEntries(
                5,
                4,
                ulong.MaxValue));
        Assert.Throws<InvalidOperationException>(
            () => storage.GetEntries(
                4,
                8,
                ulong.MaxValue));
    }

    [Fact]
    public void AppendReplacesSuffixAndRejectsGaps()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateRetainedStorage(
                directory,
                Entries((3, 3), (4, 4), (5, 5)));
        storage.SetHardState(
            new HardState
            {
                Term = 6,
            });

        storage.Append(
            Entries((4, 6), (5, 6), (6, 6)));

        Assert.Equal(4UL, storage.GetFirstIndex());
        Assert.Equal(6UL, storage.GetLastIndex());
        AssertEntries(
            Entries((4, 6), (5, 6), (6, 6)),
            storage.GetEntries(
                4,
                7,
                ulong.MaxValue));
        Assert.Throws<InvalidOperationException>(
            () => storage.Append(
                Entries((8, 7))));
    }

    [Fact]
    public void AppendValidationCompactedPrefixesAndOwnershipMatchMemory()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateRetainedStorage(
                directory,
                Entries((3, 3), (4, 4), (5, 5)));

        storage.Append([]);
        storage.Append(
            Entries((1, 1), (2, 2)));
        Assert.Equal(5UL, storage.GetLastIndex());

        Entry[] replacement =
            Entries((2, 3), (3, 3), (4, 8));
        storage.SetHardState(
            new HardState
            {
                Term = 8,
            });
        storage.Append(replacement);
        replacement[^1].Term = 99;
        Assert.Equal(8UL, storage.GetTerm(4));

        IReadOnlyList<Entry> returned =
            storage.GetEntries(
                4,
                5,
                ulong.MaxValue);
        returned[0].Term = 100;
        Assert.Equal(8UL, storage.GetTerm(4));

        Assert.Throws<ArgumentNullException>(
            () => storage.Append(null!));
        Assert.Throws<ArgumentException>(
            () => storage.Append(
                new Entry[] { null! }));
        Assert.Throws<InvalidOperationException>(
            () => storage.Append(
                Entries((5, 1), (7, 1))));
    }

    [Fact]
    public void CompactRetainsDummyTermAndReopens()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using (SqliteStorage storage =
               CreateRetainedStorage(
                   directory,
                   Entries((3, 3), (4, 4), (5, 5))))
        {
            storage.Compact(4);
            Assert.Equal(5UL, storage.GetFirstIndex());
            Assert.Equal(4UL, storage.GetTerm(4));
            AssertStorageError(
                StorageError.Compacted,
                () => storage.Compact(4));
        }

        using SqliteStorage reopened =
            CreateStorage(directory);
        Assert.Equal(5UL, reopened.GetFirstIndex());
        Assert.Equal(5UL, reopened.GetLastIndex());
        Assert.Equal(4UL, reopened.GetTerm(4));
        AssertEntries(
            Entries((5, 5)),
            reopened.GetEntries(
                5,
                6,
                ulong.MaxValue));
    }

    [Fact]
    public void CompactRejectsStaleAndFutureIndexes()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateRetainedStorage(
                directory,
                Entries((3, 3), (4, 4), (5, 5)));

        AssertStorageError(
            StorageError.Compacted,
            () => storage.Compact(3));
        Assert.Throws<InvalidOperationException>(
            () => storage.Compact(6));
        Assert.Equal(4UL, storage.GetFirstIndex());
        Assert.Equal(5UL, storage.GetLastIndex());
    }

    [Fact]
    public void SnapshotCreationAndApplicationOwnValues()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateRetainedStorage(
                directory,
                Entries((3, 3), (4, 4), (5, 5)));
        var confState = new ConfState();
        confState.Voters.Add([1, 2, 3]);
        storage.SetHardState(
            new HardState
            {
                Term = 5,
                Commit = 4,
            });

        Snapshot created = storage.CreateSnapshot(
            4,
            confState,
            ByteString.CopyFromUtf8("local"));
        confState.Voters.Clear();
        created.Metadata.ConfState.Voters.Clear();

        Snapshot retained = storage.GetSnapshot();
        Assert.Equal(4UL, retained.Metadata.Index);
        Assert.Equal([1UL, 2UL, 3UL],
            retained.Metadata.ConfState.Voters);
        Assert.Null(
            storage.GetPendingApplicationSnapshot());

        var incomingConf = new ConfState();
        incomingConf.Voters.Add([4, 5, 6]);
        Snapshot incoming = SnapshotAt(
            6,
            6,
            incomingConf,
            "remote");
        storage.SetHardState(
            new HardState
            {
                Term = 6,
                Commit = 4,
            });
        storage.ApplySnapshot(incoming);
        incoming.Metadata.Index = 9;
        incoming.Metadata.ConfState.Voters.Clear();

        Snapshot pending =
            Assert.IsType<Snapshot>(
                storage.GetPendingApplicationSnapshot());
        Assert.Equal(6UL, pending.Metadata.Index);
        Assert.Equal([4UL, 5UL, 6UL],
            pending.Metadata.ConfState.Voters);
        Assert.Throws<InvalidOperationException>(
            storage.GetInitialState);
        Assert.Throws<InvalidOperationException>(
            () => storage.Append(
                Entries((7, 6))));
        Assert.Equal(
            6UL,
            storage.GetHardState()?.Term);
        storage.SetHardState(
            new HardState
            {
                Term = 6,
                Vote = 4,
                Commit = 6,
            });
        storage.AcknowledgeApplicationSnapshot(6);

        Assert.Null(
            storage.GetPendingApplicationSnapshot());
        Assert.Equal(
            [4UL, 5UL, 6UL],
            storage.GetInitialState()
                .ConfState.Voters);
    }

    [Fact]
    public void SnapshotErrorsAndBootstrapMatchMemory()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateStorage(directory);
        var confState = new ConfState();
        confState.Voters.Add([1, 2, 3]);

        storage.ApplySnapshot(
            SnapshotAt(
                0,
                0,
                confState));
        Assert.Null(
            storage.GetPendingApplicationSnapshot());
        Assert.Equal(
            [1UL, 2UL, 3UL],
            storage.GetInitialState()
                .ConfState.Voters);

        storage.SetHardState(
            new HardState
            {
                Term = 3,
            });
        storage.Append(
            Entries((1, 1), (2, 1), (3, 2)));
        storage.SetHardState(
            new HardState
            {
                Term = 3,
                Commit = 3,
            });
        Snapshot created = storage.CreateSnapshot(
            2,
            null,
            ByteString.CopyFromUtf8("first"));
        Assert.Equal(
            [1UL, 2UL, 3UL],
            created.Metadata.ConfState.Voters);
        AssertStorageError(
            StorageError.SnapshotOutOfDate,
            () => storage.CreateSnapshot(
                2,
                null,
                ByteString.Empty));
        Assert.Throws<InvalidOperationException>(
            () => storage.CreateSnapshot(
                4,
                null,
                ByteString.Empty));

        storage.ApplySnapshot(
            SnapshotAt(
                3,
                2,
                confState,
                "remote"));
        storage.AcknowledgeApplicationSnapshot(
            3);
        AssertStorageError(
            StorageError.SnapshotOutOfDate,
            () => storage.ApplySnapshot(
                SnapshotAt(3, 2, confState)));
    }

    [Fact]
    public void PendingSnapshotAcknowledgementIsExact()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateStorage(directory);
        storage.SetHardState(
            new HardState
            {
                Term = 2,
            });
        storage.ApplySnapshot(
            SnapshotAt(
                4,
                2,
                data: "pending"));

        Assert.Throws<InvalidOperationException>(
            () => storage
                .AcknowledgeApplicationSnapshot(4));
        Assert.Throws<InvalidOperationException>(
            () => storage.SetHardState(
                new HardState
                {
                    Term = 2,
                    Commit = 3,
                }));
        storage.SetHardState(
            new HardState
            {
                Term = 2,
                Vote = 1,
                Commit = 4,
            });
        Assert.Throws<InvalidOperationException>(
            () => storage
                .AcknowledgeApplicationSnapshot(5));
        storage.AcknowledgeApplicationSnapshot(4);
        Assert.Throws<InvalidOperationException>(
            () => storage
                .AcknowledgeApplicationSnapshot(4));
    }

    [Fact]
    public void HardStateAndSnapshotReadsAreDetached()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateStorage(directory);
        var hardState = new HardState
        {
            Term = 2,
            Vote = 1,
        };
        storage.SetHardState(hardState);
        hardState.Term = 9;

        HardState first =
            Assert.IsType<HardState>(
                storage.GetHardState());
        Assert.Equal(2UL, first.Term);
        first.Term = 10;
        Assert.Equal(
            2UL,
            storage.GetHardState()?.Term);

        Snapshot snapshot = storage.GetSnapshot();
        snapshot.Metadata.Index = 10;
        Assert.Equal(
            0UL,
            storage.GetSnapshot()
                .Metadata.Index);
    }

    [Fact]
    public void DisposeIsIdempotentAndRejectsFurtherUse()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        var storage = CreateStorage(directory);

        storage.Dispose();
        storage.Dispose();

        Assert.Throws<ObjectDisposedException>(
            () => storage.GetLastIndex());
    }

    [Fact]
    public void FullUnsignedIndexesPreserveOrdering()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateStorage(directory);
        ulong snapshotIndex =
            ulong.MaxValue - 3;
        storage.SetHardState(
            new HardState
            {
                Term = 7,
            });
        storage.ApplySnapshot(
            SnapshotAt(
                snapshotIndex,
                7));
        storage.SetHardState(
            new HardState
            {
                Term = 7,
                Commit = snapshotIndex,
            });
        storage.AcknowledgeApplicationSnapshot(
            snapshotIndex);
        storage.SetHardState(
            new HardState
            {
                Term = 9,
                Commit = snapshotIndex,
            });
        storage.Append(
            Entries(
                (ulong.MaxValue - 2, 8),
                (ulong.MaxValue - 1, 9)));

        Assert.Equal(
            ulong.MaxValue - 2,
            storage.GetFirstIndex());
        Assert.Equal(
            ulong.MaxValue - 1,
            storage.GetLastIndex());
        AssertEntries(
            Entries(
                (ulong.MaxValue - 2, 8),
                (ulong.MaxValue - 1, 9)),
            storage.GetEntries(
                ulong.MaxValue - 2,
                ulong.MaxValue,
                ulong.MaxValue));
        Assert.Throws<InvalidOperationException>(
            () => storage.Append(
                Entries((ulong.MaxValue, 10))));
    }

    [Fact]
    public async Task ConcurrentReadersObserveAtomicBatches()
    {
        using TemporaryStorageDirectory directory =
            CreateDirectory();
        using SqliteStorage storage =
            CreateStorage(directory);
        storage.SetHardState(
            new HardState
            {
                Term = 101,
            });
        storage.Append(
            Entries((1, 1), (2, 1), (3, 1)));
        using var barrier = new Barrier(3);

        Task writer = Task.Run(() =>
        {
            barrier.SignalAndWait();
            for (ulong term = 2;
                 term < 102;
                 term++)
            {
                storage.Append(
                    Entries(
                        (1, term),
                        (2, term),
                        (3, term)));
            }
        });
        Task firstReader = Task.Run(
            () => ReadBatches(storage, barrier));
        Task secondReader = Task.Run(
            () => ReadBatches(storage, barrier));

        await Task.WhenAll(
            writer,
            firstReader,
            secondReader);
    }

    private static SqliteStorage CreateRetainedStorage(
        TemporaryStorageDirectory directory,
        Entry[] retained)
    {
        var storage = CreateStorage(directory);
        var initial = new List<Entry>();
        for (ulong index = 1;
             index < retained[0].Index;
             index++)
        {
            initial.Add(
                new Entry
                {
                    Index = index,
                });
        }

        initial.AddRange(retained);
        storage.SetHardState(
            new HardState
            {
                Term = retained.Max(
                    entry => entry.Term),
            });
        storage.Append(initial);
        storage.Compact(retained[0].Index);
        return storage;
    }

    private static void ReadBatches(
        SqliteStorage storage,
        Barrier barrier)
    {
        barrier.SignalAndWait();
        for (var iteration = 0;
             iteration < 100;
             iteration++)
        {
            IReadOnlyList<Entry> entries =
                storage.GetEntries(
                    1,
                    4,
                    ulong.MaxValue);
            ulong term = entries[0].Term;
            Assert.All(
                entries,
                entry => Assert.Equal(
                    term,
                    entry.Term));
        }
    }
}
