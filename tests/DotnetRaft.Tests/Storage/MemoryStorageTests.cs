using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

namespace DotnetRaft.Tests.Storage;

public sealed class MemoryStorageTests
{
    [Fact]
    public void EmptyStorageHasExpectedInitialStateAndIndexes()
    {
        var storage = new MemoryStorage();

        StorageState state = storage.GetInitialState();
        Snapshot snapshot = storage.GetSnapshot();

        Assert.Null(state.HardState);
        Assert.Empty(state.ConfState.Voters);
        Assert.True(state.ConfState.HasAutoLeave);
        Assert.Equal(1UL, storage.GetFirstIndex());
        Assert.Equal(0UL, storage.GetLastIndex());
        Assert.Equal(0UL, storage.GetTerm(0));
        Assert.NotNull(snapshot.Metadata);
        Assert.NotNull(snapshot.Metadata.ConfState);
        Assert.True(snapshot.Metadata.HasIndex);
        Assert.True(snapshot.Metadata.HasTerm);
        Assert.True(snapshot.Metadata.ConfState.HasAutoLeave);
        AssertStorageError(StorageError.Unavailable, () => storage.GetTerm(1));
    }

    [Fact]
    public void GetTermMatchesReferenceMatrix()
    {
        var storage = CreateStorage(Entries((3, 3), (4, 4), (5, 5)));
        var cases = new[]
        {
            new TermCase(2, StorageError.Compacted, 0),
            new TermCase(3, null, 3),
            new TermCase(4, null, 4),
            new TermCase(5, null, 5),
            new TermCase(6, StorageError.Unavailable, 0),
        };

        foreach (TermCase testCase in cases)
        {
            if (testCase.Error.HasValue)
            {
                AssertStorageError(testCase.Error.Value, () => storage.GetTerm(testCase.Index));
                continue;
            }

            Assert.Equal(testCase.Term, storage.GetTerm(testCase.Index));
        }
    }

    [Fact]
    public void GetEntriesMatchesReferenceMatrix()
    {
        var storedEntries = Entries((3, 3), (4, 4), (5, 5), (6, 6));
        var storage = CreateStorage(storedEntries);
        ulong firstTwoSize = (ulong)(storedEntries[1].CalculateSize() + storedEntries[2].CalculateSize());
        ulong allSize = firstTwoSize + (ulong)storedEntries[3].CalculateSize();
        var cases = new[]
        {
            new EntriesCase(2, 6, ulong.MaxValue, StorageError.Compacted, null),
            new EntriesCase(3, 4, ulong.MaxValue, StorageError.Compacted, null),
            new EntriesCase(4, 5, ulong.MaxValue, null, Entries((4, 4))),
            new EntriesCase(4, 6, ulong.MaxValue, null, Entries((4, 4), (5, 5))),
            new EntriesCase(4, 7, ulong.MaxValue, null, Entries((4, 4), (5, 5), (6, 6))),
            new EntriesCase(4, 7, 0, null, Entries((4, 4))),
            new EntriesCase(4, 7, firstTwoSize, null, Entries((4, 4), (5, 5))),
            new EntriesCase(
                4,
                7,
                firstTwoSize + ((ulong)storedEntries[3].CalculateSize() / 2),
                null,
                Entries((4, 4), (5, 5))),
            new EntriesCase(4, 7, allSize - 1, null, Entries((4, 4), (5, 5))),
            new EntriesCase(4, 7, allSize, null, Entries((4, 4), (5, 5), (6, 6))),
        };

        foreach (EntriesCase testCase in cases)
        {
            if (testCase.Error.HasValue)
            {
                AssertStorageError(
                    testCase.Error.Value,
                    () => storage.GetEntries(testCase.Low, testCase.High, testCase.MaxSize));
                continue;
            }

            AssertEntries(
                testCase.Entries!,
                storage.GetEntries(testCase.Low, testCase.High, testCase.MaxSize));
        }
    }

    [Fact]
    public void GetEntriesValidatesBoundsAndEmptyRanges()
    {
        var storage = CreateStorage(Entries((3, 3), (4, 4), (5, 5), (6, 6)));

        Assert.Empty(storage.GetEntries(4, 4, ulong.MaxValue));
        Assert.Empty(storage.GetEntries(7, 7, ulong.MaxValue));
        AssertStorageError(
            StorageError.Compacted,
            () => storage.GetEntries(3, 3, ulong.MaxValue));
        Assert.Throws<RaftInvariantException>(
            () => storage.GetEntries(5, 4, ulong.MaxValue));
        Assert.Throws<RaftInvariantException>(
            () => storage.GetEntries(4, 8, ulong.MaxValue));

        var emptyStorage = new MemoryStorage();
        AssertStorageError(
            StorageError.Unavailable,
            () => emptyStorage.GetEntries(1, 1, ulong.MaxValue));
    }

    [Fact]
    public void FirstAndLastIndexesTrackAppendAndCompaction()
    {
        var storage = CreateStorage(Entries((3, 3), (4, 4), (5, 5)));

        Assert.Equal(4UL, storage.GetFirstIndex());
        Assert.Equal(5UL, storage.GetLastIndex());

        storage.Append(Entries((6, 5)));
        Assert.Equal(6UL, storage.GetLastIndex());

        storage.Compact(4);
        Assert.Equal(5UL, storage.GetFirstIndex());
        Assert.Equal(6UL, storage.GetLastIndex());
    }

    [Fact]
    public void CompactMatchesReferenceMatrix()
    {
        var cases = new[]
        {
            new CompactCase(2, StorageError.Compacted, Entries((3, 3), (4, 4), (5, 5))),
            new CompactCase(3, StorageError.Compacted, Entries((3, 3), (4, 4), (5, 5))),
            new CompactCase(4, null, Entries((4, 4), (5, 5))),
            new CompactCase(5, null, Entries((5, 5))),
        };

        foreach (CompactCase testCase in cases)
        {
            var storage = CreateStorage(Entries((3, 3), (4, 4), (5, 5)));

            if (testCase.Error.HasValue)
            {
                AssertStorageError(testCase.Error.Value, () => storage.Compact(testCase.Index));
            }
            else
            {
                storage.Compact(testCase.Index);
            }

            AssertStoredEntries(testCase.Entries, storage);
        }
    }

    [Fact]
    public void CompactRejectsIndexesAfterTheLog()
    {
        var storage = CreateStorage(Entries((3, 3), (4, 4), (5, 5)));

        Assert.Throws<RaftInvariantException>(() => storage.Compact(6));
    }

    [Fact]
    public void AppendMatchesReferenceMatrix()
    {
        var cases = new[]
        {
            new AppendCase(
                Entries((1, 1), (2, 2)),
                Entries((3, 3), (4, 4), (5, 5))),
            new AppendCase(
                Entries((3, 3), (4, 4), (5, 5)),
                Entries((3, 3), (4, 4), (5, 5))),
            new AppendCase(
                Entries((3, 3), (4, 6), (5, 6)),
                Entries((3, 3), (4, 6), (5, 6))),
            new AppendCase(
                Entries((3, 3), (4, 4), (5, 5), (6, 5)),
                Entries((3, 3), (4, 4), (5, 5), (6, 5))),
            new AppendCase(
                Entries((2, 3), (3, 3), (4, 5)),
                Entries((3, 3), (4, 5))),
            new AppendCase(
                Entries((4, 5)),
                Entries((3, 3), (4, 5))),
            new AppendCase(
                Entries((6, 5)),
                Entries((3, 3), (4, 4), (5, 5), (6, 5))),
        };

        foreach (AppendCase testCase in cases)
        {
            var storage = CreateStorage(Entries((3, 3), (4, 4), (5, 5)));

            storage.Append(testCase.Incoming);

            AssertStoredEntries(testCase.Expected, storage);
        }
    }

    [Fact]
    public void AppendValidatesInputAndRejectsGaps()
    {
        var storage = new MemoryStorage();
        IEnumerable<Entry> entriesWithNull = new Entry[] { null! };

        storage.Append([]);

        Assert.Equal(0UL, storage.GetLastIndex());
        Assert.Throws<ArgumentNullException>(() => storage.Append(null!));
        Assert.Throws<ArgumentException>(() => storage.Append(entriesWithNull));
        Assert.Throws<RaftInvariantException>(
            () => storage.Append(Entries((1, 1), (3, 1))));
        Assert.Throws<RaftInvariantException>(
            () => storage.Append(Entries((2, 1))));
    }

    [Fact]
    public void CreateSnapshotMatchesReferenceMatrix()
    {
        var configuration = new ConfState();
        configuration.Voters.Add([1, 2, 3]);
        ByteString data = ByteString.CopyFromUtf8("data");

        foreach ((ulong index, ulong term) in new[] { (4UL, 4UL), (5UL, 5UL) })
        {
            var storage = CreateStorage(Entries((3, 3), (4, 4), (5, 5)));

            Snapshot snapshot = storage.CreateSnapshot(index, configuration, data);

            Assert.Equal(index, snapshot.Metadata.Index);
            Assert.Equal(term, snapshot.Metadata.Term);
            Assert.Equal(configuration, snapshot.Metadata.ConfState);
            Assert.Equal(data, snapshot.Data);
        }
    }

    [Fact]
    public void CreateSnapshotValidatesRetainedBoundsAndStaleness()
    {
        var storage = CreateStorage(Entries((3, 3), (4, 4), (5, 5)));

        Assert.Throws<RaftInvariantException>(
            () => storage.CreateSnapshot(2, null, ByteString.Empty));
        Assert.Throws<RaftInvariantException>(
            () => storage.CreateSnapshot(6, null, ByteString.Empty));

        storage.CreateSnapshot(4, null, ByteString.Empty);

        AssertStorageError(
            StorageError.SnapshotOutOfDate,
            () => storage.CreateSnapshot(4, null, ByteString.Empty));
        AssertStorageError(
            StorageError.SnapshotOutOfDate,
            () => storage.CreateSnapshot(2, null, ByteString.Empty));
    }

    [Fact]
    public void CreateSnapshotClonesConfigurationAndReturnedSnapshots()
    {
        var storage = CreateStorage(Entries((3, 3), (4, 4), (5, 5)));
        var configuration = new ConfState();
        configuration.Voters.Add([1, 2, 3]);

        Snapshot created = storage.CreateSnapshot(
            4,
            configuration,
            ByteString.CopyFromUtf8("first"));
        configuration.Voters.Clear();
        created.Metadata.ConfState.Voters.Clear();
        created.Metadata.Term = 99;

        Snapshot stored = storage.GetSnapshot();
        Assert.Equal([1UL, 2UL, 3UL], stored.Metadata.ConfState.Voters);
        Assert.Equal(4UL, stored.Metadata.Term);

        Snapshot next = storage.CreateSnapshot(
            5,
            null,
            ByteString.CopyFromUtf8("second"));
        next.Metadata.ConfState.Voters.Clear();

        Assert.Equal([1UL, 2UL, 3UL], storage.GetSnapshot().Metadata.ConfState.Voters);
    }

    [Fact]
    public void ApplySnapshotMatchesReferenceBehavior()
    {
        var configuration = new ConfState();
        configuration.Voters.Add([1, 2, 3]);
        var storage = new MemoryStorage();

        storage.ApplySnapshot(SnapshotAt(4, 4, configuration));

        Assert.Equal(5UL, storage.GetFirstIndex());
        Assert.Equal(4UL, storage.GetLastIndex());
        Assert.Equal(4UL, storage.GetTerm(4));
        AssertStorageError(
            StorageError.Unavailable,
            () => storage.GetEntries(5, 5, ulong.MaxValue));
        Assert.Equal(configuration, storage.GetInitialState().ConfState);

        AssertStorageError(
            StorageError.SnapshotOutOfDate,
            () => storage.ApplySnapshot(SnapshotAt(3, 3, configuration)));
        AssertStorageError(
            StorageError.SnapshotOutOfDate,
            () => storage.ApplySnapshot(SnapshotAt(4, 4, configuration)));
        AssertStorageError(
            StorageError.SnapshotOutOfDate,
            () => storage.ApplySnapshot(SnapshotAt(0, 0, configuration)));
    }

    [Fact]
    public void ApplySnapshotAllowsZeroIndexBootstrapOnFreshStorage()
    {
        var configuration = new ConfState();
        configuration.Voters.Add([1, 2, 3]);
        var storage = new MemoryStorage();

        storage.ApplySnapshot(SnapshotAt(0, 0, configuration));

        Assert.Equal(configuration, storage.GetInitialState().ConfState);
        Assert.Equal(0UL, storage.GetLastIndex());
        Assert.Equal(1UL, storage.GetFirstIndex());
    }

    [Fact]
    public void ApplySnapshotClonesBeforeNormalizingOrRetaining()
    {
        var emptySnapshot = new Snapshot();
        var emptyStorage = new MemoryStorage();

        emptyStorage.ApplySnapshot(emptySnapshot);

        Assert.Null(emptySnapshot.Metadata);
        Assert.NotNull(emptyStorage.GetSnapshot().Metadata);
        Assert.NotNull(emptyStorage.GetSnapshot().Metadata.ConfState);

        var configuration = new ConfState();
        configuration.Voters.Add(1);
        Snapshot input = SnapshotAt(4, 4, configuration);
        var storage = new MemoryStorage();

        storage.ApplySnapshot(input);
        input.Metadata.Index = 9;
        input.Metadata.Term = 9;
        input.Metadata.ConfState.Voters.Clear();

        Snapshot stored = storage.GetSnapshot();
        Assert.Equal(4UL, stored.Metadata.Index);
        Assert.Equal(4UL, stored.Metadata.Term);
        Assert.Equal([1UL], stored.Metadata.ConfState.Voters);

        stored.Metadata.Index = 10;
        Assert.Equal(4UL, storage.GetSnapshot().Metadata.Index);
    }

    [Fact]
    public void HardStateIsClonedOnInputAndOutput()
    {
        var storage = new MemoryStorage();
        var input = new HardState
        {
            Term = 2,
            Vote = 1,
            Commit = 3,
        };

        storage.SetHardState(input);
        input.Term = 9;

        StorageState firstRead = storage.GetInitialState();
        Assert.NotNull(firstRead.HardState);
        Assert.Equal(2UL, firstRead.HardState.Term);

        firstRead.HardState.Term = 10;
        Assert.Equal(2UL, storage.GetInitialState().HardState!.Term);
    }

    [Fact]
    public void EntriesAreClonedOnInputAndOutput()
    {
        var storage = new MemoryStorage();
        Entry[] input = Entries((1, 1), (2, 1));

        storage.Append(input);
        input[0].Term = 9;

        IReadOnlyList<Entry> firstRead = storage.GetEntries(1, 3, ulong.MaxValue);
        Assert.Equal(1UL, firstRead[0].Term);

        firstRead[0].Term = 10;
        Assert.Equal(1UL, storage.GetEntries(1, 2, ulong.MaxValue)[0].Term);
    }

    [Fact]
    public void MaximumIndexIsRejectedWithoutMutatingStorage()
    {
        var snapshotStorage = new MemoryStorage();

        Assert.Throws<RaftInvariantException>(
            () => snapshotStorage.ApplySnapshot(SnapshotAt(ulong.MaxValue, 1)));
        Assert.Equal(0UL, snapshotStorage.GetLastIndex());
        Assert.Equal(1UL, snapshotStorage.GetFirstIndex());

        var appendStorage = new MemoryStorage();
        appendStorage.ApplySnapshot(SnapshotAt(ulong.MaxValue - 1, 1));

        Assert.Throws<RaftInvariantException>(
            () => appendStorage.Append(Entries((ulong.MaxValue, 1))));
        Assert.Equal(ulong.MaxValue - 1, appendStorage.GetLastIndex());
        Assert.Equal(ulong.MaxValue, appendStorage.GetFirstIndex());
        Assert.Equal(1UL, appendStorage.GetTerm(ulong.MaxValue - 1));
    }

    [Fact]
    public async Task ConcurrentReadersAndWriterObserveAtomicEntryBatches()
    {
        var storage = new MemoryStorage();
        storage.Append(Entries((1, 1), (2, 1), (3, 1)));
        using var barrier = new Barrier(3);

        Task writer = Task.Run(() =>
        {
            barrier.SignalAndWait();
            for (ulong term = 2; term < 202; term++)
            {
                storage.Append(Entries((1, term), (2, term), (3, term)));
            }
        });
        Task firstReader = Task.Run(() => ReadEntryBatches(storage, barrier));
        Task secondReader = Task.Run(() => ReadEntryBatches(storage, barrier));

        await Task.WhenAll(writer, firstReader, secondReader);
    }

    private static void ReadEntryBatches(MemoryStorage storage, Barrier barrier)
    {
        barrier.SignalAndWait();
        for (int iteration = 0; iteration < 200; iteration++)
        {
            IReadOnlyList<Entry> entries = storage.GetEntries(1, 4, ulong.MaxValue);
            Assert.Equal(3, entries.Count);
            Assert.Equal([1UL, 2UL, 3UL], entries.Select(entry => entry.Index));

            ulong term = entries[0].Term;
            Assert.All(entries, entry => Assert.Equal(term, entry.Term));
        }
    }

    private static MemoryStorage CreateStorage(Entry[] retainedEntries)
    {
        Assert.NotEmpty(retainedEntries);

        var storage = new MemoryStorage();
        Entry dummyEntry = retainedEntries[0];

        if (dummyEntry.Index == 0)
        {
            storage.Append(retainedEntries.Skip(1));
            return storage;
        }

        var initialEntries = new List<Entry>();
        for (ulong index = 1; index < dummyEntry.Index; index++)
        {
            initialEntries.Add(new Entry
            {
                Index = index,
            });
        }

        initialEntries.AddRange(retainedEntries);
        storage.Append(initialEntries);
        storage.Compact(dummyEntry.Index);
        return storage;
    }

    private static void AssertStoredEntries(
        Entry[] expected,
        MemoryStorage storage)
    {
        Assert.NotEmpty(expected);
        Assert.Equal(expected[0].Index + 1, storage.GetFirstIndex());
        Assert.Equal(expected[^1].Index, storage.GetLastIndex());
        Assert.Equal(expected[0].Term, storage.GetTerm(expected[0].Index));

        if (expected.Length == 1)
        {
            AssertStorageError(
                StorageError.Unavailable,
                () => storage.GetEntries(
                    storage.GetFirstIndex(),
                    storage.GetFirstIndex(),
                    ulong.MaxValue));
            return;
        }

        AssertEntries(
            expected.Skip(1).ToArray(),
            storage.GetEntries(expected[1].Index, expected[^1].Index + 1, ulong.MaxValue));
    }

    private static void AssertEntries(
        Entry[] expected,
        IReadOnlyList<Entry> actual)
    {
        Assert.Equal(expected.Length, actual.Count);
        for (int index = 0; index < expected.Length; index++)
        {
            Assert.Equal(expected[index], actual[index]);
        }
    }

    private static void AssertStorageError(StorageError error, Action action)
    {
        StorageException exception = Assert.Throws<StorageException>(action);
        Assert.Equal(error, exception.Error);
    }

    private static Snapshot SnapshotAt(
        ulong index,
        ulong term,
        ConfState? confState = null)
    {
        return new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = index,
                Term = term,
                ConfState = confState?.Clone() ?? new ConfState(),
            },
        };
    }

    private static Entry[] Entries(params (ulong Index, ulong Term)[] values)
    {
        return values.Select(value => new Entry
        {
            Index = value.Index,
            Term = value.Term,
        }).ToArray();
    }

    private sealed record TermCase(
        ulong Index,
        StorageError? Error,
        ulong Term);

    private sealed record EntriesCase(
        ulong Low,
        ulong High,
        ulong MaxSize,
        StorageError? Error,
        Entry[]? Entries);

    private sealed record CompactCase(
        ulong Index,
        StorageError? Error,
        Entry[] Entries);

    private sealed record AppendCase(
        Entry[] Incoming,
        Entry[] Expected);
}
