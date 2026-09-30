using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

namespace DotnetRaft.Tests.Core;

public sealed class RaftLogTests
{
    [Fact]
    public void ConstructorRecoversStorageBoundaryAndCursors()
    {
        MemoryStorage storage = StorageWithSnapshot(
            3,
            1,
            Entries((4, 2)));

        var log = new RaftLog(storage, NullRaftLogger.Instance);

        Assert.Equal(4UL, log.FirstIndex);
        Assert.Equal(4UL, log.LastIndex);
        Assert.Equal(3UL, log.Committed);
        Assert.Equal(3UL, log.Applying);
        Assert.Equal(3UL, log.Applied);
        Assert.Equal(5UL, log.Unstable.Offset);
    }

    [Fact]
    public void FindConflictMatchesReferenceMatrix()
    {
        var log = new RaftLog(new MemoryStorage(), NullRaftLogger.Instance);
        log.Append(Entries((1, 1), (2, 2), (3, 3)));
        var cases = new[]
        {
            new ConflictCase([], 0),
            new ConflictCase(Entries((1, 1), (2, 2), (3, 3)), 0),
            new ConflictCase(Entries((2, 2), (3, 3)), 0),
            new ConflictCase(Entries((3, 3)), 0),
            new ConflictCase(Entries((1, 1), (2, 2), (3, 3), (4, 4), (5, 4)), 4),
            new ConflictCase(Entries((2, 2), (3, 3), (4, 4), (5, 5)), 4),
            new ConflictCase(Entries((4, 4), (5, 4)), 4),
            new ConflictCase(Entries((1, 4), (2, 4)), 1),
            new ConflictCase(Entries((2, 1), (3, 4), (4, 4)), 2),
            new ConflictCase(Entries((3, 1), (4, 2), (5, 4)), 3),
        };

        foreach (ConflictCase testCase in cases)
        {
            Assert.Equal(testCase.Expected, log.FindConflict(testCase.Entries));
        }
    }

    [Fact]
    public void FindConflictByTermMatchesReferenceBehavior()
    {
        var cases = new[]
        {
            new ConflictByTermCase(
                Entries((0, 0), (1, 2), (2, 2), (3, 5), (4, 5), (5, 5)),
                100,
                2,
                new EntryId(Term: 0, Index: 100)),
            new ConflictByTermCase(
                Entries((0, 0), (1, 2), (2, 2), (3, 5), (4, 5), (5, 5)),
                5,
                4,
                new EntryId(Term: 2, Index: 2)),
            new ConflictByTermCase(
                Entries((0, 0), (1, 2), (2, 2), (3, 5), (4, 5), (5, 5)),
                5,
                1,
                new EntryId(Term: 0, Index: 0)),
            new ConflictByTermCase(
                Entries((10, 3), (11, 3), (12, 3), (13, 4), (14, 4), (15, 4)),
                14,
                3,
                new EntryId(Term: 3, Index: 12)),
            new ConflictByTermCase(
                Entries((10, 3), (11, 3), (12, 3), (13, 4), (14, 4), (15, 4)),
                10,
                2,
                new EntryId(Term: 0, Index: 9)),
            new ConflictByTermCase(
                Entries((10, 3), (11, 3), (12, 3), (13, 4), (14, 4), (15, 4)),
                4,
                2,
                new EntryId(Term: 0, Index: 4)),
        };

        foreach (ConflictByTermCase testCase in cases)
        {
            RaftLog log = LogFromRetainedEntries(testCase.Entries);

            Assert.Equal(
                testCase.Expected,
                log.FindConflictByTerm(testCase.Index, testCase.Term));
        }
    }

    [Fact]
    public void IsUpToDateComparesTermBeforeIndex()
    {
        var log = new RaftLog(new MemoryStorage(), NullRaftLogger.Instance);
        log.Append(Entries((1, 1), (2, 2), (3, 3)));

        Assert.True(log.IsUpToDate(new EntryId(Term: 4, Index: 2)));
        Assert.True(log.IsUpToDate(new EntryId(Term: 4, Index: 4)));
        Assert.False(log.IsUpToDate(new EntryId(Term: 2, Index: 4)));
        Assert.False(log.IsUpToDate(new EntryId(Term: 3, Index: 2)));
        Assert.True(log.IsUpToDate(new EntryId(Term: 3, Index: 3)));
        Assert.True(log.IsUpToDate(new EntryId(Term: 3, Index: 4)));
    }

    [Fact]
    public void AppendReplacesOnlyTheConflictingSuffix()
    {
        var cases = new[]
        {
            new AppendCase([], 2, Entries((1, 1), (2, 2)), 3),
            new AppendCase(Entries((3, 2)), 3, Entries((1, 1), (2, 2), (3, 2)), 3),
            new AppendCase(Entries((1, 2)), 1, Entries((1, 2)), 1),
            new AppendCase(Entries((2, 3), (3, 3)), 3, Entries((1, 1), (2, 3), (3, 3)), 2),
        };

        foreach (AppendCase testCase in cases)
        {
            var storage = new MemoryStorage();
            storage.Append(Entries((1, 1), (2, 2)));
            var log = new RaftLog(storage, NullRaftLogger.Instance);

            ulong lastIndex = log.Append(testCase.Incoming);

            Assert.Equal(testCase.ExpectedLastIndex, lastIndex);
            AssertEntries(testCase.Expected, log.GetEntries(1));
            Assert.Equal(testCase.ExpectedUnstableOffset, log.Unstable.Offset);
        }
    }

    [Fact]
    public void MaybeAppendMatchesAndCommitsSafely()
    {
        var log = new RaftLog(new MemoryStorage(), NullRaftLogger.Instance);
        log.Append(Entries((1, 1), (2, 2), (3, 3)));
        log.CommitTo(1);

        bool matched = log.MaybeAppend(
            new LogSlice(
                4,
                new EntryId(Term: 3, Index: 3),
                Entries((4, 4), (5, 4))),
            5,
            out ulong lastNewIndex);

        Assert.True(matched);
        Assert.Equal(5UL, lastNewIndex);
        Assert.Equal(5UL, log.Committed);
        AssertEntries(
            Entries((1, 1), (2, 2), (3, 3), (4, 4), (5, 4)),
            log.GetEntries(1));

        Assert.False(log.MaybeAppend(
            new LogSlice(
                5,
                new EntryId(Term: 2, Index: 3),
                Entries((4, 5))),
            5,
            out lastNewIndex));
        Assert.Equal(0UL, lastNewIndex);
    }

    [Fact]
    public void MaybeAppendRejectsConflictsWithCommittedEntries()
    {
        var log = new RaftLog(new MemoryStorage(), NullRaftLogger.Instance);
        log.Append(Entries((1, 1), (2, 2), (3, 3)));
        log.CommitTo(3);

        Assert.Throws<RaftInvariantException>(() => log.MaybeAppend(
            new LogSlice(
                4,
                new EntryId(Term: 1, Index: 1),
                Entries((2, 4))),
            3,
            out _));
    }

    [Fact]
    public void CommitAndCurrentTermChecksNeverRegress()
    {
        var log = new RaftLog(new MemoryStorage(), NullRaftLogger.Instance);
        log.Append(Entries((1, 1), (2, 2), (3, 3)));
        log.CommitTo(2);

        log.CommitTo(1);
        Assert.Equal(2UL, log.Committed);
        Assert.Throws<RaftInvariantException>(() => log.CommitTo(4));

        Assert.False(log.MaybeCommit(new EntryId(Term: 0, Index: 3)));
        Assert.False(log.MaybeCommit(new EntryId(Term: 2, Index: 3)));
        Assert.True(log.MaybeCommit(new EntryId(Term: 3, Index: 3)));
        Assert.Equal(3UL, log.Committed);
    }

    [Fact]
    public void TermLookupCoversStableUnstableAndSnapshotRanges()
    {
        MemoryStorage storage = StorageWithSnapshot(
            100,
            1,
            Entries((101, 2), (102, 3)));
        var log = new RaftLog(storage, NullRaftLogger.Instance);
        log.Append(Entries((103, 4), (104, 5)));

        AssertStorageError(StorageError.Compacted, () => log.GetTerm(99));
        Assert.Equal(1UL, log.GetTerm(100));
        Assert.Equal(2UL, log.GetTerm(101));
        Assert.Equal(5UL, log.GetTerm(104));
        AssertStorageError(StorageError.Unavailable, () => log.GetTerm(105));

        log.Restore(SnapshotAt(110, 6));

        AssertStorageError(StorageError.Compacted, () => log.GetTerm(100));
        AssertStorageError(StorageError.Compacted, () => log.GetTerm(109));
        Assert.Equal(6UL, log.GetTerm(110));
        AssertStorageError(StorageError.Unavailable, () => log.GetTerm(111));
    }

    [Fact]
    public void RestoreRequiresNewerSnapshotAndAcknowledgesAtomically()
    {
        MemoryStorage storage = StorageWithSnapshot(3, 1);
        var log = new RaftLog(storage, NullRaftLogger.Instance);
        log.Append(Entries((4, 2), (5, 2)));
        log.CommitTo(5);
        log.AcceptApplying(5, 0, allowUnstable: true);
        log.AppliedTo(5, 0);

        Assert.Throws<RaftInvariantException>(() => log.Restore(SnapshotAt(5, 2)));
        Assert.Equal(5UL, log.Committed);

        log.Restore(SnapshotAt(8, 3));

        Assert.Equal(8UL, log.Committed);
        Assert.Equal(9UL, log.FirstIndex);
        Assert.Equal(8UL, log.LastIndex);
        Assert.True(log.HasUnstableSnapshot);
        Assert.Equal(5UL, log.Applied);
        Assert.False(log.HasNextCommittedEntries(allowUnstable: true));

        Snapshot pendingSnapshot = log.GetNextUnstableSnapshot()!;
        log.AcceptUnstable();
        storage.ApplySnapshot(pendingSnapshot);
        log.AcknowledgeSnapshot(8);

        Assert.Equal(8UL, log.Applied);
        Assert.Equal(8UL, log.Applying);
        Assert.False(log.HasUnstableSnapshot);
        Assert.Equal(9UL, log.FirstIndex);
        Assert.Equal(8UL, log.LastIndex);
        Assert.True(log.Committed <= log.LastIndex);
    }

    [Fact]
    public void NextUnstableEntriesTrackStabilization()
    {
        var storage = new MemoryStorage();
        storage.Append(Entries((1, 1)));
        var log = new RaftLog(storage, NullRaftLogger.Instance);
        log.Append(Entries((2, 2), (3, 2)));

        IReadOnlyList<Entry> pending = log.GetNextUnstableEntries();
        AssertEntries(Entries((2, 2), (3, 2)), pending);
        Assert.True(log.HasNextUnstableEntries);
        Assert.True(log.HasUnstableEntries);

        log.AcceptUnstable();
        Assert.False(log.HasNextUnstableEntries);
        Assert.True(log.HasUnstableEntries);

        log.StableTo(new EntryId(Term: 2, Index: 3));
        Assert.False(log.HasUnstableEntries);
        Assert.Equal(4UL, log.Unstable.Offset);
    }

    [Fact]
    public void CommittedEntryAvailabilityRespectsDurabilityAndApplyingCursor()
    {
        foreach (CommittedCase testCase in new[]
        {
            new CommittedCase(3, true, Entries((4, 1), (5, 1))),
            new CommittedCase(4, true, Entries((5, 1))),
            new CommittedCase(5, true, []),
            new CommittedCase(3, false, Entries((4, 1))),
            new CommittedCase(4, false, []),
        })
        {
            RaftLog log = CommittedLog();
            if (testCase.Applying > log.Applying)
            {
                log.AcceptApplying(
                    testCase.Applying,
                    0,
                    testCase.AllowUnstable);
                log.AppliedTo(testCase.Applying, 0);
            }

            Assert.Equal(
                testCase.Expected.Length > 0,
                log.HasNextCommittedEntries(testCase.AllowUnstable));
            AssertEntries(
                testCase.Expected,
                log.GetNextCommittedEntries(testCase.AllowUnstable));
        }
    }

    [Fact]
    public void ApplyingByteLimitPausesAndAcknowledgementResumes()
    {
        MemoryStorage storage = StorageWithSnapshot(
            3,
            1,
            Entries((4, 1)));
        Entry[] unstable = Entries((5, 1));
        ulong entrySize = (ulong)unstable[0].CalculateSize();
        var log = new RaftLog(
            storage,
            NullRaftLogger.Instance,
            maxApplyingEntriesSize: entrySize);
        log.Append(unstable);
        log.CommitTo(5);

        IReadOnlyList<Entry> firstBatch = log.GetNextCommittedEntries(
            allowUnstable: true);
        Assert.Single(firstBatch);
        Assert.Equal(4UL, firstBatch[0].Index);

        ulong firstSize = EntrySizing.EncodedSize(firstBatch);
        log.AcceptApplying(4, firstSize, allowUnstable: true);

        Assert.True(log.ApplyingEntriesPaused);
        Assert.False(log.HasNextCommittedEntries(allowUnstable: true));

        log.AppliedTo(4, firstSize);

        Assert.False(log.ApplyingEntriesPaused);
        AssertEntries(
            Entries((5, 1)),
            log.GetNextCommittedEntries(allowUnstable: true));
    }

    [Fact]
    public void ZeroApplyingByteLimitStillDeliversOneEntryAtATime()
    {
        MemoryStorage storage = StorageWithSnapshot(
            3,
            1,
            Entries((4, 1)));
        var log = new RaftLog(
            storage,
            NullRaftLogger.Instance,
            maxApplyingEntriesSize: 0);
        log.Append(Entries((5, 1)));
        log.CommitTo(5);

        Assert.True(log.HasNextCommittedEntries(allowUnstable: true));
        IReadOnlyList<Entry> firstBatch = log.GetNextCommittedEntries(
            allowUnstable: true);
        Assert.Single(firstBatch);
        Assert.Equal(4UL, firstBatch[0].Index);

        ulong firstSize = EntrySizing.EncodedSize(firstBatch);
        log.AcceptApplying(4, firstSize, allowUnstable: true);
        Assert.True(log.ApplyingEntriesPaused);

        log.AppliedTo(4, firstSize);
        Assert.False(log.ApplyingEntriesPaused);
        AssertEntries(
            Entries((5, 1)),
            log.GetNextCommittedEntries(allowUnstable: true));
    }

    [Fact]
    public void ApplyingCursorIsMonotonicAndAccountingDoesNotUnderflow()
    {
        RaftLog log = CommittedLog();
        log.AcceptApplying(5, 100, allowUnstable: true);

        Assert.Throws<RaftInvariantException>(
            () => log.AcceptApplying(4, 1, allowUnstable: true));
        Assert.Throws<RaftInvariantException>(() => log.AppliedTo(6, 0));

        log.AppliedTo(4, 101);

        Assert.Equal(4UL, log.Applied);
        Assert.Equal(5UL, log.Applying);
        Assert.Equal(0UL, log.ApplyingEntriesSize);
        Assert.False(log.ApplyingEntriesPaused);
    }

    [Fact]
    public void SliceCombinesStableAndUnstableEntriesWithOneGlobalLimit()
    {
        MemoryStorage storage = StorageWithSnapshot(
            100,
            1,
            Entries((101, 101), (102, 102), (103, 103), (104, 104)));
        var log = new RaftLog(storage, NullRaftLogger.Instance);
        log.Append(Entries(
            (105, 105),
            (106, 106),
            (107, 107),
            (108, 108),
            (109, 109)));

        AssertEntries(
            Entries((101, 101), (102, 102), (103, 103)),
            log.Slice(101, 104, ulong.MaxValue));
        AssertEntries(
            Entries((105, 105), (106, 106)),
            log.Slice(105, 107, ulong.MaxValue));
        AssertEntries(
            Entries((103, 103), (104, 104), (105, 105), (106, 106)),
            log.Slice(103, 107, ulong.MaxValue));
        AssertEntries(
            Entries((103, 103)),
            log.Slice(103, 107, 0));

        ulong stableTwoSize = EntrySizing.EncodedSize(
            Entries((103, 103), (104, 104)));
        AssertEntries(
            Entries((103, 103), (104, 104)),
            log.Slice(103, 107, stableTwoSize));

        AssertStorageError(
            StorageError.Compacted,
            () => log.Slice(100, 101, ulong.MaxValue));
        Assert.Throws<RaftInvariantException>(
            () => log.Slice(107, 106, ulong.MaxValue));
        Assert.Throws<RaftInvariantException>(
            () => log.Slice(109, 111, ulong.MaxValue));
    }

    [Fact]
    public void ScanPagesEntriesAndPropagatesCallbackFailure()
    {
        MemoryStorage storage = StorageWithSnapshot(
            3,
            1,
            Entries((4, 1), (5, 1)));
        var log = new RaftLog(storage, NullRaftLogger.Instance);
        log.Append(Entries((6, 1), (7, 1)));
        ulong pageSize = EntrySizing.EncodedSize(Entries((4, 1), (5, 1)));
        var scanned = new List<Entry>();

        log.Scan(4, 8, pageSize, page => scanned.AddRange(page));

        AssertEntries(Entries((4, 1), (5, 1), (6, 1), (7, 1)), scanned);

        int calls = 0;
        var expected = new InvalidOperationException("stop");
        InvalidOperationException actual = Assert.Throws<InvalidOperationException>(
            () => log.Scan(4, 8, 0, _ =>
            {
                calls++;
                if (calls == 2)
                {
                    throw expected;
                }
            }));
        Assert.Same(expected, actual);
        Assert.Equal(2, calls);
    }

    [Fact]
    public void StorageValuesAreClonedBeforeExposure()
    {
        var storage = new SharedStorage();
        var log = new RaftLog(storage, NullRaftLogger.Instance);

        IReadOnlyList<Entry> entries = log.GetEntries(1);
        entries[0].Term = 9;
        Snapshot snapshot = log.GetSnapshot();
        snapshot.Metadata.Term = 9;

        Assert.Equal(1UL, storage.SharedEntry.Term);
        Assert.Equal(0UL, storage.SharedSnapshot.Metadata.Term);
    }

    [Fact]
    public void TemporarySnapshotUnavailabilityRemainsRetryable()
    {
        var log = new RaftLog(
            new TemporarilyUnavailableSnapshotStorage(),
            NullRaftLogger.Instance);

        StorageException exception = Assert.Throws<StorageException>(
            log.GetSnapshot);

        Assert.Equal(
            StorageError.SnapshotTemporarilyUnavailable,
            exception.Error);
    }

    private static RaftLog CommittedLog()
    {
        MemoryStorage storage = StorageWithSnapshot(
            3,
            1,
            Entries((4, 1)));
        var log = new RaftLog(storage, NullRaftLogger.Instance);
        log.Append(Entries((5, 1), (6, 1)));
        log.CommitTo(5);
        return log;
    }

    private static RaftLog LogFromRetainedEntries(Entry[] retainedEntries)
    {
        Entry snapshotEntry = retainedEntries[0];
        MemoryStorage storage = StorageWithSnapshot(
            snapshotEntry.Index,
            snapshotEntry.Term);
        var log = new RaftLog(storage, NullRaftLogger.Instance);
        log.Append(retainedEntries.Skip(1));
        return log;
    }

    private static MemoryStorage StorageWithSnapshot(
        ulong index,
        ulong term,
        params Entry[] entries)
    {
        var storage = new MemoryStorage();
        storage.ApplySnapshot(SnapshotAt(index, term));
        storage.Append(entries);
        return storage;
    }

    private static Snapshot SnapshotAt(ulong index, ulong term)
    {
        return new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = index,
                Term = term,
                ConfState = new ConfState(),
            },
        };
    }

    private static void AssertStorageError(StorageError error, Action action)
    {
        StorageException exception = Assert.Throws<StorageException>(action);
        Assert.Equal(error, exception.Error);
    }

    private static void AssertEntries(
        Entry[] expected,
        IEnumerable<Entry> actual)
    {
        Assert.Equal(expected, actual);
    }

    private static Entry[] Entries(params (ulong Index, ulong Term)[] values)
    {
        return values.Select(value => new Entry
        {
            Index = value.Index,
            Term = value.Term,
        }).ToArray();
    }

    private sealed record ConflictCase(Entry[] Entries, ulong Expected);

    private sealed record ConflictByTermCase(
        Entry[] Entries,
        ulong Index,
        ulong Term,
        EntryId Expected);

    private sealed record AppendCase(
        Entry[] Incoming,
        ulong ExpectedLastIndex,
        Entry[] Expected,
        ulong ExpectedUnstableOffset);

    private sealed record CommittedCase(
        ulong Applying,
        bool AllowUnstable,
        Entry[] Expected);

    private sealed class SharedStorage : IStorage
    {
        internal Entry SharedEntry { get; } = new()
        {
            Index = 1,
            Term = 1,
        };

        internal Snapshot SharedSnapshot { get; } = SnapshotAt(0, 0);

        public StorageState GetInitialState()
        {
            return new StorageState(null, new ConfState());
        }

        public IReadOnlyList<Entry> GetEntries(
            ulong lowInclusive,
            ulong highExclusive,
            ulong maxSize)
        {
            Assert.Equal(1UL, lowInclusive);
            Assert.Equal(2UL, highExclusive);
            return [SharedEntry];
        }

        public ulong GetTerm(ulong index)
        {
            return index switch
            {
                0 => 0,
                1 => SharedEntry.Term,
                _ => throw new StorageException(
                    StorageError.Unavailable,
                    "Unavailable."),
            };
        }

        public ulong GetLastIndex()
        {
            return 1;
        }

        public ulong GetFirstIndex()
        {
            return 1;
        }

        public Snapshot GetSnapshot()
        {
            return SharedSnapshot;
        }
    }

    private sealed class TemporarilyUnavailableSnapshotStorage : IStorage
    {
        public StorageState GetInitialState()
        {
            return new StorageState(null, new ConfState());
        }

        public IReadOnlyList<Entry> GetEntries(
            ulong lowInclusive,
            ulong highExclusive,
            ulong maxSize)
        {
            throw new StorageException(StorageError.Unavailable, "Unavailable.");
        }

        public ulong GetTerm(ulong index)
        {
            return 0;
        }

        public ulong GetLastIndex()
        {
            return 0;
        }

        public ulong GetFirstIndex()
        {
            return 1;
        }

        public Snapshot GetSnapshot()
        {
            throw new StorageException(
                StorageError.SnapshotTemporarilyUnavailable,
                "Snapshot is still being prepared.");
        }
    }
}
