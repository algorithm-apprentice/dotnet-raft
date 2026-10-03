using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

namespace DotnetRaft.Tests.Core;

public sealed class RaftLogMutationSafetyTests
{
    [Fact]
    public void MaybeAppendCapsCommitAtLastNewIndex()
    {
        var log = new RaftLog(
            new MemoryStorage(),
            NullRaftLogger.Instance);
        log.Append(Entries((1, 1)));

        bool matched = log.MaybeAppend(
            new LogSlice(
                2,
                new EntryId(Term: 1, Index: 1),
                Entries((2, 2))),
            ulong.MaxValue,
            out ulong lastNewIndex);

        Assert.True(matched);
        Assert.Equal(2UL, lastNewIndex);
        Assert.Equal(2UL, log.Committed);
    }

    [Fact]
    public void AppendAtCommittedBoundaryIsRejectedAtomically()
    {
        var log = new RaftLog(
            new MemoryStorage(),
            NullRaftLogger.Instance);
        log.Append(Entries((1, 1), (2, 1)));
        log.CommitTo(2);
        Entry[] before = [.. log.GetAllEntries()];

        Assert.Throws<RaftInvariantException>(
            () => log.Append(Entries((2, 2))));

        Assert.Equal(before, log.GetAllEntries());
        Assert.Equal(2UL, log.Committed);
    }

    [Fact]
    public void ApplyingPauseTracksBacklogAndExactBudgetIndependently()
    {
        var backlog = new RaftLog(
            new MemoryStorage(),
            NullRaftLogger.Instance,
            maxApplyingEntriesSize: 100);
        backlog.Append(Entries((1, 1), (2, 1)));
        backlog.CommitTo(2);

        backlog.AcceptApplying(
            index: 1,
            encodedSize: 1,
            allowUnstable: true);

        Assert.True(backlog.ApplyingEntriesPaused);

        var exactBudget = new RaftLog(
            new MemoryStorage(),
            NullRaftLogger.Instance,
            maxApplyingEntriesSize: 100);
        exactBudget.Append(Entries((1, 1)));
        exactBudget.CommitTo(1);

        exactBudget.AcceptApplying(
            index: 1,
            encodedSize: 100,
            allowUnstable: true);

        Assert.True(exactBudget.ApplyingEntriesPaused);
    }

    [Fact]
    public void ApplyingAccountingSubtractsPartialAcknowledgement()
    {
        var log = new RaftLog(
            new MemoryStorage(),
            NullRaftLogger.Instance,
            maxApplyingEntriesSize: 1_000);
        log.Append(Entries((1, 1), (2, 1), (3, 1)));
        log.CommitTo(3);
        log.AcceptApplying(
            index: 3,
            encodedSize: 100,
            allowUnstable: true);

        log.AppliedTo(index: 1, encodedSize: 40);

        Assert.Equal(60UL, log.ApplyingEntriesSize);
        Assert.False(log.ApplyingEntriesPaused);
    }

    [Fact]
    public void ApplyingAccountingRejectsOverflow()
    {
        var log = new RaftLog(
            new MemoryStorage(),
            NullRaftLogger.Instance);
        log.Append(Entries((1, 1)));
        log.CommitTo(1);
        log.AcceptApplying(
            index: 1,
            encodedSize: ulong.MaxValue,
            allowUnstable: true);

        Assert.Throws<OverflowException>(
            () => log.AcceptApplying(
                index: 1,
                encodedSize: 1,
                allowUnstable: true));
    }

    [Fact]
    public void TermLookupEnforcesLastIndexBeforeStorageCall()
    {
        var log = new RaftLog(
            new ContractStorage(
                entries: [EntryAt(1, 1)],
                term: _ => 99),
            NullRaftLogger.Instance);

        StorageException exception =
            Assert.Throws<StorageException>(
                () => log.GetTerm(2));

        Assert.Equal(
            StorageError.Unavailable,
            exception.Error);
    }

    [Fact]
    public void EmptySliceAtBoundaryIsAllowed()
    {
        var log = new RaftLog(
            new MemoryStorage(),
            NullRaftLogger.Instance);
        log.Append(Entries((1, 1)));

        Assert.Empty(log.Slice(2, 2, ulong.MaxValue));
    }

    [Fact]
    public void SliceStopsAtPartialStablePrefix()
    {
        var storage = new ContractStorage(
            entries: [EntryAt(1, 1)],
            lastIndex: 2);
        var log = new RaftLog(
            storage,
            NullRaftLogger.Instance);
        log.Append([EntryAt(3, 1)]);

        IReadOnlyList<Entry> entries =
            log.Slice(1, 4, ulong.MaxValue);

        Assert.Equal(
            [1UL],
            entries.Select(entry => entry.Index));
    }

    [Fact]
    public void SliceIncludesUnstableEntryAtExactRemainingBudget()
    {
        Entry[] stable =
        [
            EntryAt(1, 1),
            EntryAt(2, 1),
        ];
        Entry unstable = EntryAt(3, 1);
        var storage = new ContractStorage(
            entries: stable,
            lastIndex: 2);
        var log = new RaftLog(
            storage,
            NullRaftLogger.Instance);
        log.Append([unstable]);
        ulong maxSize =
            EntrySizing.EncodedSize(stable)
            + EntrySizing.EncodedSize([unstable]);

        IReadOnlyList<Entry> entries =
            log.Slice(1, 4, maxSize);

        Assert.Equal(
            [1UL, 2UL, 3UL],
            entries.Select(entry => entry.Index));
    }

    [Fact]
    public void StorageEntryContractViolationsAreRejected()
    {
        IReadOnlyList<Entry>[] invalidResults =
        [
            [],
            [EntryAt(1, 1), EntryAt(2, 1)],
            [null!],
            [EntryAt(2, 1)],
        ];

        foreach (IReadOnlyList<Entry> result in invalidResults)
        {
            var log = new RaftLog(
                new ContractStorage(entries: result),
                NullRaftLogger.Instance);

            Assert.Throws<RaftInvariantException>(
                () => log.Slice(1, 2, ulong.MaxValue));
        }
    }

    [Fact]
    public void NullStorageSnapshotIsRejected()
    {
        var log = new RaftLog(
            new ContractStorage(
                entries: [EntryAt(1, 1)],
                snapshot: null),
            NullRaftLogger.Instance);

        Assert.Throws<RaftInvariantException>(
            log.GetSnapshot);
    }

    [Fact]
    public void ScanRejectsNullVisitorBeforeReading()
    {
        var storage = new ContractStorage(
            entries: [EntryAt(1, 1)]);
        var log = new RaftLog(
            storage,
            NullRaftLogger.Instance);

        Assert.Throws<ArgumentNullException>(
            () => log.Scan(
                1,
                2,
                ulong.MaxValue,
                null!));

        Assert.Equal(0, storage.EntryReadCount);
    }

    private static Entry[] Entries(
        params (ulong Index, ulong Term)[] values)
    {
        return values
            .Select(value =>
                EntryAt(value.Index, value.Term))
            .ToArray();
    }

    private static Entry EntryAt(
        ulong index,
        ulong term)
    {
        return new Entry
        {
            Index = index,
            Term = term,
        };
    }

    private sealed class ContractStorage : IStorage
    {
        private readonly IReadOnlyList<Entry> entries;
        private readonly ulong lastIndex;
        private readonly Func<ulong, ulong> term;
        private readonly Snapshot? snapshot;

        internal ContractStorage(
            IReadOnlyList<Entry> entries,
            ulong lastIndex = 1,
            Func<ulong, ulong>? term = null,
            Snapshot? snapshot = default)
        {
            this.entries = entries;
            this.lastIndex = lastIndex;
            this.term = term ?? (_ => 1);
            this.snapshot = snapshot;
        }

        internal int EntryReadCount { get; private set; }

        public StorageState GetInitialState()
        {
            return new StorageState(
                null,
                new ConfState());
        }

        public IReadOnlyList<Entry> GetEntries(
            ulong lowInclusive,
            ulong highExclusive,
            ulong maxSize)
        {
            EntryReadCount++;
            return entries;
        }

        public ulong GetTerm(ulong index)
        {
            return term(index);
        }

        public ulong GetLastIndex()
        {
            return lastIndex;
        }

        public ulong GetFirstIndex()
        {
            return 1;
        }

        public Snapshot GetSnapshot()
        {
            return snapshot!;
        }
    }
}
