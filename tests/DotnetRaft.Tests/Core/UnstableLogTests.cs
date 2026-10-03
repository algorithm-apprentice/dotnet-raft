using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;

namespace DotnetRaft.Tests.Core;

public sealed class UnstableLogTests
{
    [Fact]
    public void TryGetFirstIndexMatchesReferenceMatrix()
    {
        var cases = new[]
        {
            new FirstIndexCase(Entries((5, 1)), 5, null, false, 0),
            new FirstIndexCase([], 0, null, false, 0),
            new FirstIndexCase(Entries((5, 1)), 5, SnapshotAt(4, 1), true, 5),
            new FirstIndexCase([], 5, SnapshotAt(4, 1), true, 5),
        };

        foreach (FirstIndexCase testCase in cases)
        {
            UnstableLog unstable = Create(
                testCase.Offset,
                testCase.Entries,
                testCase.Snapshot);

            bool found = unstable.TryGetFirstIndex(out ulong index);

            Assert.Equal(testCase.Found, found);
            Assert.Equal(testCase.Index, index);
        }
    }

    [Fact]
    public void TryGetLastIndexMatchesReferenceMatrix()
    {
        var cases = new[]
        {
            new LastIndexCase(Entries((5, 1)), 5, null, true, 5),
            new LastIndexCase(Entries((5, 1)), 5, SnapshotAt(4, 1), true, 5),
            new LastIndexCase([], 5, SnapshotAt(4, 1), true, 4),
            new LastIndexCase([], 0, null, false, 0),
        };

        foreach (LastIndexCase testCase in cases)
        {
            UnstableLog unstable = Create(
                testCase.Offset,
                testCase.Entries,
                testCase.Snapshot);

            bool found = unstable.TryGetLastIndex(out ulong index);

            Assert.Equal(testCase.Found, found);
            Assert.Equal(testCase.Index, index);
        }
    }

    [Fact]
    public void TryGetTermMatchesReferenceMatrix()
    {
        var cases = new[]
        {
            new TermCase(Entries((5, 1)), 5, null, 5, true, 1),
            new TermCase(Entries((5, 1)), 5, null, 6, false, 0),
            new TermCase(Entries((5, 1)), 5, null, 4, false, 0),
            new TermCase(Entries((5, 1)), 5, SnapshotAt(4, 1), 5, true, 1),
            new TermCase(Entries((5, 1)), 5, SnapshotAt(4, 1), 6, false, 0),
            new TermCase(Entries((5, 1)), 5, SnapshotAt(4, 1), 4, true, 1),
            new TermCase(Entries((5, 1)), 5, SnapshotAt(4, 1), 3, false, 0),
            new TermCase([], 5, SnapshotAt(4, 1), 5, false, 0),
            new TermCase([], 5, SnapshotAt(4, 1), 4, true, 1),
            new TermCase([], 0, null, 5, false, 0),
        };

        foreach (TermCase testCase in cases)
        {
            UnstableLog unstable = Create(
                testCase.Offset,
                testCase.Entries,
                testCase.Snapshot);

            bool found = unstable.TryGetTerm(testCase.Index, out ulong term);

            Assert.Equal(testCase.Found, found);
            Assert.Equal(testCase.Term, term);
        }
    }

    [Fact]
    public void RestoreReplacesEntriesAndResetsProgress()
    {
        UnstableLog unstable = Create(
            5,
            Entries((5, 1)),
            SnapshotAt(4, 1),
            offsetInProgress: 6,
            snapshotInProgress: true);
        Snapshot snapshot = SnapshotAt(6, 2);

        unstable.Restore(snapshot);

        Assert.Equal(7UL, unstable.Offset);
        Assert.Equal(7UL, unstable.OffsetInProgress);
        Assert.False(unstable.HasEntries);
        Assert.True(unstable.HasSnapshot);
        Assert.Equal(snapshot, unstable.GetNextSnapshot());
    }

    [Fact]
    public void GetNextEntriesReturnsOnlyWorkNotInProgress()
    {
        var cases = new[]
        {
            new NextEntriesCase(5, Entries((5, 1), (6, 1)), 5, Entries((5, 1), (6, 1))),
            new NextEntriesCase(5, Entries((5, 1), (6, 1)), 6, Entries((6, 1))),
            new NextEntriesCase(5, Entries((5, 1), (6, 1)), 7, []),
        };

        foreach (NextEntriesCase testCase in cases)
        {
            UnstableLog unstable = Create(
                testCase.Offset,
                testCase.Entries,
                offsetInProgress: testCase.OffsetInProgress);

            AssertEntries(testCase.Expected, unstable.GetNextEntries());
        }
    }

    [Fact]
    public void GetNextSnapshotHonorsProgressAndReturnsAClone()
    {
        Snapshot snapshot = SnapshotAt(4, 1);
        UnstableLog pending = Create(5, [], snapshot);
        UnstableLog inProgress = Create(
            5,
            [],
            snapshot,
            snapshotInProgress: true);
        UnstableLog absent = Create(5, []);

        Snapshot? returned = pending.GetNextSnapshot();
        Assert.NotNull(returned);
        Assert.Equal(snapshot, returned);
        returned.Metadata.Term = 9;

        Assert.Equal(1UL, pending.GetNextSnapshot()!.Metadata.Term);
        Assert.Null(inProgress.GetNextSnapshot());
        Assert.Null(absent.GetNextSnapshot());
    }

    [Fact]
    public void AcceptInProgressAdvancesCurrentHighWaterMarks()
    {
        var cases = new[]
        {
            new AcceptCase([], null, 5, false, 5, false),
            new AcceptCase(Entries((5, 1)), null, 5, false, 6, false),
            new AcceptCase(Entries((5, 1), (6, 1)), null, 5, false, 7, false),
            new AcceptCase(Entries((5, 1), (6, 1)), null, 6, false, 7, false),
            new AcceptCase(Entries((5, 1), (6, 1)), null, 7, false, 7, false),
            new AcceptCase([], SnapshotAt(4, 1), 5, false, 5, true),
            new AcceptCase(Entries((5, 1)), SnapshotAt(4, 1), 5, false, 6, true),
            new AcceptCase(Entries((5, 1), (6, 1)), SnapshotAt(4, 1), 6, false, 7, true),
            new AcceptCase([], SnapshotAt(4, 1), 5, true, 5, true),
            new AcceptCase(Entries((5, 1)), SnapshotAt(4, 1), 5, true, 6, true),
        };

        foreach (AcceptCase testCase in cases)
        {
            UnstableLog unstable = Create(
                5,
                testCase.Entries,
                testCase.Snapshot,
                testCase.OffsetInProgress,
                testCase.SnapshotInProgress);

            unstable.AcceptInProgress();

            Assert.Equal(testCase.ExpectedOffsetInProgress, unstable.OffsetInProgress);
            Assert.Equal(
                testCase.ExpectedSnapshotInProgress,
                unstable.HasSnapshot && unstable.GetNextSnapshot() is null);
        }
    }

    [Fact]
    public void StableToMatchesReferenceMatrix()
    {
        var cases = new[]
        {
            new StableCase([], 0, 0, null, new EntryId(1, 5), 0, 0, []),
            new StableCase(Entries((5, 1)), 5, 6, null, new EntryId(1, 5), 6, 6, []),
            new StableCase(
                Entries((5, 1), (6, 1)),
                5,
                6,
                null,
                new EntryId(1, 5),
                6,
                6,
                Entries((6, 1))),
            new StableCase(
                Entries((5, 1), (6, 1)),
                5,
                7,
                null,
                new EntryId(1, 5),
                6,
                7,
                Entries((6, 1))),
            new StableCase(
                Entries((6, 2)),
                6,
                7,
                null,
                new EntryId(1, 6),
                6,
                7,
                Entries((6, 2))),
            new StableCase(
                Entries((5, 1)),
                5,
                6,
                null,
                new EntryId(1, 4),
                5,
                6,
                Entries((5, 1))),
            new StableCase(
                Entries((5, 1)),
                5,
                6,
                SnapshotAt(4, 1),
                new EntryId(1, 4),
                5,
                6,
                Entries((5, 1))),
        };

        foreach (StableCase testCase in cases)
        {
            UnstableLog unstable = Create(
                testCase.Offset,
                testCase.Entries,
                testCase.Snapshot,
                testCase.OffsetInProgress);

            unstable.StableTo(testCase.EntryId);

            Assert.Equal(testCase.ExpectedOffset, unstable.Offset);
            Assert.Equal(
                testCase.ExpectedOffsetInProgress,
                unstable.OffsetInProgress);
            AssertRetainedEntries(testCase.ExpectedEntries, unstable);
        }
    }

    [Fact]
    public void StableSnapshotToRequiresTheExactSnapshotIndex()
    {
        UnstableLog unstable = Create(5, [], SnapshotAt(4, 1));

        unstable.StableSnapshotTo(3);
        Assert.True(unstable.HasSnapshot);

        unstable.StableSnapshotTo(4);
        Assert.False(unstable.HasSnapshot);
        Assert.Null(unstable.GetSnapshot());
        Assert.Null(unstable.GetNextSnapshot());
    }

    [Fact]
    public void TruncateAndAppendMatchesReferenceMatrix()
    {
        var cases = new[]
        {
            new AppendCase(
                Entries((5, 1)),
                5,
                5,
                Entries((6, 1), (7, 1)),
                5,
                5,
                Entries((5, 1), (6, 1), (7, 1))),
            new AppendCase(
                Entries((5, 1)),
                5,
                6,
                Entries((6, 1), (7, 1)),
                5,
                6,
                Entries((5, 1), (6, 1), (7, 1))),
            new AppendCase(
                Entries((5, 1)),
                5,
                5,
                Entries((5, 2), (6, 2)),
                5,
                5,
                Entries((5, 2), (6, 2))),
            new AppendCase(
                Entries((5, 1)),
                5,
                5,
                Entries((4, 2), (5, 2), (6, 2)),
                4,
                4,
                Entries((4, 2), (5, 2), (6, 2))),
            new AppendCase(
                Entries((5, 1)),
                5,
                6,
                Entries((5, 2), (6, 2)),
                5,
                5,
                Entries((5, 2), (6, 2))),
            new AppendCase(
                Entries((5, 1), (6, 1), (7, 1)),
                5,
                5,
                Entries((6, 2)),
                5,
                5,
                Entries((5, 1), (6, 2))),
            new AppendCase(
                Entries((5, 1), (6, 1), (7, 1)),
                5,
                5,
                Entries((7, 2), (8, 2)),
                5,
                5,
                Entries((5, 1), (6, 1), (7, 2), (8, 2))),
            new AppendCase(
                Entries((5, 1), (6, 1), (7, 1)),
                5,
                6,
                Entries((6, 2)),
                5,
                6,
                Entries((5, 1), (6, 2))),
            new AppendCase(
                Entries((5, 1), (6, 1), (7, 1)),
                5,
                7,
                Entries((6, 2)),
                5,
                6,
                Entries((5, 1), (6, 2))),
        };

        foreach (AppendCase testCase in cases)
        {
            UnstableLog unstable = Create(
                testCase.Offset,
                testCase.Entries,
                offsetInProgress: testCase.OffsetInProgress);

            unstable.TruncateAndAppend(testCase.Incoming);

            Assert.Equal(testCase.ExpectedOffset, unstable.Offset);
            Assert.Equal(
                testCase.ExpectedOffsetInProgress,
                unstable.OffsetInProgress);
            AssertRetainedEntries(testCase.ExpectedEntries, unstable);
        }
    }

    [Fact]
    public void SliceValidatesBoundsAndReturnsClones()
    {
        UnstableLog unstable = Create(
            5,
            Entries((5, 1), (6, 1), (7, 1)));

        Assert.Empty(unstable.Slice(5, 5));
        AssertEntries(Entries((5, 1), (6, 1)), unstable.Slice(5, 7));
        Assert.Throws<RaftInvariantException>(() => unstable.Slice(7, 6));
        Assert.Throws<RaftInvariantException>(() => unstable.Slice(4, 6));
        Assert.Throws<RaftInvariantException>(() => unstable.Slice(5, 9));

        IReadOnlyList<Entry> returned = unstable.Slice(5, 6);
        returned[0].Term = 9;
        Assert.Equal(1UL, unstable.Slice(5, 6)[0].Term);
    }

    [Fact]
    public void ConstructorValidatesStateInvariants()
    {
        Assert.Throws<RaftInvariantException>(
            () => Create(5, Entries((6, 1))));
        Assert.Throws<RaftInvariantException>(
            () => Create(5, [], offsetInProgress: 4));
        Assert.Throws<RaftInvariantException>(
            () => Create(5, Entries((5, 1)), offsetInProgress: 7));
        Assert.Throws<RaftInvariantException>(
            () => Create(5, [], SnapshotAt(5, 1)));
        Assert.Throws<RaftInvariantException>(
            () => Create(5, [], SnapshotAt(6, 1)));
        Assert.Throws<RaftInvariantException>(
            () => Create(5, [], snapshotInProgress: true));
        Assert.Throws<RaftInvariantException>(
            () => Create(
                ulong.MaxValue,
                [],
                SnapshotAt(ulong.MaxValue, 1)));
        Assert.Throws<RaftInvariantException>(
            () => Create(
                ulong.MaxValue,
                Entries((ulong.MaxValue, 1))));
    }

    [Fact]
    public void AppendRejectsSnapshotOverlapAndMaximumIndexWithoutMutation()
    {
        UnstableLog unstable = Create(
            5,
            Entries((5, 1)),
            SnapshotAt(4, 1));

        Assert.Throws<RaftInvariantException>(
            () => unstable.TruncateAndAppend(Entries((4, 2))));
        Assert.Throws<RaftInvariantException>(
            () => unstable.TruncateAndAppend(Entries((ulong.MaxValue, 2))));

        Assert.Equal(5UL, unstable.Offset);
        AssertRetainedEntries(Entries((5, 1)), unstable);
        Assert.Equal(4UL, unstable.GetSnapshot()!.Metadata.Index);
    }

    [Fact]
    public void AppendRejectsEmptyAndGappedInputWithoutMutation()
    {
        UnstableLog unstable = Create(
            5,
            Entries((5, 1)));

        Assert.Throws<ArgumentException>(
            () => unstable.TruncateAndAppend([]));
        Assert.Throws<RaftInvariantException>(
            () => unstable.TruncateAndAppend(
                Entries((8, 2))));

        Assert.Equal(5UL, unstable.Offset);
        Assert.Equal(5UL, unstable.OffsetInProgress);
        AssertRetainedEntries(
            Entries((5, 1)),
            unstable);
    }

    [Fact]
    public void AppendRejectsNullAndNoncontiguousInput()
    {
        UnstableLog unstable = Create(
            5,
            Entries((5, 1)));

        Assert.Throws<ArgumentNullException>(
            () => unstable.TruncateAndAppend(null!));
        Assert.Throws<ArgumentException>(
            () => unstable.TruncateAndAppend(
                [null!]));
        Assert.Throws<RaftInvariantException>(
            () => unstable.TruncateAndAppend(
                Entries((6, 2), (8, 2))));

        AssertRetainedEntries(
            Entries((5, 1)),
            unstable);
    }

    [Fact]
    public void InputsAndOutputsDoNotAliasRetainedState()
    {
        Entry[] entries = Entries((5, 1));
        Snapshot snapshot = SnapshotAt(4, 1);
        UnstableLog unstable = Create(5, entries, snapshot);

        entries[0].Term = 9;
        snapshot.Metadata.Term = 9;

        Assert.Equal(1UL, unstable.Slice(5, 6)[0].Term);
        Assert.Equal(1UL, unstable.GetSnapshot()!.Metadata.Term);

        Entry[] appended = Entries((6, 2));
        unstable.TruncateAndAppend(appended);
        appended[0].Term = 10;
        Assert.Equal(2UL, unstable.Slice(6, 7)[0].Term);

        Snapshot restored = SnapshotAt(8, 3);
        unstable.Restore(restored);
        restored.Metadata.Term = 11;
        Assert.Equal(3UL, unstable.GetSnapshot()!.Metadata.Term);
    }

    private static UnstableLog Create(
        ulong offset,
        IEnumerable<Entry> entries,
        Snapshot? snapshot = null,
        ulong? offsetInProgress = null,
        bool snapshotInProgress = false)
    {
        return new UnstableLog(
            offset,
            entries,
            snapshot,
            offsetInProgress ?? offset,
            snapshotInProgress,
            NullRaftLogger.Instance);
    }

    private static void AssertRetainedEntries(
        Entry[] expected,
        UnstableLog unstable)
    {
        Assert.Equal(expected.Length > 0, unstable.HasEntries);
        AssertEntries(
            expected,
            unstable.Slice(unstable.Offset, unstable.Offset + (ulong)expected.Length));
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

    private static Entry[] Entries(params (ulong Index, ulong Term)[] values)
    {
        return values.Select(value => new Entry
        {
            Index = value.Index,
            Term = value.Term,
        }).ToArray();
    }

    private sealed record FirstIndexCase(
        Entry[] Entries,
        ulong Offset,
        Snapshot? Snapshot,
        bool Found,
        ulong Index);

    private sealed record LastIndexCase(
        Entry[] Entries,
        ulong Offset,
        Snapshot? Snapshot,
        bool Found,
        ulong Index);

    private sealed record TermCase(
        Entry[] Entries,
        ulong Offset,
        Snapshot? Snapshot,
        ulong Index,
        bool Found,
        ulong Term);

    private sealed record NextEntriesCase(
        ulong Offset,
        Entry[] Entries,
        ulong OffsetInProgress,
        Entry[] Expected);

    private sealed record AcceptCase(
        Entry[] Entries,
        Snapshot? Snapshot,
        ulong OffsetInProgress,
        bool SnapshotInProgress,
        ulong ExpectedOffsetInProgress,
        bool ExpectedSnapshotInProgress);

    private sealed record StableCase(
        Entry[] Entries,
        ulong Offset,
        ulong OffsetInProgress,
        Snapshot? Snapshot,
        EntryId EntryId,
        ulong ExpectedOffset,
        ulong ExpectedOffsetInProgress,
        Entry[] ExpectedEntries);

    private sealed record AppendCase(
        Entry[] Entries,
        ulong Offset,
        ulong OffsetInProgress,
        Entry[] Incoming,
        ulong ExpectedOffset,
        ulong ExpectedOffsetInProgress,
        Entry[] ExpectedEntries);
}
