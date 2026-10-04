using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal sealed class UnstableLog
{
    private readonly IRaftLogger _logger;
    private List<Entry> _entries;
    private Snapshot? _snapshot;
    private bool _snapshotInProgress;

    internal UnstableLog(
        ulong offset,
        IEnumerable<Entry> entries,
        Snapshot? snapshot,
        ulong offsetInProgress,
        bool snapshotInProgress,
        IRaftLogger logger)
    {
        ArgumentNullException.ThrowIfNull(logger);

        List<Entry> storedEntries = CloneAndValidateEntries(entries);
        Snapshot? storedSnapshot = CloneAndValidateSnapshot(snapshot);
        if (storedEntries.Count > 0 && storedEntries[0].Index != offset)
        {
            throw new RaftInvariantException(
                $"First unstable entry index {storedEntries[0].Index} does not match offset {offset}.");
        }

        ulong upperBound = storedEntries.Count == 0
            ? offset
            : storedEntries[^1].Index + 1;
        if (offsetInProgress < offset || offsetInProgress > upperBound)
        {
            throw new RaftInvariantException(
                $"Unstable progress {offsetInProgress} is outside [{offset}, {upperBound}].");
        }

        if (storedSnapshot is not null
            && storedSnapshot.Metadata.Index >= offset)
        {
            throw new RaftInvariantException(
                $"Snapshot index {storedSnapshot.Metadata.Index} must precede unstable offset {offset}.");
        }

        if (snapshotInProgress && storedSnapshot is null)
        {
            throw new RaftInvariantException(
                "A snapshot cannot be in progress when no snapshot is retained.");
        }

        Offset = offset;
        OffsetInProgress = offsetInProgress;
        _entries = storedEntries;
        _snapshot = storedSnapshot;
        _snapshotInProgress = snapshotInProgress;
        _logger = logger;
    }

    internal ulong Offset { get; private set; }

    internal ulong OffsetInProgress { get; private set; }

    internal bool HasEntries => _entries.Count > 0;

    internal bool HasSnapshot => _snapshot is not null;

    internal bool TryGetFirstIndex(out ulong index)
    {
        if (_snapshot is null)
        {
            index = 0;
            return false;
        }

        index = _snapshot.Metadata.Index + 1;
        return true;
    }

    internal bool TryGetLastIndex(out ulong index)
    {
        if (_entries.Count > 0)
        {
            index = _entries[^1].Index;
            return true;
        }

        if (_snapshot is not null)
        {
            index = _snapshot.Metadata.Index;
            return true;
        }

        index = 0;
        return false;
    }

    internal bool TryGetTerm(ulong index, out ulong term)
    {
        if (index < Offset)
        {
            if (_snapshot is not null && _snapshot.Metadata.Index == index)
            {
                term = _snapshot.Metadata.Term;
                return true;
            }

            term = 0;
            return false;
        }

        if (_entries.Count == 0 || index > _entries[^1].Index)
        {
            term = 0;
            return false;
        }

        term = _entries[(int)(index - Offset)].Term;
        return true;
    }

    internal IReadOnlyList<Entry> GetNextEntries()
    {
        int start = checked((int)(OffsetInProgress - Offset));
        if (start == _entries.Count)
        {
            return [];
        }

        var result = new Entry[_entries.Count - start];
        for (int index = start; index < _entries.Count; index++)
        {
            result[index - start] = _entries[index].Clone();
        }

        return result;
    }

    internal Snapshot? GetNextSnapshot()
    {
        return _snapshot is null || _snapshotInProgress
            ? null
            : _snapshot.Clone();
    }

    internal Snapshot? GetSnapshot()
    {
        return _snapshot?.Clone();
    }

    internal bool HasSnapshotAt(ulong index)
    {
        return _snapshot?.Metadata.Index == index;
    }

    internal void AcceptInProgress()
    {
        if (_entries.Count > 0)
        {
            OffsetInProgress = _entries[^1].Index + 1;
        }

        if (_snapshot is not null)
        {
            _snapshotInProgress = true;
        }
    }

    internal void StableTo(EntryId entryId)
    {
        if (!TryGetTerm(entryId.Index, out ulong term))
        {
            LogInformation(
                $"Entry at index {entryId.Index} is missing from the unstable log; ignoring.");
            return;
        }

        if (entryId.Index < Offset)
        {
            LogInformation(
                $"Entry at index {entryId.Index} matched the unstable snapshot; ignoring.");
            return;
        }

        if (term != entryId.Term)
        {
            LogInformation(
                $"Entry ({entryId.Index}, {entryId.Term}) does not match unstable term {term}; ignoring.");
            return;
        }

        int stableCount = checked((int)((entryId.Index + 1) - Offset));
        if (stableCount == _entries.Count)
        {
            _entries = [];
        }
        else
        {
            _entries.RemoveRange(0, stableCount);
        }

        Offset = entryId.Index + 1;
        OffsetInProgress = Math.Max(OffsetInProgress, Offset);
    }

    internal void StableSnapshotTo(ulong index)
    {
        if (_snapshot is null || _snapshot.Metadata.Index != index)
        {
            return;
        }

        _snapshot = null;
        _snapshotInProgress = false;
    }

    internal void Restore(Snapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        Snapshot storedSnapshot = CloneAndValidateSnapshot(snapshot)
            ?? throw new RaftInvariantException("A restored snapshot cannot be null.");
        ulong offset = storedSnapshot.Metadata.Index + 1;

        _entries = [];
        Offset = offset;
        OffsetInProgress = offset;
        _snapshot = storedSnapshot;
        _snapshotInProgress = false;
    }

    internal void TruncateAndAppend(IEnumerable<Entry> entries)
    {
        List<Entry> incomingEntries = CloneAndValidateEntries(entries);
        if (incomingEntries.Count == 0)
        {
            throw new ArgumentException(
                "At least one unstable entry is required.",
                nameof(entries));
        }

        ulong fromIndex = incomingEntries[0].Index;
        if (_snapshot is not null && fromIndex <= _snapshot.Metadata.Index)
        {
            throw new RaftInvariantException(
                $"Entry index {fromIndex} cannot overlap snapshot index {_snapshot.Metadata.Index}.");
        }

        ulong upperBound = GetUpperBound();
        if (fromIndex == upperBound)
        {
            _entries.AddRange(incomingEntries);
            return;
        }

        if (fromIndex <= Offset)
        {
            LogInformation($"Replacing unstable entries from index {fromIndex}.");
            _entries = incomingEntries;
            Offset = fromIndex;
            OffsetInProgress = fromIndex;
            return;
        }

        if (fromIndex > upperBound)
        {
            throw new RaftInvariantException(
                $"Append index {fromIndex} exceeds unstable upper bound {upperBound}.");
        }

        LogInformation($"Truncating unstable entries before index {fromIndex}.");
        int retainedCount = checked((int)(fromIndex - Offset));
        var combined = new List<Entry>(retainedCount + incomingEntries.Count);
        for (int index = 0; index < retainedCount; index++)
        {
            combined.Add(_entries[index]);
        }

        combined.AddRange(incomingEntries);
        _entries = combined;
        OffsetInProgress = Math.Min(OffsetInProgress, fromIndex);
    }

    internal IReadOnlyList<Entry> Slice(
        ulong lowInclusive,
        ulong highExclusive)
    {
        if (lowInclusive > highExclusive)
        {
            throw new RaftInvariantException(
                $"Unstable slice [{lowInclusive}, {highExclusive}) is reversed.");
        }

        ulong upperBound = GetUpperBound();
        if (lowInclusive < Offset || highExclusive > upperBound)
        {
            throw new RaftInvariantException(
                $"Unstable slice [{lowInclusive}, {highExclusive}) is outside [{Offset}, {upperBound}).");
        }

        int start = checked((int)(lowInclusive - Offset));
        int count = checked((int)(highExclusive - lowInclusive));
        var result = new Entry[count];
        for (int index = 0; index < count; index++)
        {
            result[index] = _entries[start + index].Clone();
        }

        return result;
    }

    private ulong GetUpperBound()
    {
        return _entries.Count == 0
            ? Offset
            : _entries[^1].Index + 1;
    }

    private void LogInformation(string message)
    {
        RaftLogging.Write(
            _logger,
            RaftLogLevel.Information,
            message);
    }

    private static List<Entry> CloneAndValidateEntries(
        IEnumerable<Entry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        var result = new List<Entry>();
        ulong? previousIndex = null;
        foreach (Entry entry in entries)
        {
            if (entry is null)
            {
                throw new ArgumentException(
                    "Entries cannot contain null values.",
                    nameof(entries));
            }

            if (previousIndex.HasValue
                && (previousIndex.Value == ulong.MaxValue
                    || entry.Index != previousIndex.Value + 1))
            {
                throw new RaftInvariantException(
                    $"Entries are not contiguous at indexes {previousIndex.Value} and {entry.Index}.");
            }

            result.Add(entry.Clone());
            previousIndex = entry.Index;
        }

        if (result.Count > 0 && result[^1].Index == ulong.MaxValue)
        {
            throw new RaftInvariantException(
                $"Entry index {ulong.MaxValue} has no representable successor.");
        }

        return result;
    }

    private static Snapshot? CloneAndValidateSnapshot(Snapshot? snapshot)
    {
        if (snapshot is null)
        {
            return null;
        }

        Snapshot result = ProtocolDefaults.EnsureSnapshot(snapshot.Clone());
        if (result.Metadata.Index == ulong.MaxValue)
        {
            throw new RaftInvariantException(
                $"Snapshot index {ulong.MaxValue} has no representable successor.");
        }

        return result;
    }
}
