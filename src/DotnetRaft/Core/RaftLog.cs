using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

namespace DotnetRaft.Core;

internal sealed class RaftLog
{
    private readonly IRaftLogger _logger;
    private readonly ulong _maxApplyingEntriesSize;
    private readonly IStorage _storage;
    private ulong _applyingEntriesSize;
    private bool _applyingEntriesPaused;

    internal RaftLog(
        IStorage storage,
        IRaftLogger logger,
        ulong maxApplyingEntriesSize = ulong.MaxValue)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(logger);

        ulong firstIndex = storage.GetFirstIndex();
        ulong lastIndex = storage.GetLastIndex();
        if (lastIndex == ulong.MaxValue)
        {
            throw new RaftInvariantException(
                $"Last index {lastIndex} has no representable successor.");
        }

        if (firstIndex == 0 || firstIndex > lastIndex + 1)
        {
            throw new RaftInvariantException(
                $"Storage index range [{firstIndex}, {lastIndex}] is invalid.");
        }

        _storage = storage;
        _logger = logger;
        _maxApplyingEntriesSize = maxApplyingEntriesSize;
        Unstable = new UnstableLog(
            lastIndex + 1,
            [],
            null,
            lastIndex + 1,
            false,
            logger);

        Committed = firstIndex - 1;
        Applying = firstIndex - 1;
        Applied = firstIndex - 1;
    }

    internal ulong Committed { get; private set; }

    internal ulong Applying { get; private set; }

    internal ulong Applied { get; private set; }

    internal ulong ApplyingEntriesSize => _applyingEntriesSize;

    internal bool ApplyingEntriesPaused => _applyingEntriesPaused;

    internal UnstableLog Unstable { get; }

    internal ulong FirstIndex
    {
        get
        {
            return Unstable.TryGetFirstIndex(out ulong index)
                ? index
                : _storage.GetFirstIndex();
        }
    }

    internal ulong LastIndex
    {
        get
        {
            return Unstable.TryGetLastIndex(out ulong index)
                ? index
                : _storage.GetLastIndex();
        }
    }

    internal EntryId LastEntryId
    {
        get
        {
            ulong index = LastIndex;
            try
            {
                return new EntryId(Term: GetTerm(index), Index: index);
            }
            catch (StorageException exception)
                when (exception.Error is StorageError.Compacted or StorageError.Unavailable)
            {
                throw new RaftInvariantException(
                    $"Last term at index {index} is unavailable.");
            }
        }
    }

    internal bool HasNextUnstableEntries => GetNextUnstableEntries().Count > 0;

    internal bool HasUnstableEntries => Unstable.HasEntries;

    internal bool HasNextUnstableSnapshot => Unstable.GetNextSnapshot() is not null;

    internal bool HasUnstableSnapshot => Unstable.HasSnapshot;

    internal bool HasUnstableSnapshotAt(ulong index)
    {
        return Unstable.HasSnapshotAt(index);
    }

    internal bool MaybeAppend(
        LogSlice slice,
        ulong committed,
        out ulong lastNewIndex)
    {
        return MaybeAppend(
            slice,
            committed,
            out lastNewIndex,
            out _,
            out _);
    }

    internal bool MaybeAppend(
        LogSlice slice,
        ulong committed,
        out ulong lastNewIndex,
        out ulong firstAppendedIndex,
        out int appendedCount)
    {
        if (!MaybeAppendEntries(
                slice,
                out lastNewIndex,
                out firstAppendedIndex,
                out appendedCount))
        {
            return false;
        }

        CommitTo(Math.Min(committed, lastNewIndex));
        return true;
    }

    internal bool MaybeAppendEntries(
        LogSlice slice,
        out ulong lastNewIndex,
        out ulong firstAppendedIndex,
        out int appendedCount)
    {
        ArgumentNullException.ThrowIfNull(slice);
        slice.Validate();

        if (!MatchTerm(slice.Previous))
        {
            lastNewIndex = 0;
            firstAppendedIndex = 0;
            appendedCount = 0;
            return false;
        }

        lastNewIndex = slice.LastIndex;
        firstAppendedIndex = 0;
        appendedCount = 0;
        ulong conflictIndex = FindConflict(slice.Entries);
        if (conflictIndex != 0)
        {
            if (conflictIndex <= Committed)
            {
                throw new RaftInvariantException(
                    $"Entry {conflictIndex} conflicts with committed index {Committed}.");
            }

            ulong firstIncomingIndex = slice.Previous.Index + 1;
            ulong position = conflictIndex - firstIncomingIndex;
            if (position > (ulong)slice.Entries.Count)
            {
                throw new RaftInvariantException(
                    $"Conflict position {position} exceeds {slice.Entries.Count} incoming entries.");
            }

            Append(slice.Entries.Skip(checked((int)position)));
            firstAppendedIndex = conflictIndex;
            appendedCount =
                slice.Entries.Count - checked((int)position);
        }

        return true;
    }

    internal ulong Append(IEnumerable<Entry> entries)
    {
        Entry[] incomingEntries = MaterializeAndValidateEntries(entries);
        if (incomingEntries.Length == 0)
        {
            return LastIndex;
        }

        if (incomingEntries[0].Index == 0)
        {
            throw new RaftInvariantException(
                "A real log entry cannot use index zero.");
        }

        ulong after = incomingEntries[0].Index - 1;
        if (after < Committed)
        {
            throw new RaftInvariantException(
                $"Append after index {after} would overwrite committed index {Committed}.");
        }

        Unstable.TruncateAndAppend(incomingEntries);
        return LastIndex;
    }

    internal ulong FindConflict(IReadOnlyList<Entry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ValidateEntries(entries);

        foreach (Entry entry in entries)
        {
            var entryId = EntryId.From(entry);
            if (MatchTerm(entryId))
            {
                continue;
            }

            if (entryId.Index <= LastIndex)
            {
                ulong existingTerm = GetTermOrZero(entryId.Index);
                LogInformation(
                    $"Found conflict at index {entryId.Index}: existing term {existingTerm}, incoming term {entryId.Term}.");
            }

            return entryId.Index;
        }

        return 0;
    }

    internal EntryId FindConflictByTerm(ulong index, ulong term)
    {
        while (index > 0)
        {
            try
            {
                ulong localTerm = GetTerm(index);
                if (localTerm <= term)
                {
                    return new EntryId(Term: localTerm, Index: index);
                }
            }
            catch (StorageException exception)
                when (exception.Error is StorageError.Compacted or StorageError.Unavailable)
            {
                return new EntryId(Term: 0, Index: index);
            }

            index--;
        }

        return new EntryId(Term: 0, Index: 0);
    }

    internal bool IsUpToDate(EntryId candidate)
    {
        EntryId local = LastEntryId;
        return candidate.Term > local.Term
            || (candidate.Term == local.Term && candidate.Index >= local.Index);
    }

    internal bool MatchTerm(EntryId entryId)
    {
        try
        {
            return GetTerm(entryId.Index) == entryId.Term;
        }
        catch (StorageException exception)
            when (exception.Error is StorageError.Compacted or StorageError.Unavailable)
        {
            return false;
        }
    }

    internal bool MaybeCommit(EntryId entryId)
    {
        if (entryId.Term == 0
            || entryId.Index <= Committed
            || !MatchTerm(entryId))
        {
            return false;
        }

        CommitTo(entryId.Index);
        return true;
    }

    internal void CommitTo(ulong index)
    {
        if (index <= Committed)
        {
            return;
        }

        ulong lastIndex = LastIndex;
        if (index > lastIndex)
        {
            throw new RaftInvariantException(
                $"Commit index {index} exceeds last index {lastIndex}.");
        }

        Committed = index;
    }

    internal void Restore(Snapshot snapshot)
    {
        Snapshot normalized =
            RestoreUncommitted(snapshot);
        CommitTo(normalized.Metadata.Index);
    }

    internal Snapshot RestoreUncommitted(
        Snapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        Snapshot normalized = ProtocolDefaults.EnsureSnapshot(snapshot.Clone());
        if (normalized.Metadata.Index <= Committed)
        {
            throw new RaftInvariantException(
                $"Snapshot index {normalized.Metadata.Index} is not newer than committed index {Committed}.");
        }

        Unstable.Restore(normalized);
        return normalized;
    }

    internal IReadOnlyList<Entry> GetNextUnstableEntries()
    {
        return Unstable.GetNextEntries();
    }

    internal Snapshot? GetNextUnstableSnapshot()
    {
        return Unstable.GetNextSnapshot();
    }

    internal Snapshot GetSnapshot()
    {
        Snapshot? unstableSnapshot = Unstable.GetSnapshot();
        if (unstableSnapshot is not null)
        {
            return unstableSnapshot;
        }

        Snapshot snapshot = _storage.GetSnapshot()
            ?? throw new RaftInvariantException("Storage returned a null snapshot.");
        return ProtocolDefaults.EnsureSnapshot(snapshot.Clone());
    }

    internal void AcceptUnstable()
    {
        Unstable.AcceptInProgress();
    }

    internal void StableTo(EntryId entryId)
    {
        Unstable.StableTo(entryId);
    }

    internal void AcknowledgeSnapshot(ulong index)
    {
        StableSnapshotTo(index);
        AppliedTo(Math.Max(index, Applied), 0);
    }

    internal void StableSnapshotTo(ulong index)
    {
        Unstable.StableSnapshotTo(index);
    }

    internal IReadOnlyList<Entry> GetNextCommittedEntries(bool allowUnstable)
    {
        if (_applyingEntriesPaused || HasUnstableSnapshot)
        {
            return [];
        }

        ulong low = Applying + 1;
        ulong high = GetMaxApplicableIndex(allowUnstable) + 1;
        if (low >= high)
        {
            return [];
        }

        if (IsApplyingLimitReached())
        {
            throw new RaftInvariantException(
                "No application byte budget remains while delivery is unpaused.");
        }

        ulong remainingSize = _maxApplyingEntriesSize - _applyingEntriesSize;
        return Slice(low, high, remainingSize);
    }

    internal bool HasNextCommittedEntries(bool allowUnstable)
    {
        if (_applyingEntriesPaused || HasUnstableSnapshot)
        {
            return false;
        }

        ulong low = Applying + 1;
        ulong high = GetMaxApplicableIndex(allowUnstable) + 1;
        return low < high;
    }

    internal void AcceptApplying(
        ulong index,
        ulong encodedSize,
        bool allowUnstable)
    {
        if (index < Applying || index > Committed)
        {
            throw new RaftInvariantException(
                $"Applying index {index} is outside [{Applying}, {Committed}].");
        }

        Applying = index;
        _applyingEntriesSize = checked(_applyingEntriesSize + encodedSize);
        _applyingEntriesPaused =
            IsApplyingLimitReached()
            || index < GetMaxApplicableIndex(allowUnstable);
    }

    internal void AppliedTo(ulong index, ulong encodedSize)
    {
        if (index < Applied || index > Committed)
        {
            throw new RaftInvariantException(
                $"Applied index {index} is outside [{Applied}, {Committed}].");
        }

        Applied = index;
        Applying = Math.Max(Applying, index);
        _applyingEntriesSize = _applyingEntriesSize > encodedSize
            ? _applyingEntriesSize - encodedSize
            : 0;
        _applyingEntriesPaused = IsApplyingLimitReached();
    }

    internal ulong GetTerm(ulong index)
    {
        if (Unstable.TryGetTerm(index, out ulong unstableTerm))
        {
            return unstableTerm;
        }

        ulong firstIndex = FirstIndex;
        if (index < firstIndex - 1)
        {
            throw new StorageException(
                StorageError.Compacted,
                $"Term at index {index} has been compacted before {firstIndex - 1}.");
        }

        if (index > LastIndex)
        {
            throw new StorageException(
                StorageError.Unavailable,
                $"Term at index {index} is unavailable.");
        }

        return _storage.GetTerm(index);
    }

    internal IReadOnlyList<Entry> GetEntries(
        ulong startIndex,
        ulong maxSize = ulong.MaxValue)
    {
        ulong lastIndex = LastIndex;
        return startIndex > lastIndex
            ? []
            : Slice(startIndex, lastIndex + 1, maxSize);
    }

    internal IReadOnlyList<Entry> GetAllEntries()
    {
        while (true)
        {
            try
            {
                return GetEntries(FirstIndex);
            }
            catch (StorageException exception)
                when (exception.Error == StorageError.Compacted)
            {
            }
        }
    }

    internal IReadOnlyList<Entry> Slice(
        ulong lowInclusive,
        ulong highExclusive,
        ulong maxSize)
    {
        CheckSliceBounds(lowInclusive, highExclusive);
        if (lowInclusive == highExclusive)
        {
            return [];
        }

        if (lowInclusive >= Unstable.Offset)
        {
            return EntrySizing.LimitSize(
                Unstable.Slice(lowInclusive, highExclusive),
                maxSize);
        }

        ulong stableHigh = Math.Min(highExclusive, Unstable.Offset);
        IReadOnlyList<Entry> storageEntries = [];
        try
        {
            storageEntries = _storage.GetEntries(
                lowInclusive,
                stableHigh,
                maxSize);
        }
        catch (StorageException exception)
            when (exception.Error == StorageError.Compacted)
        {
            throw;
        }
        catch (StorageException exception)
            when (exception.Error == StorageError.Unavailable)
        {
            throw new RaftInvariantException(
                $"Stable entries [{lowInclusive}, {stableHigh}) are unexpectedly unavailable.");
        }

        Entry[] stableEntries = CloneAndValidateStorageEntries(
            storageEntries,
            lowInclusive,
            stableHigh);
        stableEntries = EntrySizing.LimitSize(stableEntries, maxSize).ToArray();
        if (highExclusive <= Unstable.Offset)
        {
            return stableEntries;
        }

        ulong requestedStableCount = stableHigh - lowInclusive;
        if ((ulong)stableEntries.Length < requestedStableCount)
        {
            return stableEntries;
        }

        ulong stableSize = EntrySizing.EncodedSize(stableEntries);
        if (stableSize >= maxSize)
        {
            return stableEntries;
        }

        IReadOnlyList<Entry> unstableEntries = EntrySizing.LimitSize(
            Unstable.Slice(Unstable.Offset, highExclusive),
            maxSize - stableSize);
        ulong unstableSize = EntrySizing.EncodedSize(unstableEntries);
        if (unstableEntries.Count == 1
            && unstableSize > maxSize - stableSize)
        {
            return stableEntries;
        }

        var result = new Entry[stableEntries.Length + unstableEntries.Count];
        stableEntries.CopyTo(result, 0);
        for (int index = 0; index < unstableEntries.Count; index++)
        {
            result[stableEntries.Length + index] = unstableEntries[index].Clone();
        }

        return result;
    }

    internal void Scan(
        ulong lowInclusive,
        ulong highExclusive,
        ulong pageSize,
        Action<IReadOnlyList<Entry>> visitor)
    {
        ArgumentNullException.ThrowIfNull(visitor);

        ulong next = lowInclusive;
        while (next < highExclusive)
        {
            IReadOnlyList<Entry> entries = Slice(next, highExclusive, pageSize);
            if (entries.Count == 0)
            {
                throw new RaftInvariantException(
                    $"Scan returned no entries for [{next}, {highExclusive}).");
            }

            visitor(entries);
            next += (ulong)entries.Count;
        }
    }

    public override string ToString()
    {
        return $"committed={Committed}, applied={Applied}, applying={Applying}, "
            + $"unstable.offset={Unstable.Offset}, "
            + $"unstable.offsetInProgress={Unstable.OffsetInProgress}, "
            + $"hasUnstableEntries={Unstable.HasEntries}";
    }

    private ulong GetMaxApplicableIndex(bool allowUnstable)
    {
        return allowUnstable
            ? Committed
            : Math.Min(Committed, Unstable.Offset - 1);
    }

    private bool IsApplyingLimitReached()
    {
        return _applyingEntriesSize != 0
            && _applyingEntriesSize >= _maxApplyingEntriesSize;
    }

    private void CheckSliceBounds(
        ulong lowInclusive,
        ulong highExclusive)
    {
        if (lowInclusive > highExclusive)
        {
            throw new RaftInvariantException(
                $"Log slice [{lowInclusive}, {highExclusive}) is reversed.");
        }

        ulong firstIndex = FirstIndex;
        if (lowInclusive < firstIndex)
        {
            throw new StorageException(
                StorageError.Compacted,
                $"Log slice starts at compacted index {lowInclusive}; first index is {firstIndex}.");
        }

        ulong lastIndex = LastIndex;
        if (highExclusive > lastIndex + 1)
        {
            throw new RaftInvariantException(
                $"Log slice [{lowInclusive}, {highExclusive}) exceeds last index {lastIndex}.");
        }
    }

    private ulong GetTermOrZero(ulong index)
    {
        try
        {
            return GetTerm(index);
        }
        catch (StorageException exception)
            when (exception.Error is StorageError.Compacted or StorageError.Unavailable)
        {
            return 0;
        }
    }

    private void LogInformation(string message)
    {
        if (_logger.IsEnabled(RaftLogLevel.Information))
        {
            _logger.Log(RaftLogLevel.Information, message);
        }
    }

    private static Entry[] MaterializeAndValidateEntries(
        IEnumerable<Entry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        Entry[] result = entries.ToArray();
        ValidateEntries(result);
        return result;
    }

    private static void ValidateEntries(IReadOnlyList<Entry> entries)
    {
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

            previousIndex = entry.Index;
        }
    }

    private static Entry[] CloneAndValidateStorageEntries(
        IReadOnlyList<Entry> entries,
        ulong lowInclusive,
        ulong highExclusive)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ulong requestedCount = highExclusive - lowInclusive;
        if ((ulong)entries.Count > requestedCount)
        {
            throw new RaftInvariantException(
                $"Storage returned {entries.Count} entries for a {requestedCount}-entry range.");
        }

        if (requestedCount > 0 && entries.Count == 0)
        {
            throw new RaftInvariantException(
                $"Storage returned no entries for [{lowInclusive}, {highExclusive}).");
        }

        var result = new Entry[entries.Count];
        for (int index = 0; index < entries.Count; index++)
        {
            Entry entry = entries[index]
                ?? throw new RaftInvariantException(
                    "Storage returned a null entry.");
            ulong expectedIndex = lowInclusive + (ulong)index;
            if (entry.Index != expectedIndex)
            {
                throw new RaftInvariantException(
                    $"Storage returned entry index {entry.Index}; expected {expectedIndex}.");
            }

            result[index] = entry.Clone();
        }

        return result;
    }
}
