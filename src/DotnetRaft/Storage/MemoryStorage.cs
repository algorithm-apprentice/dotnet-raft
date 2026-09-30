using DotnetRaft.Core;
using DotnetRaft.Protocol;

using Google.Protobuf;

namespace DotnetRaft.Storage;

public sealed class MemoryStorage : IStorage
{
    private readonly object _gate = new();
    private readonly List<Entry> _entries = [new Entry()];
    private HardState? _hardState;
    private Snapshot _snapshot = ProtocolDefaults.EnsureSnapshot(new Snapshot());

    public StorageState GetInitialState()
    {
        lock (_gate)
        {
            _snapshot = ProtocolDefaults.EnsureSnapshot(_snapshot);
            return new StorageState(
                _hardState?.Clone(),
                _snapshot.Metadata.ConfState.Clone());
        }
    }

    public IReadOnlyList<Entry> GetEntries(
        ulong lowInclusive,
        ulong highExclusive,
        ulong maxSize)
    {
        lock (_gate)
        {
            ulong offset = _entries[0].Index;
            if (lowInclusive <= offset)
            {
                throw new StorageException(
                    StorageError.Compacted,
                    $"Entry index {lowInclusive} has been compacted through {offset}.");
            }

            if (highExclusive < lowInclusive)
            {
                throw new RaftInvariantException(
                    $"Entry range [{lowInclusive}, {highExclusive}) is reversed.");
            }

            ulong lastIndex = GetLastIndexCore();
            if ((highExclusive - 1) > lastIndex)
            {
                throw new RaftInvariantException(
                    $"Entry range [{lowInclusive}, {highExclusive}) exceeds last index {lastIndex}.");
            }

            if (_entries.Count == 1)
            {
                throw new StorageException(
                    StorageError.Unavailable,
                    "No log entries are available.");
            }

            int start = checked((int)(lowInclusive - offset));
            int count = checked((int)(highExclusive - lowInclusive));
            var entries = new Entry[count];

            for (int index = 0; index < count; index++)
            {
                entries[index] = _entries[start + index].Clone();
            }

            return EntrySizing.LimitSize(entries, maxSize);
        }
    }

    public ulong GetTerm(ulong index)
    {
        lock (_gate)
        {
            ulong offset = _entries[0].Index;
            if (index < offset)
            {
                throw new StorageException(
                    StorageError.Compacted,
                    $"Entry index {index} has been compacted through {offset}.");
            }

            ulong position = index - offset;
            if (position >= (ulong)_entries.Count)
            {
                throw new StorageException(
                    StorageError.Unavailable,
                    $"Entry index {index} is unavailable.");
            }

            return _entries[(int)position].Term;
        }
    }

    public ulong GetLastIndex()
    {
        lock (_gate)
        {
            return GetLastIndexCore();
        }
    }

    public ulong GetFirstIndex()
    {
        lock (_gate)
        {
            return GetFirstIndexCore();
        }
    }

    public Snapshot GetSnapshot()
    {
        lock (_gate)
        {
            _snapshot = ProtocolDefaults.EnsureSnapshot(_snapshot);
            return _snapshot.Clone();
        }
    }

    public void SetHardState(HardState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        HardState storedState = state.Clone();

        lock (_gate)
        {
            _hardState = storedState;
        }
    }

    public void ApplySnapshot(Snapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        Snapshot storedSnapshot = ProtocolDefaults.EnsureSnapshot(snapshot.Clone());
        EnsureHasRepresentableSuccessor(storedSnapshot.Metadata.Index, "Snapshot");

        lock (_gate)
        {
            ulong currentIndex = _snapshot.Metadata.Index;
            ulong incomingIndex = storedSnapshot.Metadata.Index;
            if (currentIndex != 0 && currentIndex >= incomingIndex)
            {
                throw new StorageException(
                    StorageError.SnapshotOutOfDate,
                    $"Snapshot index {incomingIndex} is not newer than current index {currentIndex}.");
            }

            _snapshot = storedSnapshot;
            _entries.Clear();
            _entries.Add(new Entry
            {
                Index = storedSnapshot.Metadata.Index,
                Term = storedSnapshot.Metadata.Term,
            });
        }
    }

    public Snapshot CreateSnapshot(
        ulong index,
        ConfState? confState,
        ByteString data)
    {
        ArgumentNullException.ThrowIfNull(data);

        lock (_gate)
        {
            ulong currentSnapshotIndex = _snapshot.Metadata.Index;
            if (index <= currentSnapshotIndex)
            {
                throw new StorageException(
                    StorageError.SnapshotOutOfDate,
                    $"Snapshot index {index} is not newer than current index {currentSnapshotIndex}.");
            }

            ulong offset = _entries[0].Index;
            if (index < offset)
            {
                throw new RaftInvariantException(
                    $"Snapshot index {index} precedes the retained term at index {offset}.");
            }

            ulong lastIndex = GetLastIndexCore();
            if (index > lastIndex)
            {
                throw new RaftInvariantException(
                    $"Snapshot index {index} exceeds last index {lastIndex}.");
            }

            _snapshot.Metadata.Index = index;
            _snapshot.Metadata.Term = _entries[(int)(index - offset)].Term;
            if (confState is not null)
            {
                _snapshot.Metadata.ConfState = confState.Clone();
            }

            _snapshot.Data = data;
            return _snapshot.Clone();
        }
    }

    public void Compact(ulong compactIndex)
    {
        lock (_gate)
        {
            ulong offset = _entries[0].Index;
            if (compactIndex <= offset)
            {
                throw new StorageException(
                    StorageError.Compacted,
                    $"Cannot compact index {compactIndex}; storage is already compacted through {offset}.");
            }

            ulong lastIndex = GetLastIndexCore();
            if (compactIndex > lastIndex)
            {
                throw new RaftInvariantException(
                    $"Compact index {compactIndex} exceeds last index {lastIndex}.");
            }

            int position = checked((int)(compactIndex - offset));
            var dummyEntry = new Entry
            {
                Index = _entries[position].Index,
                Term = _entries[position].Term,
            };

            _entries.RemoveRange(0, position);
            _entries[0] = dummyEntry;
        }
    }

    public void Append(IEnumerable<Entry> entries)
    {
        List<Entry> incomingEntries = CloneAndValidateEntries(entries);
        if (incomingEntries.Count == 0)
        {
            return;
        }

        EnsureHasRepresentableSuccessor(incomingEntries[^1].Index, "Final appended entry");

        lock (_gate)
        {
            ulong firstIndex = GetFirstIndexCore();
            ulong incomingLastIndex = incomingEntries[^1].Index;
            if (incomingLastIndex < firstIndex)
            {
                return;
            }

            if (firstIndex > incomingEntries[0].Index)
            {
                int compactedCount = checked((int)(firstIndex - incomingEntries[0].Index));
                incomingEntries.RemoveRange(0, compactedCount);
            }

            ulong offset = incomingEntries[0].Index - _entries[0].Index;
            if ((ulong)_entries.Count > offset)
            {
                int position = checked((int)offset);
                _entries.RemoveRange(position, _entries.Count - position);
                _entries.AddRange(incomingEntries);
                return;
            }

            if ((ulong)_entries.Count == offset)
            {
                _entries.AddRange(incomingEntries);
                return;
            }

            throw new RaftInvariantException(
                $"Missing log entry between last index {GetLastIndexCore()} and append index {incomingEntries[0].Index}.");
        }
    }

    private ulong GetLastIndexCore()
    {
        return _entries[0].Index + ((ulong)_entries.Count - 1);
    }

    private ulong GetFirstIndexCore()
    {
        return checked(_entries[0].Index + 1);
    }

    private static List<Entry> CloneAndValidateEntries(IEnumerable<Entry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        var result = new List<Entry>();
        ulong? previousIndex = null;

        foreach (Entry entry in entries)
        {
            if (entry is null)
            {
                throw new ArgumentException("Entries cannot contain null values.", nameof(entries));
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

        return result;
    }

    private static void EnsureHasRepresentableSuccessor(ulong index, string description)
    {
        if (index == ulong.MaxValue)
        {
            throw new RaftInvariantException(
                $"{description} index {index} has no representable successor.");
        }
    }
}
