using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal sealed class LogSlice
{
    private readonly IReadOnlyList<Entry> entries;

    private LogSlice(EntryId previous, EntryId last, IReadOnlyList<Entry> entries)
    {
        Previous = previous;
        LastEntryId = last;
        this.entries = entries;
    }

    internal EntryId Previous { get; }

    internal EntryId LastEntryId { get; }

    internal ulong LastIndex => LastEntryId.Index;

    internal int Count => entries.Count;

    internal EntryId EntryIdAt(int offset)
    {
        Entry entry = entries[offset];
        return new EntryId(entry.Term, entry.Index);
    }

    internal List<Entry> CloneSuffix(int offset)
    {
        var suffix = new List<Entry>(Count - offset);
        for (int index = offset; index < Count; index++)
        {
            suffix.Add(entries[index].Clone());
        }

        return suffix;
    }

    // The two facades already detach the message from their caller. Borrow
    // its entries only for this serialized transition; never retain them in the log.
    internal static LogSlice ParseOwnedPeerAppend(Message owned) => Parse(
        owned.Term,
        new EntryId(owned.LogTerm, owned.Index),
        owned.Entries,
        fromPeer: true);

    internal static LogSlice ParseInternal(ulong term, EntryId previous, IReadOnlyList<Entry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        Entry[] detached = ProtocolCloning.CloneEntries(entries);
        return Parse(term, previous, detached, fromPeer: false);
    }

    private static LogSlice Parse(ulong term, EntryId previous, IReadOnlyList<Entry> entries, bool fromPeer)
    {
        EntryId preceding = previous;
        foreach (var entry in entries)
        {
            if (fromPeer)
            {
                NetworkMessageValidation.ValidateEntry(entry);
            }

            var current = new EntryId(entry.Term, entry.Index);
            if (preceding.Index == ulong.MaxValue)
            {
                throw fromPeer
                    ? NetworkMessageValidation.Invalid($"Append entries cannot follow index {ulong.MaxValue}.")
                    : Inconsistent(term, preceding, current);
            }

            if (current.Index != preceding.Index + 1)
            {
                throw fromPeer
                    ? NetworkMessageValidation.Invalid(
                        $"Append entry index {current.Index} does not follow {preceding.Index}.")
                    : Inconsistent(term, preceding, current);
            }

            if (current.Index == ulong.MaxValue)
            {
                string message = $"Append entry index {ulong.MaxValue} has no representable successor.";
                throw fromPeer
                    ? NetworkMessageValidation.Invalid(message)
                    : new RaftInvariantException(message);
            }

            if (current.Term < preceding.Term)
            {
                throw fromPeer
                    ? NetworkMessageValidation.Invalid(
                        $"Append entry term {current.Term} precedes prior term {preceding.Term}.")
                    : Inconsistent(term, preceding, current);
            }

            if (fromPeer && current.Term > term)
            {
                throw NetworkMessageValidation.Invalid(
                    $"Append entry term {current.Term} exceeds message term {term}.");
            }

            preceding = current;
        }

        if (term < preceding.Term)
        {
            throw fromPeer
                ? NetworkMessageValidation.Invalid(
                    $"Append message term {term} precedes prior term {preceding.Term}.")
                : new RaftInvariantException($"Leader term {term}: entry {preceding} has a newer term.");
        }

        return new LogSlice(previous, preceding, entries);
    }

    private static RaftInvariantException Inconsistent(ulong term, EntryId preceding, EntryId current) =>
        new($"Leader term {term}: entries {preceding} and {current} are not consistent.");
}
