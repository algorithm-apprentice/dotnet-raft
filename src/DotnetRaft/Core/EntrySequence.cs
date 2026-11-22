using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal static class EntrySequence
{
    internal static List<Entry> CloneContiguous(
        IEnumerable<Entry> entries,
        Func<string, Exception> createGapError)
    {
        ArgumentNullException.ThrowIfNull(entries);

        var result = new List<Entry>();
        ulong? previous = null;
        foreach (Entry entry in entries)
        {
            previous = ValidateNext(entry, previous, nameof(entries), createGapError);
            result.Add(entry.Clone());
        }

        return result;
    }

    internal static void ValidateContiguous(
        IReadOnlyList<Entry> entries,
        Func<string, Exception> createGapError)
    {
        ArgumentNullException.ThrowIfNull(entries);

        ulong? previous = null;
        foreach (Entry entry in entries)
        {
            previous = ValidateNext(entry, previous, nameof(entries), createGapError);
        }
    }

    private static ulong ValidateNext(
        Entry entry,
        ulong? previous,
        string parameterName,
        Func<string, Exception> createGapError)
    {
        if (entry is null)
        {
            throw new ArgumentException("Entries cannot contain null values.", parameterName);
        }

        if (previous is ulong index
            && (index == ulong.MaxValue || entry.Index != index + 1))
        {
            string message = $"Entries are not contiguous at indexes {index} and {entry.Index}.";
            throw createGapError(message);
        }

        return entry.Index;
    }
}
