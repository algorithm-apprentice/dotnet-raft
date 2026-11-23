using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal static class EntrySizing
{
    public static ulong EncodedSize(IRaftLogFormat format, IEnumerable<Entry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        ulong size = 0;
        foreach (var entry in entries)
        {
            ArgumentNullException.ThrowIfNull(entry);
            size = checked(size + format.MeasureEntry(entry));
        }

        return size;
    }

    public static ulong PayloadSize(Entry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        return (ulong)entry.Data.Length;
    }

    public static ulong PayloadSize(IEnumerable<Entry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        ulong size = 0;
        foreach (var entry in entries)
        {
            size = checked(size + PayloadSize(entry));
        }

        return size;
    }

    public static IReadOnlyList<Entry> LimitSize(IRaftLogFormat format, IReadOnlyList<Entry> entries, ulong maxSize)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count == 0)
        {
            return [];
        }

        ulong size = format.MeasureEntry(entries[0]);
        var limit = 1;

        while (limit < entries.Count)
        {
            ulong nextSize = format.MeasureEntry(entries[limit]);
            if (!FitsWithinBudget(size, nextSize, maxSize))
            {
                break;
            }

            size += nextSize;
            limit++;
        }

        var result = new Entry[limit];
        for (var index = 0; index < limit; index++)
        {
            result[index] = entries[index];
        }

        return result;
    }

    internal static bool FitsWithinBudget(ulong usedSize, ulong nextSize, ulong maxSize)
    {
        return usedSize <= maxSize && nextSize <= maxSize - usedSize;
    }
}
