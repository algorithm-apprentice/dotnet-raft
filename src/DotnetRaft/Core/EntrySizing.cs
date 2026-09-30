using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal static class EntrySizing
{
    public static ulong EncodedSize(IEnumerable<Entry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        ulong size = 0;
        foreach (var entry in entries)
        {
            ArgumentNullException.ThrowIfNull(entry);
            size = checked(size + (ulong)entry.CalculateSize());
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

    public static IReadOnlyList<Entry> LimitSize(IReadOnlyList<Entry> entries, ulong maxSize)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count == 0)
        {
            return [];
        }

        var size = (ulong)entries[0].CalculateSize();
        var limit = 1;

        while (limit < entries.Count)
        {
            var nextSize = (ulong)entries[limit].CalculateSize();
            if (size > maxSize || nextSize > maxSize - size)
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
}
