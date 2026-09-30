using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal readonly record struct EntryId(ulong Term, ulong Index)
{
    public static EntryId From(Entry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        return new EntryId(entry.Term, entry.Index);
    }
}
