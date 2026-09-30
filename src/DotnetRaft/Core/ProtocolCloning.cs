using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal static class ProtocolCloning
{
    public static Entry[] CloneEntries(IEnumerable<Entry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        return entries.Select(entry =>
        {
            ArgumentNullException.ThrowIfNull(entry);
            return entry.Clone();
        }).ToArray();
    }
}
