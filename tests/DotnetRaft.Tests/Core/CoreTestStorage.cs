using DotnetRaft.Protocol;
using DotnetRaft.Storage;

namespace DotnetRaft.Tests.Core;

internal sealed class CoreTestStorage : IStorage
{
    internal CoreTestStorage()
    {
        InitialState = new StorageState(null, new ConfState());
    }

    internal MemoryStorage LogStorage { get; } = new();

    internal StorageState? InitialState { get; set; }

    internal int CallCount { get; private set; }

    internal List<(
        ulong LowInclusive,
        ulong HighExclusive,
        ulong MaxSize)> EntryRequests
    { get; } = [];

    internal Func<ulong, ulong>? GetTermOverride { get; set; }

    internal Func<Snapshot>? GetSnapshotOverride { get; set; }

    public StorageState GetInitialState()
    {
        CallCount++;
        return InitialState!;
    }

    public IReadOnlyList<Entry> GetEntries(
        ulong lowInclusive,
        ulong highExclusive,
        ulong maxSize)
    {
        CallCount++;
        EntryRequests.Add((lowInclusive, highExclusive, maxSize));
        return LogStorage.GetEntries(
            lowInclusive,
            highExclusive,
            maxSize);
    }

    public ulong GetTerm(ulong index)
    {
        CallCount++;
        if (GetTermOverride is not null)
        {
            return GetTermOverride(index);
        }

        return LogStorage.GetTerm(index);
    }

    public ulong GetLastIndex()
    {
        CallCount++;
        return LogStorage.GetLastIndex();
    }

    public ulong GetFirstIndex()
    {
        CallCount++;
        return LogStorage.GetFirstIndex();
    }

    public Snapshot GetSnapshot()
    {
        CallCount++;
        if (GetSnapshotOverride is not null)
        {
            return GetSnapshotOverride();
        }

        return LogStorage.GetSnapshot();
    }
}
