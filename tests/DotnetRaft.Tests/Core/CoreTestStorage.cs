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
        return LogStorage.GetEntries(
            lowInclusive,
            highExclusive,
            maxSize);
    }

    public ulong GetTerm(ulong index)
    {
        CallCount++;
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
        return LogStorage.GetSnapshot();
    }
}
