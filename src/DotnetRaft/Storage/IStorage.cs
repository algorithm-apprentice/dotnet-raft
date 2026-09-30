using DotnetRaft.Protocol;

namespace DotnetRaft.Storage;

public interface IStorage
{
    StorageState GetInitialState();

    IReadOnlyList<Entry> GetEntries(
        ulong lowInclusive,
        ulong highExclusive,
        ulong maxSize);

    ulong GetTerm(ulong index);

    ulong GetLastIndex();

    ulong GetFirstIndex();

    Snapshot GetSnapshot();
}
