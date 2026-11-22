using DotnetRaft.Protocol;

namespace DotnetRaft.Storage;

/// <summary>The synchronous read side of a node's stable Raft log.</summary>
/// <remarks>The host publishes Ready writes separately. Reads must see a coherent view even with asynchronous storage writes.</remarks>
public interface IStorage
{
    StorageState GetInitialState();

    /// <summary>Reads a contiguous prefix of the half-open range, limited by encoded entry bytes.</summary>
    /// <remarks>A nonempty retained range returns its first entry even if it exceeds the byte budget.</remarks>
    IReadOnlyList<Entry> GetEntries(
        ulong lowInclusive,
        ulong highExclusive,
        ulong maxSize);

    ulong GetTerm(ulong index);

    ulong GetLastIndex();

    /// <summary>The first entry after the retained boundary, whose term remains readable at the preceding index.</summary>
    ulong GetFirstIndex();

    Snapshot GetSnapshot();
}
