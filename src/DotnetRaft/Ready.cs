using System.Collections.ObjectModel;

using DotnetRaft.Protocol;

namespace DotnetRaft;

/// <summary>Work handed to the host, not effects that have already happened.</summary>
/// <remarks>
/// In Ready/Advance mode, the host meets the persistence and application requirements before acknowledging the batch.
/// With asynchronous storage writes, drive writes and application from local requests in
/// <see cref="Messages"/>, not the mirrored fields, and do not call Advance.
/// Consume <see cref="ReadStates"/> in either mode.
/// </remarks>
public sealed class Ready
{
    private readonly ReadOnlyCollection<ReadState> _readStates;
    private readonly ReadOnlyCollection<Entry> _entries;
    private readonly ReadOnlyCollection<Entry> _committedEntries;
    private readonly ReadOnlyCollection<Message> _messages;

    internal static Ready FromDetachedWork(
        SoftState? softState,
        HardState? hardState,
        ReadState[] readStates,
        Entry[] entries,
        Snapshot? snapshot,
        Entry[] committedEntries,
        Message[] messages,
        bool mustSync) => new(softState, hardState, readStates, entries, snapshot, committedEntries, messages, mustSync);

    private Ready(
        SoftState? softState,
        HardState? hardState,
        ReadState[] readStates,
        Entry[] entries,
        Snapshot? snapshot,
        Entry[] committedEntries,
        Message[] messages,
        bool mustSync)
    {
        // The producer transfers detached values, including independent
        // mutable protobufs for each work/completion stream.
        SoftState = softState;
        HardState = hardState;
        Snapshot = snapshot;
        MustSync = mustSync;

        _readStates = Array.AsReadOnly(readStates);
        _entries = Array.AsReadOnly(entries);
        _committedEntries = Array.AsReadOnly(committedEntries);
        _messages = Array.AsReadOnly(messages);
    }

    public SoftState? SoftState { get; }

    public HardState? HardState { get; }

    public IReadOnlyList<ReadState> ReadStates =>
        _readStates;

    public IReadOnlyList<Entry> Entries => _entries;

    public Snapshot? Snapshot { get; }

    public IReadOnlyList<Entry> CommittedEntries =>
        _committedEntries;

    public IReadOnlyList<Message> Messages => _messages;

    public bool MustSync { get; }
}
