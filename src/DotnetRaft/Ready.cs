using System.Collections.ObjectModel;

using DotnetRaft.Protocol;

namespace DotnetRaft;

public sealed class Ready
{
    private readonly ReadOnlyCollection<ReadState> _readStates;
    private readonly ReadOnlyCollection<Entry> _entries;
    private readonly ReadOnlyCollection<Entry> _committedEntries;
    private readonly ReadOnlyCollection<Message> _messages;

    internal Ready(
        SoftState? softState,
        HardState? hardState,
        IEnumerable<ReadState> readStates,
        IEnumerable<Entry> entries,
        Snapshot? snapshot,
        IEnumerable<Entry> committedEntries,
        IEnumerable<Message> messages,
        bool mustSync)
    {
        ArgumentNullException.ThrowIfNull(readStates);
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(committedEntries);
        ArgumentNullException.ThrowIfNull(messages);

        SoftState = softState is null
            ? null
            : new SoftState(
                softState.LeaderId,
                softState.Role);
        HardState = hardState?.Clone();
        Snapshot = snapshot?.Clone();
        MustSync = mustSync;

        _readStates = Array.AsReadOnly(
            readStates
                .Select(Clone)
                .ToArray());
        _entries = Array.AsReadOnly(
            entries
                .Select(entry => entry.Clone())
                .ToArray());
        _committedEntries = Array.AsReadOnly(
            committedEntries
                .Select(entry => entry.Clone())
                .ToArray());
        _messages = Array.AsReadOnly(
            messages
                .Select(message => message.Clone())
                .ToArray());
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

    private static ReadState Clone(ReadState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        return new ReadState(
            state.Index,
            state.RequestContext);
    }
}
