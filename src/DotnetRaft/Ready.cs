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
