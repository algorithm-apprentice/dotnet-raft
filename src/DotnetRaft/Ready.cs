using System.Collections.ObjectModel;

using DotnetRaft.Protocol;

namespace DotnetRaft;

/// <summary>Work handed to the host, not effects that have already happened.</summary>
/// <remarks>
/// In Ready/Advance mode, the host meets the persistence and application requirements before acknowledging the batch.
/// With asynchronous storage writes, use <see cref="AppendWork"/> and <see cref="ApplyWork"/>,
/// not the mirrored fields, and return their completions instead of calling Advance.
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
        RaftHardState? hardState,
        ReadState[] readStates,
        Entry[] entries,
        Snapshot? snapshot,
        Entry[] committedEntries,
        Message[] messages,
        bool mustSync,
        StorageAppendWork? appendWork = null,
        StorageApplyWork? applyWork = null) =>
        new(softState, hardState, readStates, entries, snapshot, committedEntries, messages, mustSync, appendWork, applyWork);

    private Ready(
        SoftState? softState,
        RaftHardState? hardState,
        ReadState[] readStates,
        Entry[] entries,
        Snapshot? snapshot,
        Entry[] committedEntries,
        Message[] messages,
        bool mustSync,
        StorageAppendWork? appendWork,
        StorageApplyWork? applyWork)
    {
        // The producer transfers detached values, including independent
        // mutable entries and messages for each work/completion stream.
        SoftState = softState;
        HardState = hardState;
        Snapshot = snapshot;
        MustSync = mustSync;
        AppendWork = appendWork;
        ApplyWork = applyWork;

        _readStates = Array.AsReadOnly(readStates);
        _entries = Array.AsReadOnly(entries);
        _committedEntries = Array.AsReadOnly(committedEntries);
        _messages = Array.AsReadOnly(messages);
    }

    public SoftState? SoftState { get; }

    /// <summary>The complete state to publish, or null if this batch has no hard-state update.</summary>
    public RaftHardState? HardState { get; }

    public IReadOnlyList<ReadState> ReadStates =>
        _readStates;

    public IReadOnlyList<Entry> Entries => _entries;

    public Snapshot? Snapshot { get; }

    public IReadOnlyList<Entry> CommittedEntries =>
        _committedEntries;

    public IReadOnlyList<Message> Messages => _messages;

    public StorageAppendWork? AppendWork { get; }

    public StorageApplyWork? ApplyWork { get; }

    public bool MustSync { get; }

    internal IEnumerable<Message> DescribeMessages()
    {
        foreach (Message message in _messages) yield return message;
        if (AppendWork is not null) yield return AppendWork.DescribeWork();
        if (ApplyWork is not null) yield return ApplyWork.DescribeWork();
    }
}
