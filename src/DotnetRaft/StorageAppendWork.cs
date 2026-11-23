using DotnetRaft.Core;
using DotnetRaft.Protocol;

namespace DotnetRaft;

/// <summary>Local durable publication work. This is not a peer message.</summary>
public sealed class StorageAppendWork
{
    private readonly ulong nodeId;

    internal StorageAppendWork(ulong nodeId, RaftHardState? hardState, Entry[] entries,
        Snapshot? snapshot, Message[] afterPersist, StorageCompletion? completion)
    {
        this.nodeId = nodeId;
        HardState = hardState;
        Entries = Array.AsReadOnly(ProtocolCloning.CloneEntries(entries));
        Snapshot = snapshot?.Clone();
        MessagesAfterPersist = Array.AsReadOnly(afterPersist.Select(message => message.Clone()).ToArray());
        Completion = completion;
    }

    public RaftHardState? HardState { get; }
    public IReadOnlyList<Entry> Entries { get; }
    public Snapshot? Snapshot { get; }

    /// <summary>Release in order only after publication; step self-addressed replies locally.</summary>
    public IReadOnlyList<Message> MessagesAfterPersist { get; }

    /// <summary>Return after publication and after releasing the ordered responses.</summary>
    public StorageCompletion? Completion { get; }

    internal Message DescribeWork()
    {
        var description = new Message
        {
            From = nodeId,
            To = RaftLocalMessageTargets.AppendThread,
            Type = MessageType.MsgStorageAppend,
            Snapshot = Snapshot,
        };
        if (HardState is { } state)
        {
            StorageAppendHardState.Write(description, state);
        }

        description.Entries.AddRange(Entries);
        description.Responses.AddRange(MessagesAfterPersist);
        if (Completion is not null)
        {
            description.Responses.Add(Completion.DescribeReceipt());
        }

        return description;
    }
}
