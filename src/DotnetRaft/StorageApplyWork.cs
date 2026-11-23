using DotnetRaft.Core;
using DotnetRaft.Protocol;

namespace DotnetRaft;

/// <summary>Local application work for an ordered range of committed entries.</summary>
public sealed class StorageApplyWork
{
    private readonly ulong nodeId;

    internal StorageApplyWork(ulong nodeId, Entry[] entries, StorageCompletion completion)
    {
        this.nodeId = nodeId;
        Entries = Array.AsReadOnly(ProtocolCloning.CloneEntries(entries));
        Completion = completion;
    }

    public IReadOnlyList<Entry> Entries { get; }

    /// <summary>Return only after publishing the application effects.</summary>
    public StorageCompletion Completion { get; }

    internal Message DescribeWork()
    {
        var description = new Message
        {
            From = nodeId,
            To = RaftLocalMessageTargets.ApplyThread,
            Type = MessageType.MsgStorageApply,
            Term = 0,
        };
        description.Entries.AddRange(Entries);
        description.Responses.Add(Completion.DescribeReceipt());
        return description;
    }
}
