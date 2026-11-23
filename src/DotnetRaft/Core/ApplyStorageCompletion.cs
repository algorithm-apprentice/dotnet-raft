using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal sealed class ApplyStorageCompletion(RawNode owner, ulong nodeId, Entry[] entries) : StorageCompletion(owner)
{
    private readonly Entry[] entries = ProtocolCloning.CloneEntries(entries);

    internal override void ApplyTo(RaftCore core) => core.CompleteStorageApply(entries);

    internal override Message DescribeReceipt()
    {
        var transition = new Message
        {
            From = RaftLocalMessageTargets.ApplyThread,
            To = nodeId,
            Type = MessageType.MsgStorageApplyResp,
            Term = 0,
        };
        transition.Entries.AddRange(entries.Select(entry => entry.Clone()));
        return transition;
    }
}
