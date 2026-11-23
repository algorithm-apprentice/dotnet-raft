using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal sealed class AppendStorageCompletion(
    RawNode owner,
    ulong nodeId,
    ulong term,
    EntryId? stableEntry,
    Snapshot? snapshot) : StorageCompletion(owner)
{
    private readonly Snapshot? snapshot = snapshot?.Clone();

    internal override void ApplyTo(RaftCore core) =>
        core.CompleteStorageAppend(term, stableEntry, snapshot?.Metadata.Index);

    internal override Message DescribeReceipt()
    {
        var transition = new Message
        {
            From = RaftLocalMessageTargets.AppendThread,
            To = nodeId,
            Type = MessageType.MsgStorageAppendResp,
            Term = term,
            Snapshot = snapshot?.Clone(),
        };
        if (stableEntry is { } stable)
        {
            transition.Index = stable.Index;
            transition.LogTerm = stable.Term;
        }

        return transition;
    }
}
