using DotnetRaft.Protocol;

using Google.Protobuf;

using ProtocolConfChange =
    DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Examples.KvCluster;

public sealed class FixedMembershipConfiguration
{
    private readonly ulong[] peerIds;

    public FixedMembershipConfiguration(
        IEnumerable<ulong> peerIds)
    {
        ArgumentNullException.ThrowIfNull(peerIds);
        this.peerIds =
        [
            .. peerIds.Order(),
        ];
        if (this.peerIds.Length != 3
            || this.peerIds[0] == 0
            || this.peerIds.Distinct().Count()
                != this.peerIds.Length)
        {
            throw new ArgumentException(
                "Fixed membership requires three distinct nonzero peer IDs.",
                nameof(peerIds));
        }
    }

    public bool IsBootstrapIndex(ulong index)
    {
        return index >= 1
            && index <= (ulong)peerIds.Length;
    }

    public ProtocolConfChange ValidateBootstrapEntry(
        Entry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        if (!IsBootstrapIndex(entry.Index))
        {
            throw new ArgumentOutOfRangeException(
                nameof(entry),
                entry.Index,
                "Entry is outside the bootstrap prefix.");
        }

        int position = checked(
            (int)entry.Index - 1);
        if (entry.Term != 1
            || entry.Type !=
                EntryType.EntryConfChange)
        {
            throw InvalidBootstrap(
                entry.Index,
                peerIds[position]);
        }

        ProtocolConfChange change;
        try
        {
            change =
                ProtocolConfChange.Parser.ParseFrom(
                    entry.Data);
        }
        catch (InvalidProtocolBufferException exception)
        {
            throw new InvalidDataException(
                $"Bootstrap configuration entry {entry.Index} is malformed.",
                exception);
        }

        if (change.Type !=
                ConfChangeType.ConfChangeAddNode
            || change.NodeId != peerIds[position]
            || !change.Context.IsEmpty)
        {
            throw InvalidBootstrap(
                entry.Index,
                peerIds[position]);
        }

        return change;
    }

    private static InvalidDataException InvalidBootstrap(
        ulong index,
        ulong expectedId)
    {
        return new InvalidDataException(
            $"Bootstrap configuration entry {index} does not match peer {expectedId}.");
    }
}
