using System.Buffers.Binary;

using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Quorum;

using Google.Protobuf;

namespace DotnetRaft.Read;

internal sealed class ReadOnlyTracker : IAckedIndexer
{
    private readonly Dictionary<ulong, ulong> acknowledgements = [];
    private readonly Queue<ReadIndexRequest> pendingRequests = [];
    private ulong confirmedCount;

    internal ReadOnlyTracker(ReadOnlyOption option)
    {
        Option = option;
    }

    internal ReadOnlyOption Option { get; }

    internal int PendingCount => pendingRequests.Count;

    internal ulong ConfirmedCount => confirmedCount;

    internal void AddRequest(ulong commitIndex, Message request)
    {
        ArgumentNullException.ThrowIfNull(request);

        ulong currentPosition = GetCurrentPosition();
        if (currentPosition == ulong.MaxValue)
        {
            throw new RaftInvariantException(
                "Read-only request position overflowed.");
        }

        pendingRequests.Enqueue(new ReadIndexRequest(
            request.Clone(),
            commitIndex));
    }

    internal void ReceiveAcknowledgement(
        ulong from,
        ByteString context)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.Length == 0)
        {
            return;
        }

        if (context.Length != sizeof(ulong))
        {
            throw new RaftInvariantException(
                "Read-only acknowledgement context must contain exactly eight bytes.");
        }

        ulong position = BinaryPrimitives.ReadUInt64LittleEndian(
            context.Span);
        ulong currentPosition = GetCurrentPosition();
        if (position == 0 || position > currentPosition)
        {
            throw new RaftInvariantException(
                $"Read-only acknowledgement position {position} is outside " +
                $"the valid range 1..{currentPosition}.");
        }

        if (!acknowledgements.TryGetValue(from, out ulong acknowledged)
            || position > acknowledged)
        {
            acknowledgements[from] = position;
        }
    }

    internal void ValidateAcknowledgementContext(
        ByteString context)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.Length == 0)
        {
            return;
        }

        if (context.Length != sizeof(ulong))
        {
            throw new ArgumentException(
                "Read-only acknowledgement context must contain exactly eight bytes.",
                nameof(context));
        }

        ulong position =
            BinaryPrimitives.ReadUInt64LittleEndian(
                context.Span);
        ulong currentPosition = GetCurrentPosition();
        if (position == 0
            || position > currentPosition)
        {
            throw new ArgumentException(
                $"Read-only acknowledgement position {position} is outside " +
                $"the valid range 1..{currentPosition}.",
                nameof(context));
        }
    }

    internal ReadIndexRequest[] Advance(JointConfig voters)
    {
        ArgumentNullException.ThrowIfNull(voters);

        ulong newConfirmedCount = voters.CommittedIndex(this);
        if (newConfirmedCount <= confirmedCount)
        {
            return [];
        }

        ulong currentPosition = GetCurrentPosition();
        if (newConfirmedCount > currentPosition)
        {
            throw new RaftInvariantException(
                $"Read-only quorum position {newConfirmedCount} exceeds " +
                $"the latest request position {currentPosition}.");
        }

        int releaseCount = checked(
            (int)(newConfirmedCount - confirmedCount));
        var confirmed = new ReadIndexRequest[releaseCount];
        for (var index = 0; index < confirmed.Length; index++)
        {
            confirmed[index] = pendingRequests.Dequeue();
        }

        confirmedCount = newConfirmedCount;
        return confirmed;
    }

    internal ByteString GetHeartbeatContext()
    {
        if (pendingRequests.Count == 0)
        {
            return ByteString.Empty;
        }

        var context = new byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64LittleEndian(
            context,
            GetCurrentPosition());
        return ByteString.CopyFrom(context);
    }

    bool IAckedIndexer.TryGetAckedIndex(
        ulong voterId,
        out ulong index)
    {
        return acknowledgements.TryGetValue(voterId, out index);
    }

    private ulong GetCurrentPosition()
    {
        ulong pendingCount = (ulong)pendingRequests.Count;
        if (confirmedCount > ulong.MaxValue - pendingCount)
        {
            throw new RaftInvariantException(
                "Read-only request position overflowed.");
        }

        return confirmedCount + pendingCount;
    }
}
