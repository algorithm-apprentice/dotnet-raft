using System.Buffers.Binary;

using DotnetRaft.Core;

using Google.Protobuf;

namespace DotnetRaft.Read;

// A position in one leader's read stream, not a log index or client context.
internal readonly struct ReadAcknowledgement
{
    // Default means no evidence inside dispatch. It has no readable position
    // and must never be passed to the tracker as an acknowledgement.
    private readonly ulong position;

    private ReadAcknowledgement(ulong position)
    {
        this.position = position;
    }

    internal ulong Position => position != 0
        ? position
        : throw new RaftInvariantException("Read acknowledgement was not initialized.");

    internal bool IsPresent => position != 0;

    internal static ReadAcknowledgement? Parse(
        ByteString context,
        ulong latestPosition,
        bool fromPeer)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.IsEmpty)
        {
            return null;
        }

        if (context.Length != sizeof(ulong))
        {
            throw Invalid(
                "Read-only acknowledgement context must contain exactly eight bytes.",
                fromPeer,
                nameof(context));
        }

        ulong position = BinaryPrimitives.ReadUInt64LittleEndian(context.Span);
        if (position == 0 || position > latestPosition)
        {
            throw Invalid(
                $"Read-only acknowledgement position {position} is outside the valid range 1..{latestPosition}.",
                fromPeer,
                nameof(context));
        }

        return new ReadAcknowledgement(position);
    }

    internal static ReadAcknowledgement FromRegistration(ulong position)
    {
        if (position == 0)
        {
            throw new RaftInvariantException("A registered read must have a positive position.");
        }

        return new ReadAcknowledgement(position);
    }

    private static Exception Invalid(string message, bool fromPeer, string parameterName)
    {
        return fromPeer
            ? new ArgumentException(message, parameterName)
            : new RaftInvariantException(message);
    }
}
