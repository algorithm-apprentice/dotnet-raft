using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal sealed class RaftOutput
{
    private readonly List<Message> messages = [];
    private readonly List<Message> messagesAfterAppend = [];

    internal bool HasMessages =>
        messages.Count > 0;

    internal bool HasMessagesAfterAppend =>
        messagesAfterAppend.Count > 0;

    internal Message Enqueue(
        Message message,
        ulong senderId,
        ulong currentTerm)
    {
        ArgumentNullException.ThrowIfNull(message);
        Message outbound = message.Clone();

        if (outbound.From == RaftMessageTargets.None)
        {
            outbound.From = senderId;
        }

        if (IsVoteMessage(outbound.Type))
        {
            if (outbound.Term == 0)
            {
                throw new RaftInvariantException(
                    $"Term must be set when sending {outbound.Type}.");
            }
        }
        else
        {
            if (outbound.Term != 0)
            {
                throw new RaftInvariantException(
                    $"Term must not be set when sending {outbound.Type}.");
            }

            if (outbound.Type is not MessageType.MsgProp
                and not MessageType.MsgReadIndex)
            {
                outbound.Term = currentTerm;
            }
        }

        if (RequiresDurableState(outbound.Type))
        {
            messagesAfterAppend.Add(outbound);
            return outbound;
        }

        if (outbound.To == senderId)
        {
            throw new RaftInvariantException(
                $"Immediate outbound {outbound.Type} cannot target the local node.");
        }

        messages.Add(outbound);
        return outbound;
    }

    internal Message[] TakeMessages()
    {
        return Take(messages);
    }

    internal Message[] PeekMessages()
    {
        return CloneMessages(messages);
    }

    internal Message[] TakeMessagesAfterAppend()
    {
        return Take(messagesAfterAppend);
    }

    internal Message[] PeekMessagesAfterAppend()
    {
        return CloneMessages(messagesAfterAppend);
    }

    private static bool IsVoteMessage(MessageType type)
    {
        return type is
            MessageType.MsgVote or
            MessageType.MsgVoteResp or
            MessageType.MsgPreVote or
            MessageType.MsgPreVoteResp;
    }

    private static bool RequiresDurableState(MessageType type)
    {
        return type is
            MessageType.MsgAppResp or
            MessageType.MsgVoteResp or
            MessageType.MsgPreVoteResp;
    }

    private static Message[] Take(
        List<Message> queue)
    {
        Message[] taken = [.. queue];
        queue.Clear();
        return taken;
    }

    private static Message[] CloneMessages(
        IEnumerable<Message> source)
    {
        return
        [
            .. source.Select(
                message => message.Clone()),
        ];
    }
}
