using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal static class NetworkMessageValidation
{
    internal static bool IsStorageInput(Message message) =>
        MessageClassifier.IsStorageResponse(message.Type) || RaftMessageTargets.IsLocal(message.From);

    // RawNode has already checked that the owned message has a known type.
    internal static void ValidatePeerFields(
        Message message,
        ulong lastIndex,
        bool handlesLeaderMessages,
        bool handlesLeaderResponses)
    {
        if (MessageClassifier.IsVoteMessage(message.Type)
            && message.Term == 0)
        {
            throw Invalid(
                $"{message.Type} must carry a nonzero term.");
        }

        if (message.Type is
                MessageType.MsgProp or
                MessageType.MsgReadIndex
            && message.Term != 0)
        {
            throw Invalid(
                $"{message.Type} must not carry a term.");
        }

        switch (message.Type)
        {
            case MessageType.MsgAppResp:
                if (handlesLeaderResponses
                    && !message.Reject
                    && message.Index > lastIndex)
                {
                    throw Invalid(
                        $"Append acknowledgement {message.Index} exceeds last index {lastIndex}.");
                }

                break;
            case MessageType.MsgHeartbeat:
                if (handlesLeaderMessages
                    && message.Commit > lastIndex)
                {
                    throw Invalid(
                        $"Heartbeat commit {message.Commit} exceeds last index {lastIndex}.");
                }

                break;
            case MessageType.MsgReadIndex:
                if (message.Entries.Count != 1)
                {
                    throw Invalid(
                        $"{MessageType.MsgReadIndex} must contain exactly one entry.");
                }

                break;
        }
    }

    internal static void ValidateType(
        Message message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (!MessageClassifier.HasKnownType(message))
        {
            throw Invalid(
                "Raft message type must be present and known.");
        }
    }

    internal static ArgumentException Invalid(
        string message)
    {
        return new ArgumentException(
            message,
            nameof(message));
    }
}
