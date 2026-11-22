using DotnetRaft.ConfChange;
using DotnetRaft.Protocol;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Core;

internal static class NetworkMessageValidation
{
    internal static LogSlice? Prepare(
        Message message,
        ulong lastIndex,
        bool handlesLeaderMessages,
        bool handlesLeaderResponses,
        Action<Snapshot?> validateSnapshot)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(
            validateSnapshot);

        ValidateType(message);

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
            case MessageType.MsgProp:
                ValidateProposal(message);
                break;
            case MessageType.MsgApp:
                return LogSlice.ParseOwnedPeerAppend(message);
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
            case MessageType.MsgSnap:
                if (handlesLeaderMessages)
                {
                    validateSnapshot(
                        message.Snapshot);
                }

                break;
        }

        return null;
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

    private static void ValidateProposal(
        Message message)
    {
        if (message.Entries.Count == 0)
        {
            throw Invalid(
                "A proposal must contain at least one entry.");
        }

        foreach (Entry entry in message.Entries)
        {
            ValidateEntry(entry);
        }
    }

    internal static void ValidateEntry(
        Entry entry)
    {
        if (!Enum.IsDefined(entry.Type))
        {
            throw Invalid(
                $"Entry type {(int)entry.Type} is unknown.");
        }

        try
        {
            switch (entry.Type)
            {
                case EntryType.EntryConfChange:
                    ConfigurationChangeValidation
                        .ValidateProposal(
                            ProtocolConfChange.Parser
                                .ParseFrom(entry.Data),
                            nameof(entry));
                    break;
                case EntryType.EntryConfChangeV2:
                    ConfigurationChangeValidation
                        .ValidateProposal(
                            ConfChangeV2.Parser
                                .ParseFrom(entry.Data),
                            nameof(entry));
                    break;
            }
        }
        catch (InvalidProtocolBufferException exception)
        {
            throw new ArgumentException(
                $"Configuration entry payload is malformed: {exception.Message}",
                nameof(entry),
                exception);
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
