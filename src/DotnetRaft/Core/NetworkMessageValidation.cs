using DotnetRaft.ConfChange;
using DotnetRaft.Protocol;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Core;

internal static class NetworkMessageValidation
{
    internal static void Validate(
        Message message,
        ulong lastIndex,
        bool handlesLeaderMessages,
        bool handlesLeaderResponses,
        Action<ByteString> validateReadContext,
        Action<Snapshot?> validateSnapshot)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(
            validateReadContext);
        ArgumentNullException.ThrowIfNull(
            validateSnapshot);

        ValidateType(message);

        if (IsVoteMessage(message.Type)
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
                ValidateAppend(message);
                break;
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
            case MessageType.MsgHeartbeatResp:
                if (handlesLeaderResponses)
                {
                    validateReadContext(
                        message.Context);
                }

                break;
        }
    }

    internal static void ValidateType(
        Message message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (!message.HasType
            || !Enum.IsDefined(message.Type))
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

    private static void ValidateAppend(
        Message message)
    {
        ulong previousIndex = message.Index;
        ulong previousTerm = message.LogTerm;
        foreach (Entry entry in message.Entries)
        {
            ValidateEntry(entry);
            if (previousIndex == ulong.MaxValue)
            {
                throw Invalid(
                    $"Append entries cannot follow index {ulong.MaxValue}.");
            }

            ulong expectedIndex =
                previousIndex + 1;
            if (entry.Index != expectedIndex)
            {
                throw Invalid(
                    $"Append entry index {entry.Index} does not follow {previousIndex}.");
            }

            if (entry.Index == ulong.MaxValue)
            {
                throw Invalid(
                    $"Append entry index {ulong.MaxValue} has no representable successor.");
            }

            if (entry.Term < previousTerm)
            {
                throw Invalid(
                    $"Append entry term {entry.Term} precedes prior term {previousTerm}.");
            }

            if (entry.Term > message.Term)
            {
                throw Invalid(
                    $"Append entry term {entry.Term} exceeds message term {message.Term}.");
            }

            previousIndex = entry.Index;
            previousTerm = entry.Term;
        }

        if (message.Term < previousTerm)
        {
            throw Invalid(
                $"Append message term {message.Term} precedes prior term {previousTerm}.");
        }
    }

    private static void ValidateEntry(
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

    private static bool IsVoteMessage(
        MessageType type)
    {
        return type is
            MessageType.MsgVote or
            MessageType.MsgVoteResp or
            MessageType.MsgPreVote or
            MessageType.MsgPreVoteResp;
    }

    private static ArgumentException Invalid(
        string message)
    {
        return new ArgumentException(
            message,
            nameof(message));
    }
}
