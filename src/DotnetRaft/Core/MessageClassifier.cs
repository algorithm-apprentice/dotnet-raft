using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal static class MessageClassifier
{
    internal static bool HasKnownType(Message message)
    {
        return message.HasType && Enum.IsDefined(message.Type);
    }

    public static bool IsLocal(MessageType type)
    {
        return type is
            MessageType.MsgHup or
            MessageType.MsgBeat or
            MessageType.MsgUnreachable or
            MessageType.MsgSnapStatus or
            MessageType.MsgCheckQuorum or
            MessageType.MsgStorageAppend or
            MessageType.MsgStorageApply
            || IsStorageResponse(type);
    }

    public static bool IsResponse(MessageType type)
    {
        return type is
            MessageType.MsgAppResp or
            MessageType.MsgVoteResp or
            MessageType.MsgHeartbeatResp or
            MessageType.MsgUnreachable or
            MessageType.MsgReadIndexResp or
            MessageType.MsgPreVoteResp
            || IsStorageResponse(type);
    }

    internal static bool IsStorageResponse(MessageType type)
    {
        return type is MessageType.MsgStorageAppendResp or
            MessageType.MsgStorageApplyResp;
    }

    internal static bool IsVoteMessage(MessageType type)
    {
        return type is MessageType.MsgVote or MessageType.MsgVoteResp
            or MessageType.MsgPreVote or MessageType.MsgPreVoteResp;
    }

    public static MessageType VoteResponseType(MessageType type)
    {
        return type switch
        {
            MessageType.MsgVote => MessageType.MsgVoteResp,
            MessageType.MsgPreVote => MessageType.MsgPreVoteResp,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Expected a vote message."),
        };
    }
}
