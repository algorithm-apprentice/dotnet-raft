using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal static class MessageClassifier
{
    public static bool IsLocal(MessageType type)
    {
        return type is
            MessageType.MsgHup or
            MessageType.MsgBeat or
            MessageType.MsgUnreachable or
            MessageType.MsgSnapStatus or
            MessageType.MsgCheckQuorum or
            MessageType.MsgStorageAppend or
            MessageType.MsgStorageAppendResp or
            MessageType.MsgStorageApply or
            MessageType.MsgStorageApplyResp;
    }

    public static bool IsResponse(MessageType type)
    {
        return type is
            MessageType.MsgAppResp or
            MessageType.MsgVoteResp or
            MessageType.MsgHeartbeatResp or
            MessageType.MsgUnreachable or
            MessageType.MsgReadIndexResp or
            MessageType.MsgPreVoteResp or
            MessageType.MsgStorageAppendResp or
            MessageType.MsgStorageApplyResp;
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
