namespace DotnetRaft.Protocol;

public enum MessageType
{
    MsgHup,
    MsgBeat,
    MsgProp,
    MsgApp,
    MsgAppResp,
    MsgVote,
    MsgVoteResp,
    MsgSnap,
    MsgHeartbeat,
    MsgHeartbeatResp,
    MsgUnreachable,
    MsgSnapStatus,
    MsgCheckQuorum,
    MsgTransferLeader,
    MsgTimeoutNow,
    MsgReadIndex,
    MsgReadIndexResp,
    MsgPreVote,
    MsgPreVoteResp,
    MsgStorageAppend,
    MsgStorageAppendResp,
    MsgStorageApply,
    MsgStorageApplyResp,
    MsgForgetLeader,
}
