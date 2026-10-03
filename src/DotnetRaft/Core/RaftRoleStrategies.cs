using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal interface IRaftRoleStrategy
{
    void Handle(
        RaftCore core,
        Message message);
}

internal static class RaftRoleStrategies
{
    internal static IRaftRoleStrategy Resolve(
        RaftRole role)
    {
        return role switch
        {
            RaftRole.Follower =>
                FollowerRoleStrategy.Instance,
            RaftRole.PreCandidate =>
                PreCandidateRoleStrategy.Instance,
            RaftRole.Candidate =>
                CandidateRoleStrategy.Instance,
            RaftRole.Leader =>
                LeaderRoleStrategy.Instance,
            _ => throw new RaftInvariantException(
                $"Unknown Raft role {role}."),
        };
    }
}

internal sealed class FollowerRoleStrategy
    : IRaftRoleStrategy
{
    internal static FollowerRoleStrategy Instance { get; } =
        new();

    private FollowerRoleStrategy()
    {
    }

    public void Handle(
        RaftCore core,
        Message message)
    {
        switch (message.Type)
        {
            case MessageType.MsgHup:
                core.HandleHup();
                return;
            case MessageType.MsgProp:
                core.HandleFollowerProposal(message);
                return;
            case MessageType.MsgForgetLeader:
                core.HandleFollowerForgetLeader();
                return;
            case MessageType.MsgTransferLeader:
                core.HandleFollowerTransferLeader(
                    message);
                return;
            case MessageType.MsgTimeoutNow:
                core.HandleFollowerTimeoutNow();
                return;
            case MessageType.MsgReadIndex:
                core.HandleFollowerReadIndex(message);
                return;
            case MessageType.MsgReadIndexResp:
                core.HandleFollowerReadIndexResponse(
                    message);
                return;
            case MessageType.MsgApp:
            case MessageType.MsgHeartbeat:
            case MessageType.MsgSnap:
                core.HandleFollowerLeaderMessage(
                    message);
                return;
        }
    }
}

internal sealed class PreCandidateRoleStrategy
    : IRaftRoleStrategy
{
    internal static PreCandidateRoleStrategy Instance { get; } =
        new();

    private PreCandidateRoleStrategy()
    {
    }

    public void Handle(
        RaftCore core,
        Message message)
    {
        switch (message.Type)
        {
            case MessageType.MsgHup:
                core.HandleHup();
                return;
            case MessageType.MsgProp:
                core.HandleCampaigningProposal(
                    message);
                return;
            case MessageType.MsgApp:
            case MessageType.MsgHeartbeat:
            case MessageType.MsgSnap:
                core.HandleCampaigningLeaderMessage(
                    message);
                return;
            case MessageType.MsgPreVoteResp:
                core.HandlePreCandidateVoteResponse(
                    message);
                return;
        }
    }
}

internal sealed class CandidateRoleStrategy
    : IRaftRoleStrategy
{
    internal static CandidateRoleStrategy Instance { get; } =
        new();

    private CandidateRoleStrategy()
    {
    }

    public void Handle(
        RaftCore core,
        Message message)
    {
        switch (message.Type)
        {
            case MessageType.MsgHup:
                core.HandleHup();
                return;
            case MessageType.MsgProp:
                core.HandleCampaigningProposal(
                    message);
                return;
            case MessageType.MsgApp:
            case MessageType.MsgHeartbeat:
            case MessageType.MsgSnap:
                core.HandleCampaigningLeaderMessage(
                    message);
                return;
            case MessageType.MsgVoteResp:
                core.HandleCandidateVoteResponse(
                    message);
                return;
        }
    }
}

internal sealed class LeaderRoleStrategy
    : IRaftRoleStrategy
{
    internal static LeaderRoleStrategy Instance { get; } =
        new();

    private LeaderRoleStrategy()
    {
    }

    public void Handle(
        RaftCore core,
        Message message)
    {
        switch (message.Type)
        {
            case MessageType.MsgProp:
                core.HandleLeaderProposal(message);
                return;
            case MessageType.MsgBeat:
                core.BroadcastHeartbeat();
                return;
            case MessageType.MsgCheckQuorum:
                core.HandleCheckQuorum();
                return;
            case MessageType.MsgTransferLeader:
                core.HandleLeaderTransfer(
                    message.From);
                return;
            case MessageType.MsgAppResp:
                core.HandleAppendResponse(message);
                return;
            case MessageType.MsgHeartbeatResp:
                core.HandleHeartbeatResponse(
                    message);
                return;
            case MessageType.MsgReadIndex:
                core.HandleLeaderReadIndex(message);
                return;
            case MessageType.MsgUnreachable:
                core.HandleUnreachable(message);
                return;
            case MessageType.MsgSnapStatus:
                core.HandleSnapshotStatus(message);
                return;
        }
    }
}
