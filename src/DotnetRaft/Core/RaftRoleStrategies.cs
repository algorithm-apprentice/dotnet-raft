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
        core.HandleRoleMessageLegacy(message);
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
        core.HandleRoleMessageLegacy(message);
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
        core.HandleRoleMessageLegacy(message);
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
        core.HandleRoleMessageLegacy(message);
    }
}
