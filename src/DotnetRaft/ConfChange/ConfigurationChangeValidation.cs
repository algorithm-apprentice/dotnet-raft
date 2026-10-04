using DotnetRaft.Protocol;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.ConfChange;

internal static class ConfigurationChangeValidation
{
    internal static void ValidateProposal(
        ProtocolConfChange change,
        string parameterName)
    {
        ArgumentNullException.ThrowIfNull(change);
        if (!Enum.IsDefined(change.Type))
        {
            throw new ArgumentException(
                $"Configuration change type {(int)change.Type} is unknown.",
                parameterName);
        }

        ValidateProposalNodeId(
            change.NodeId,
            parameterName);
    }

    internal static void ValidateProposal(
        ConfChangeV2 change,
        string parameterName)
    {
        ArgumentNullException.ThrowIfNull(change);
        if (!Enum.IsDefined(change.Transition))
        {
            throw new ArgumentException(
                $"Configuration transition {(int)change.Transition} is unknown.",
                parameterName);
        }

        foreach (ConfChangeSingle single in change.Changes)
        {
            if (!Enum.IsDefined(single.Type))
            {
                throw new ArgumentException(
                    $"Configuration change type {(int)single.Type} is unknown.",
                    parameterName);
            }

            ValidateProposalNodeId(
                single.NodeId,
                parameterName);
        }
    }

    internal static void ValidateApplied(
        ConfChangeSingle change)
    {
        ArgumentNullException.ThrowIfNull(change);
        if (!Enum.IsDefined(change.Type))
        {
            throw new ConfigurationChangeException(
                $"unexpected conf type {(int)change.Type}");
        }

        if (change.NodeId != 0
            && RaftLocalMessageTargets.IsLocal(
                change.NodeId))
        {
            throw new ConfigurationChangeException(
                $"local storage target {change.NodeId} cannot be a Raft member");
        }
    }

    private static void ValidateProposalNodeId(
        ulong nodeId,
        string parameterName)
    {
        if (nodeId != 0
            && RaftLocalMessageTargets.IsLocal(
                nodeId))
        {
            throw new ArgumentException(
                $"Local storage target {nodeId} cannot be a Raft member.",
                parameterName);
        }
    }
}
