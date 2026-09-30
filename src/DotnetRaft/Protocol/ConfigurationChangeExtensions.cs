using DotnetRaft.ConfChange;

namespace DotnetRaft.Protocol;

internal static class ConfigurationChangeExtensions
{
    internal static ConfChangeV2 AsV2(
        this ConfChange change)
    {
        ArgumentNullException.ThrowIfNull(change);

        var result = new ConfChangeV2
        {
            Context = change.Context,
        };
        result.Changes.Add(
            new ConfChangeSingle
            {
                Type = change.Type,
                NodeId = change.NodeId,
            });
        return result;
    }

    internal static bool IsLeaveJoint(
        this ConfChangeV2 change)
    {
        ArgumentNullException.ThrowIfNull(change);
        return change.Transition
                == ConfChangeTransition.Auto
            && change.Changes.Count == 0;
    }

    internal static bool TryGetJointTransition(
        this ConfChangeV2 change,
        out bool autoLeave)
    {
        ArgumentNullException.ThrowIfNull(change);

        if (change.Transition
                == ConfChangeTransition.Auto
            && change.Changes.Count <= 1)
        {
            autoLeave = false;
            return false;
        }

        autoLeave = change.Transition switch
        {
            ConfChangeTransition.Auto => true,
            ConfChangeTransition.JointImplicit => true,
            ConfChangeTransition.JointExplicit => false,
            _ => throw new ConfigurationChangeException(
                $"unknown transition {(int)change.Transition}"),
        };
        return true;
    }
}
