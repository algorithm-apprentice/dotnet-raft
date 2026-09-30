using DotnetRaft.Protocol;
using DotnetRaft.Tracker;

namespace DotnetRaft.ConfChange;

internal static class ConfigurationRestore
{
    internal static ConfigurationChangeResult Restore(
        ConfigurationChanger changer,
        ConfState state)
    {
        ArgumentNullException.ThrowIfNull(changer);
        ArgumentNullException.ThrowIfNull(state);

        EnsureEmpty(changer.Tracker);
        ConfState normalized = ProtocolDefaults.EnsureConfState(
            state.Clone());
        (
            ConfChangeSingle[] outgoing,
            ConfChangeSingle[] incoming) = ToChanges(normalized);

        var scratch = new ProgressTracker(
            changer.Tracker.MaxInflightMessages,
            changer.Tracker.MaxInflightBytes);

        if (outgoing.Length == 0)
        {
            foreach (ConfChangeSingle change in incoming)
            {
                Install(
                    scratch,
                    new ConfigurationChanger(
                        scratch,
                        changer.LastIndex).Simple([change]));
            }
        }
        else
        {
            foreach (ConfChangeSingle change in outgoing)
            {
                Install(
                    scratch,
                    new ConfigurationChanger(
                        scratch,
                        changer.LastIndex).Simple([change]));
            }

            Install(
                scratch,
                new ConfigurationChanger(
                    scratch,
                    changer.LastIndex).EnterJoint(
                    normalized.AutoLeave,
                    incoming));
        }

        var result = new ConfigurationChangeResult(
            scratch.Config,
            scratch.Progress);
        if (!normalized.IsEquivalentTo(result.Config.ToConfState()))
        {
            throw new ConfigurationChangeException(
                "restored configuration does not match ConfState");
        }

        return result;
    }

    private static void EnsureEmpty(ProgressTracker tracker)
    {
        if (tracker.Config.Voters.Incoming.Count != 0
            || tracker.Config.Voters.Outgoing.Count != 0
            || tracker.Config.Learners.Count != 0
            || tracker.Config.LearnersNext.Count != 0
            || tracker.Config.AutoLeave
            || tracker.Progress.Count != 0)
        {
            throw new ConfigurationChangeException(
                "cannot restore into a non-empty tracker");
        }
    }

    private static (
        ConfChangeSingle[] Outgoing,
        ConfChangeSingle[] Incoming) ToChanges(
        ConfState state)
    {
        var outgoing = new List<ConfChangeSingle>();
        var incoming = new List<ConfChangeSingle>();

        foreach (ulong id in state.VotersOutgoing)
        {
            outgoing.Add(Change(
                ConfChangeType.ConfChangeAddNode,
                id));
            incoming.Add(Change(
                ConfChangeType.ConfChangeRemoveNode,
                id));
        }

        foreach (ulong id in state.Voters)
        {
            incoming.Add(Change(
                ConfChangeType.ConfChangeAddNode,
                id));
        }

        foreach (ulong id in state.Learners)
        {
            incoming.Add(Change(
                ConfChangeType.ConfChangeAddLearnerNode,
                id));
        }

        foreach (ulong id in state.LearnersNext)
        {
            incoming.Add(Change(
                ConfChangeType.ConfChangeAddLearnerNode,
                id));
        }

        return ([.. outgoing], [.. incoming]);
    }

    private static ConfChangeSingle Change(
        ConfChangeType type,
        ulong id)
    {
        return new ConfChangeSingle
        {
            Type = type,
            NodeId = id,
        };
    }

    private static void Install(
        ProgressTracker tracker,
        ConfigurationChangeResult result)
    {
        tracker.Config = result.Config;
        tracker.Progress = result.Progress;
    }
}
