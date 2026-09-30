using DotnetRaft.Protocol;
using DotnetRaft.Quorum;
using DotnetRaft.Tracker;

namespace DotnetRaft.ConfChange;

internal sealed class ConfigurationChanger
{
    internal ConfigurationChanger(
        ProgressTracker tracker,
        ulong lastIndex)
    {
        ArgumentNullException.ThrowIfNull(tracker);
        Tracker = tracker;
        LastIndex = lastIndex;
    }

    internal ProgressTracker Tracker { get; }

    internal ulong LastIndex { get; }

    internal ConfigurationChangeResult Simple(
        IEnumerable<ConfChangeSingle> changes)
    {
        ConfChangeSingle[] materialized = Materialize(changes);
        (TrackerConfig config, ProgressMap progress) = CheckAndCopy();

        if (config.Voters.Outgoing.Count > 0)
        {
            throw new ConfigurationChangeException(
                "can't apply simple config change in joint config");
        }

        Apply(config, progress, materialized);
        int changedVoters = SymmetricDifferenceCount(
            Tracker.Config.Voters.Incoming,
            config.Voters.Incoming);
        if (changedVoters > 1)
        {
            throw new ConfigurationChangeException(
                "more than one voter changed without entering joint config");
        }

        return ValidateAndReturn(config, progress);
    }

    internal ConfigurationChangeResult EnterJoint(
        bool autoLeave,
        IEnumerable<ConfChangeSingle> changes)
    {
        ConfChangeSingle[] materialized = Materialize(changes);
        (TrackerConfig config, ProgressMap progress) = CheckAndCopy();

        if (config.Voters.Outgoing.Count > 0)
        {
            throw new ConfigurationChangeException(
                "config is already joint");
        }

        if (config.Voters.Incoming.Count == 0)
        {
            throw new ConfigurationChangeException(
                "can't make a zero-voter config joint");
        }

        foreach (ulong id in config.Voters.Incoming)
        {
            config.Voters.Outgoing.Add(id);
        }

        Apply(config, progress, materialized);
        config.AutoLeave = autoLeave;
        return ValidateAndReturn(config, progress);
    }

    internal ConfigurationChangeResult LeaveJoint()
    {
        (TrackerConfig config, ProgressMap progress) = CheckAndCopy();

        if (config.Voters.Outgoing.Count == 0)
        {
            throw new ConfigurationChangeException(
                "can't leave a non-joint config");
        }

        foreach (ulong id in config.LearnersNext)
        {
            config.Learners.Add(id);
            progress[id].IsLearner = true;
        }

        config.LearnersNext.Clear();

        foreach (ulong id in config.Voters.Outgoing)
        {
            if (!config.Voters.Incoming.Contains(id)
                && !config.Learners.Contains(id))
            {
                progress.Remove(id);
            }
        }

        Clear(config.Voters.Outgoing);
        config.AutoLeave = false;
        return ValidateAndReturn(config, progress);
    }

    private (
        TrackerConfig Config,
        ProgressMap Progress) CheckAndCopy()
    {
        Validate(Tracker.Config, Tracker.Progress);
        return (
            Tracker.Config.Clone(),
            Tracker.Progress.Clone());
    }

    private void Apply(
        TrackerConfig config,
        ProgressMap progress,
        IEnumerable<ConfChangeSingle> changes)
    {
        foreach (ConfChangeSingle change in changes)
        {
            ulong id = change.NodeId;
            if (id == 0)
            {
                continue;
            }

            switch (change.Type)
            {
                case ConfChangeType.ConfChangeAddNode:
                    MakeVoter(config, progress, id);
                    break;
                case ConfChangeType.ConfChangeAddLearnerNode:
                    MakeLearner(config, progress, id);
                    break;
                case ConfChangeType.ConfChangeRemoveNode:
                    Remove(config, progress, id);
                    break;
                case ConfChangeType.ConfChangeUpdateNode:
                    break;
                default:
                    throw new ConfigurationChangeException(
                        $"unexpected conf type {(int)change.Type}");
            }
        }

        if (config.Voters.Incoming.Count == 0)
        {
            throw new ConfigurationChangeException(
                "removed all voters");
        }
    }

    private void MakeVoter(
        TrackerConfig config,
        ProgressMap progress,
        ulong id)
    {
        if (!progress.TryGetValue(id, out Progress? existing))
        {
            InitializeProgress(
                config,
                progress,
                id,
                isLearner: false);
            return;
        }

        existing.IsLearner = false;
        config.Learners.Remove(id);
        config.LearnersNext.Remove(id);
        config.Voters.Incoming.Add(id);
    }

    private void MakeLearner(
        TrackerConfig config,
        ProgressMap progress,
        ulong id)
    {
        if (!progress.TryGetValue(id, out Progress? existing))
        {
            InitializeProgress(
                config,
                progress,
                id,
                isLearner: true);
            return;
        }

        if (existing.IsLearner)
        {
            return;
        }

        Remove(config, progress, id);
        progress[id] = existing;

        if (config.Voters.Outgoing.Contains(id))
        {
            config.LearnersNext.Add(id);
        }
        else
        {
            existing.IsLearner = true;
            config.Learners.Add(id);
        }
    }

    private static void Remove(
        TrackerConfig config,
        ProgressMap progress,
        ulong id)
    {
        if (!progress.ContainsKey(id))
        {
            return;
        }

        config.Voters.Incoming.Remove(id);
        config.Learners.Remove(id);
        config.LearnersNext.Remove(id);

        if (!config.Voters.Outgoing.Contains(id))
        {
            progress.Remove(id);
        }
    }

    private void InitializeProgress(
        TrackerConfig config,
        ProgressMap progress,
        ulong id,
        bool isLearner)
    {
        if (isLearner)
        {
            config.Learners.Add(id);
        }
        else
        {
            config.Voters.Incoming.Add(id);
        }

        progress[id] = new Progress(
            match: 0,
            next: Math.Max(LastIndex, 1),
            maxInflightMessages: Tracker.MaxInflightMessages,
            maxInflightBytes: Tracker.MaxInflightBytes,
            isLearner: isLearner,
            recentActive: true);
    }

    private static ConfigurationChangeResult ValidateAndReturn(
        TrackerConfig config,
        ProgressMap progress)
    {
        Validate(config, progress);
        return new ConfigurationChangeResult(config, progress);
    }

    private static void Validate(
        TrackerConfig config,
        ProgressMap progress)
    {
        IReadOnlySet<ulong> voters = config.Voters.Ids();
        foreach (IEnumerable<ulong> ids in new IEnumerable<ulong>[]
                 {
                     voters,
                     config.Learners,
                     config.LearnersNext,
                 })
        {
            foreach (ulong id in ids)
            {
                if (!progress.ContainsKey(id))
                {
                    throw new ConfigurationChangeException(
                        $"no progress for {id}");
                }
            }
        }

        foreach (ulong id in config.LearnersNext)
        {
            if (!config.Voters.Outgoing.Contains(id))
            {
                throw new ConfigurationChangeException(
                    $"{id} is in LearnersNext, but not Voters[1]");
            }

            if (progress[id].IsLearner)
            {
                throw new ConfigurationChangeException(
                    $"{id} is in LearnersNext, but is already marked as learner");
            }
        }

        foreach (ulong id in config.Learners)
        {
            if (config.Voters.Outgoing.Contains(id))
            {
                throw new ConfigurationChangeException(
                    $"{id} is in Learners and Voters[1]");
            }

            if (config.Voters.Incoming.Contains(id))
            {
                throw new ConfigurationChangeException(
                    $"{id} is in Learners and Voters[0]");
            }

            if (!progress[id].IsLearner)
            {
                throw new ConfigurationChangeException(
                    $"{id} is in Learners, but is not marked as learner");
            }
        }

        foreach (ulong id in voters)
        {
            if (progress[id].IsLearner)
            {
                throw new ConfigurationChangeException(
                    $"{id} is in Voters, but is marked as learner");
            }
        }

        if (config.Voters.Outgoing.Count == 0)
        {
            if (config.LearnersNext.Count > 0)
            {
                throw new ConfigurationChangeException(
                    "LearnersNext must be empty when not joint");
            }

            if (config.AutoLeave)
            {
                throw new ConfigurationChangeException(
                    "AutoLeave must be false when not joint");
            }
        }
    }

    private static ConfChangeSingle[] Materialize(
        IEnumerable<ConfChangeSingle> changes)
    {
        ArgumentNullException.ThrowIfNull(changes);
        ConfChangeSingle[] materialized = [.. changes];
        if (materialized.Any(change => change is null))
        {
            throw new ArgumentException(
                "Configuration changes cannot contain null.",
                nameof(changes));
        }

        return materialized;
    }

    private static int SymmetricDifferenceCount(
        IEnumerable<ulong> left,
        IEnumerable<ulong> right)
    {
        var difference = new HashSet<ulong>(left);
        difference.SymmetricExceptWith(right);
        return difference.Count;
    }

    private static void Clear(
        MajorityConfig config)
    {
        foreach (ulong id in config.ToArray())
        {
            config.Remove(id);
        }
    }
}
