using DotnetRaft.Protocol;
using DotnetRaft.Quorum;

namespace DotnetRaft.Tracker;

internal sealed class ProgressTracker
{
    private Dictionary<ulong, bool> votes = [];

    internal ProgressTracker(
        int maxInflightMessages,
        ulong maxInflightBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(
            maxInflightMessages);

        MaxInflightMessages = maxInflightMessages;
        MaxInflightBytes = maxInflightBytes;
        Config = new TrackerConfig();
        Progress = [];
    }

    internal TrackerConfig Config { get; private set; }

    internal ProgressMap Progress { get; private set; }

    internal IReadOnlyDictionary<ulong, bool> Votes => votes;

    internal int MaxInflightMessages { get; }

    internal ulong MaxInflightBytes { get; }

    internal void Install(
        TrackerConfig config,
        ProgressMap progress)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(progress);

        Config = config;
        Progress = progress;
    }

    internal bool Contains(ulong id)
    {
        return Progress.ContainsKey(id);
    }

    internal bool IsLearner(ulong id)
    {
        return Progress.TryGetValue(
                id,
                out Progress? progress)
            && progress.IsLearner;
    }

    internal bool IsVoter(ulong id)
    {
        if (!Config.Voters.Incoming.Contains(id)
            && !Config.Voters.Outgoing.Contains(id))
        {
            return false;
        }

        return Progress.TryGetValue(
                id,
                out Progress? progress)
            && !progress.IsLearner;
    }

    internal bool IsSingleton =>
        Config.Voters.Incoming.Count == 1
        && Config.Voters.Outgoing.Count == 0;

    internal ulong CommittedIndex =>
        Config.Voters.CommittedIndex(Progress);

    internal ConfState ToConfState()
    {
        return Config.ToConfState();
    }

    internal void Visit(Action<ulong, Progress> visitor)
    {
        ArgumentNullException.ThrowIfNull(visitor);

        ulong[] ids = [.. Progress.Keys.Order()];
        foreach (ulong id in ids)
        {
            visitor(id, Progress[id]);
        }
    }

    internal bool QuorumActive()
    {
        var activity = new Dictionary<ulong, bool>();
        Visit((id, progress) =>
        {
            if (!progress.IsLearner)
            {
                activity[id] = progress.RecentActive;
            }
        });

        return Config.Voters.VoteResult(activity)
            == VoteResult.Won;
    }

    internal ulong[] VoterNodes()
    {
        return [.. Config.Voters.Ids().Order()];
    }

    internal ulong[] LearnerNodes()
    {
        return [.. Config.Learners.Order()];
    }

    internal void ResetVotes()
    {
        votes = [];
    }

    internal void RecordVote(ulong id, bool granted)
    {
        votes.TryAdd(id, granted);
    }

    internal (
        int Granted,
        int Rejected,
        VoteResult Result) TallyVotes()
    {
        var granted = 0;
        var rejected = 0;

        foreach ((ulong id, Progress progress) in Progress)
        {
            var vote = false;
            if (progress.IsLearner
                || !votes.TryGetValue(id, out vote))
            {
                continue;
            }

            if (vote)
            {
                granted++;
            }
            else
            {
                rejected++;
            }
        }

        VoteResult result = Config.Voters.VoteResult(votes);
        return (granted, rejected, result);
    }
}
