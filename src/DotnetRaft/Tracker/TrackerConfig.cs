using System.Text;

using DotnetRaft.Protocol;
using DotnetRaft.Quorum;

namespace DotnetRaft.Tracker;

internal sealed class TrackerConfig
{
    internal TrackerConfig()
        : this(new JointConfig(new MajorityConfig()))
    {
    }

    internal TrackerConfig(
        JointConfig voters,
        IEnumerable<ulong>? learners = null,
        IEnumerable<ulong>? learnersNext = null,
        bool autoLeave = false)
    {
        ArgumentNullException.ThrowIfNull(voters);

        Voters = new JointConfig(
            voters.Incoming.Clone(),
            voters.Outgoing.Clone());
        Learners = learners is null
            ? []
            : [.. learners];
        LearnersNext = learnersNext is null
            ? []
            : [.. learnersNext];
        AutoLeave = autoLeave;
    }

    internal JointConfig Voters { get; }

    internal HashSet<ulong> Learners { get; }

    internal HashSet<ulong> LearnersNext { get; }

    internal bool AutoLeave { get; set; }

    internal TrackerConfig Clone()
    {
        return new TrackerConfig(
            Voters,
            Learners,
            LearnersNext,
            AutoLeave);
    }

    internal ConfState ToConfState()
    {
        var state = new ConfState
        {
            AutoLeave = AutoLeave,
        };
        state.Voters.Add(Voters.Incoming.Order());
        state.VotersOutgoing.Add(Voters.Outgoing.Order());
        state.Learners.Add(Learners.Order());
        state.LearnersNext.Add(LearnersNext.Order());
        return state;
    }

    public override string ToString()
    {
        var builder = new StringBuilder();
        builder.Append("voters=")
            .Append(Voters);

        if (Learners.Count > 0)
        {
            builder.Append(" learners=")
                .Append(new MajorityConfig(Learners));
        }

        if (LearnersNext.Count > 0)
        {
            builder.Append(" learners_next=")
                .Append(new MajorityConfig(LearnersNext));
        }

        if (AutoLeave)
        {
            builder.Append(" autoleave");
        }

        return builder.ToString();
    }
}
