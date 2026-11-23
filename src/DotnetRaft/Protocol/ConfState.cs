namespace DotnetRaft.Protocol;

/// <summary>The committed membership represented by a recovery point.</summary>
public sealed class ConfState : IEquatable<ConfState>
{
    private bool? autoLeave;

    public List<ulong> Voters { get; } = [];
    public List<ulong> Learners { get; } = [];
    public List<ulong> VotersOutgoing { get; } = [];
    public List<ulong> LearnersNext { get; } = [];
    public bool AutoLeave { get => autoLeave.GetValueOrDefault(); set => autoLeave = value; }
    public bool HasAutoLeave => autoLeave.HasValue;
    public RaftBytes ExtensionData { get; init; }

    public ConfState Clone()
    {
        var copy = new ConfState { autoLeave = autoLeave, ExtensionData = ExtensionData };
        copy.Voters.AddRange(Voters);
        copy.Learners.AddRange(Learners);
        copy.VotersOutgoing.AddRange(VotersOutgoing);
        copy.LearnersNext.AddRange(LearnersNext);
        return copy;
    }

    public bool Equals(ConfState? other) => other is not null && AutoLeave == other.AutoLeave
        && Voters.SequenceEqual(other.Voters) && Learners.SequenceEqual(other.Learners)
        && VotersOutgoing.SequenceEqual(other.VotersOutgoing) && LearnersNext.SequenceEqual(other.LearnersNext)
        && ExtensionData == other.ExtensionData;

    public override bool Equals(object? obj) => obj is ConfState other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(AutoLeave, Voters.Count, Learners.Count,
        VotersOutgoing.Count, LearnersNext.Count, ExtensionData);
}
