namespace DotnetRaft.Protocol;

public sealed class ConfChangeV2 : IEquatable<ConfChangeV2>
{
    private ConfChangeTransition? transition;
    private RaftBytes? context;

    public ConfChangeTransition Transition { get => transition.GetValueOrDefault(); set => transition = value; }
    public List<ConfChangeSingle> Changes { get; } = [];
    public RaftBytes Context { get => context.GetValueOrDefault(); set => context = value; }
    public bool HasTransition => transition.HasValue;
    public bool HasContext => context.HasValue;
    public RaftBytes ExtensionData { get; init; }

    public ConfChangeV2 Clone()
    {
        var copy = new ConfChangeV2 { transition = transition, context = context, ExtensionData = ExtensionData };
        foreach (ConfChangeSingle change in Changes)
        {
            ArgumentNullException.ThrowIfNull(change);
            copy.Changes.Add(change.Clone());
        }

        return copy;
    }

    public bool Equals(ConfChangeV2? other) => other is not null && Transition == other.Transition
        && Changes.SequenceEqual(other.Changes) && Context == other.Context && ExtensionData == other.ExtensionData;

    public override bool Equals(object? obj) => obj is ConfChangeV2 other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(Transition, Changes.Count, Context, ExtensionData);
}
