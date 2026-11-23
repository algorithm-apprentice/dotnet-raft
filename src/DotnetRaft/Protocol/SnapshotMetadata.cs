namespace DotnetRaft.Protocol;

public sealed class SnapshotMetadata : IEquatable<SnapshotMetadata>
{
    private ulong? index;
    private ulong? term;

    // An unnormalized or incoming snapshot can omit its configuration.
    public ConfState ConfState { get; set; } = null!;
    public ulong Index { get => index.GetValueOrDefault(); set => index = value; }
    public ulong Term { get => term.GetValueOrDefault(); set => term = value; }
    public bool HasIndex => index.HasValue;
    public bool HasTerm => term.HasValue;
    public RaftBytes ExtensionData { get; init; }

    public SnapshotMetadata Clone() => new()
    {
        index = index,
        term = term,
        ConfState = ConfState?.Clone()!,
        ExtensionData = ExtensionData,
    };

    public bool Equals(SnapshotMetadata? other) => other is not null && Index == other.Index && Term == other.Term
        && Equals(ConfState, other.ConfState) && ExtensionData == other.ExtensionData;

    public override bool Equals(object? obj) => obj is SnapshotMetadata other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(Index, Term, ConfState, ExtensionData);
}
