namespace DotnetRaft.Protocol;

/// <summary>A recovery point and its opaque application state.</summary>
public sealed class Snapshot : IEquatable<Snapshot>
{
    private RaftBytes? data;

    public RaftBytes Data { get => data.GetValueOrDefault(); set => data = value; }
    public SnapshotMetadata Metadata { get; set; } = null!;
    public bool HasData => data.HasValue;
    public RaftBytes ExtensionData { get; init; }

    public Snapshot Clone() => new()
    {
        data = data,
        Metadata = Metadata?.Clone()!,
        ExtensionData = ExtensionData,
    };

    public bool Equals(Snapshot? other) => other is not null && Data == other.Data && Equals(Metadata, other.Metadata)
        && ExtensionData == other.ExtensionData;

    public override bool Equals(object? obj) => obj is Snapshot other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(Data, Metadata, ExtensionData);
}
