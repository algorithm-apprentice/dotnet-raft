namespace DotnetRaft.Protocol;

public sealed class ConfChangeSingle : IEquatable<ConfChangeSingle>
{
    private ConfChangeType? type;
    private ulong? nodeId;

    public ConfChangeType Type { get => type.GetValueOrDefault(); set => type = value; }
    public ulong NodeId { get => nodeId.GetValueOrDefault(); set => nodeId = value; }
    public bool HasType => type.HasValue;
    public bool HasNodeId => nodeId.HasValue;
    public RaftBytes ExtensionData { get; init; }

    public ConfChangeSingle Clone() => (ConfChangeSingle)MemberwiseClone();

    public bool Equals(ConfChangeSingle? other) => other is not null && Type == other.Type && NodeId == other.NodeId
        && ExtensionData == other.ExtensionData;

    public override bool Equals(object? obj) => obj is ConfChangeSingle other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(Type, NodeId, ExtensionData);
}
