namespace DotnetRaft.Protocol;

public sealed class ConfChange : IEquatable<ConfChange>
{
    private ConfChangeType? type;
    private ulong? nodeId;
    private RaftBytes? context;
    private ulong? id;

    public ConfChangeType Type { get => type.GetValueOrDefault(); set => type = value; }
    public ulong NodeId { get => nodeId.GetValueOrDefault(); set => nodeId = value; }
    public RaftBytes Context { get => context.GetValueOrDefault(); set => context = value; }
    public ulong Id { get => id.GetValueOrDefault(); set => id = value; }
    public bool HasType => type.HasValue;
    public bool HasNodeId => nodeId.HasValue;
    public bool HasContext => context.HasValue;
    public bool HasId => id.HasValue;
    public RaftBytes ExtensionData { get; init; }

    public ConfChange Clone() => (ConfChange)MemberwiseClone();

    public bool Equals(ConfChange? other) => other is not null && Type == other.Type && NodeId == other.NodeId
        && Context == other.Context && Id == other.Id && ExtensionData == other.ExtensionData;

    public override bool Equals(object? obj) => obj is ConfChange other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(Type, NodeId, Context, Id, ExtensionData);
}
