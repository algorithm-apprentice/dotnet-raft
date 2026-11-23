namespace DotnetRaft.Protocol;

/// <summary>An owned record in the replicated log.</summary>
public sealed class Entry : IEquatable<Entry>
{
    private ulong? term;
    private ulong? index;
    private EntryType? type;
    private RaftBytes? data;

    public ulong Term { get => term.GetValueOrDefault(); set => term = value; }
    public ulong Index { get => index.GetValueOrDefault(); set => index = value; }
    public EntryType Type { get => type.GetValueOrDefault(); set => type = value; }
    public RaftBytes Data { get => data.GetValueOrDefault(); set => data = value; }
    public bool HasTerm => term.HasValue;
    public bool HasIndex => index.HasValue;
    public bool HasType => type.HasValue;
    public bool HasData => data.HasValue;

    /// <summary>Codec-owned extension data to preserve when forwarding or persisting this record.</summary>
    public RaftBytes ExtensionData { get; init; }

    public Entry Clone() => (Entry)MemberwiseClone();

    public bool Equals(Entry? other) => other is not null && Term == other.Term && Index == other.Index
        && Type == other.Type && Data == other.Data && ExtensionData == other.ExtensionData;

    public override bool Equals(object? obj) => obj is Entry other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(Term, Index, Type, Data, ExtensionData);
}
