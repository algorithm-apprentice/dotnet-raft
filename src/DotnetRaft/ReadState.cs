namespace DotnetRaft;

public sealed record ReadState
{
    public ReadState(
        ulong index,
        ReadOnlySpan<byte> requestContext)
    {
        Index = index;
        RequestContext = requestContext.ToArray();
    }

    public ulong Index { get; }

    public ReadOnlyMemory<byte> RequestContext { get; }

    public bool Equals(ReadState? other) => other is not null
        && Index == other.Index && RequestContext.Span.SequenceEqual(other.RequestContext.Span);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Index);
        hash.AddBytes(RequestContext.Span);
        return hash.ToHashCode();
    }
}
