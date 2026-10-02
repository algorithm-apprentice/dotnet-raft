using Google.Protobuf;

namespace DotnetRaft;

public sealed record ReadState
{
    public ReadState(
        ulong index,
        ByteString requestContext)
    {
        ArgumentNullException.ThrowIfNull(requestContext);
        Index = index;
        RequestContext = requestContext;
    }

    public ulong Index { get; }

    public ByteString RequestContext { get; }
}
