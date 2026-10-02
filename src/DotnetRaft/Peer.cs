using Google.Protobuf;

namespace DotnetRaft;

public sealed record Peer
{
    public Peer(ulong id)
        : this(id, ByteString.Empty)
    {
    }

    public Peer(ulong id, ByteString context)
    {
        ArgumentNullException.ThrowIfNull(context);
        Id = id;
        Context = context;
    }

    public ulong Id { get; }

    public ByteString Context { get; }
}
