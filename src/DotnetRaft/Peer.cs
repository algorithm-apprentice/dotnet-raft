using DotnetRaft.Protocol;

namespace DotnetRaft;

public sealed record Peer
{
    public Peer(ulong id)
        : this(id, RaftBytes.Empty)
    {
    }

    public Peer(ulong id, RaftBytes context)
    {
        Id = id;
        Context = context;
    }

    public ulong Id { get; }

    public RaftBytes Context { get; }
}
