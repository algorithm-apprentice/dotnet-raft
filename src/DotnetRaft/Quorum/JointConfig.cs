namespace DotnetRaft.Quorum;

internal sealed class JointConfig
{
    public JointConfig(MajorityConfig incoming, MajorityConfig? outgoing = null)
    {
        ArgumentNullException.ThrowIfNull(incoming);
        Incoming = incoming;
        Outgoing = outgoing ?? new MajorityConfig();
    }

    public MajorityConfig Incoming { get; }

    public MajorityConfig Outgoing { get; }

    public IReadOnlySet<ulong> Ids()
    {
        var ids = new HashSet<ulong>(Incoming);
        ids.UnionWith(Outgoing);
        return ids;
    }

    public ulong CommittedIndex(IAckedIndexer indexer)
    {
        ArgumentNullException.ThrowIfNull(indexer);
        return Math.Min(
            Incoming.CommittedIndex(indexer),
            Outgoing.CommittedIndex(indexer));
    }

    public VoteResult VoteResult(IReadOnlyDictionary<ulong, bool> votes)
    {
        ArgumentNullException.ThrowIfNull(votes);

        var incoming = Incoming.VoteResult(votes);
        var outgoing = Outgoing.VoteResult(votes);
        if (incoming == outgoing)
        {
            return incoming;
        }

        if (incoming == Quorum.VoteResult.Lost || outgoing == Quorum.VoteResult.Lost)
        {
            return Quorum.VoteResult.Lost;
        }

        return Quorum.VoteResult.Pending;
    }

    public string Describe(IAckedIndexer indexer)
    {
        return new MajorityConfig(Ids()).Describe(indexer);
    }

    public override string ToString()
    {
        return Outgoing.Count > 0
            ? $"{Incoming}&&{Outgoing}"
            : Incoming.ToString();
    }
}
