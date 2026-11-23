namespace DotnetRaft.Examples.KvCluster;

public sealed class PendingReadRegistry
    : PendingOperationRegistry
{
    private readonly Dictionary<Guid, ulong>
        deferred = [];
    private readonly object gate = new();

    public void CompleteOrDefer(
        Guid requestId,
        ulong index,
        ulong physicalApplied)
    {
        if (index <= physicalApplied)
        {
            Complete(requestId, index);
            return;
        }

        lock (gate)
        {
            deferred[requestId] = index;
        }

        if (!IsPending(requestId))
        {
            lock (gate)
            {
                deferred.Remove(requestId);
            }
        }
    }

    public void CompleteThrough(
        ulong physicalApplied)
    {
        KeyValuePair<Guid, ulong>[] ready = TakeAppliedReads(physicalApplied);
        foreach (KeyValuePair<Guid, ulong> pair in ready)
        {
            Complete(pair.Key, pair.Value);
        }
    }

    private KeyValuePair<Guid, ulong>[] TakeAppliedReads(ulong physicalApplied)
    {
        KeyValuePair<Guid, ulong>[] ready;
        lock (gate)
        {
            ready =
            [
                .. deferred.Where(
                    pair =>
                        pair.Value
                        <= physicalApplied),
            ];
            foreach (KeyValuePair<Guid, ulong>
                     pair in ready)
            {
                deferred.Remove(pair.Key);
            }
        }

        return ready;
    }

    public override bool Remove(Guid requestId)
    {
        lock (gate)
        {
            deferred.Remove(requestId);
        }

        return base.Remove(requestId);
    }
}
