using DotnetRaft.Protocol;

namespace DotnetRaft.Examples.Counter;

// The in-process simulation drives all three replicas and delivers their
// messages without background threads or sockets.
internal sealed class CounterCluster(bool asyncStorage, CounterWriteTrace? trace)
{
    private const int MaxRounds = 100;
    private readonly Queue<Action> _delivery = new();

    internal IReadOnlyList<CounterReplica> Replicas { get; } = CreateReplicas(asyncStorage, trace);

    internal void Drain(bool runApplyWorkers = true)
    {
        for (var round = 0; round < MaxRounds; round++)
        {
            if (!RunOneRound(runApplyWorkers))
            {
                return;
            }
        }

        throw new InvalidOperationException("The example cluster did not become idle.");
    }

    private bool RunOneRound(bool runApplyWorkers)
    {
        var didWork = false;
        foreach (CounterReplica replica in Replicas)
        {
            // Execute every enabled stage, even if an earlier stage made progress.
            bool readied = TryProcessReady(replica);
            bool appended = TryRunAppendWorker(replica);
            bool applied = runApplyWorkers && TryRunApplyWorker(replica);
            didWork = didWork || readied || appended || applied;
        }

        bool delivered = TryDeliverMessage();
        return didWork || delivered;
    }

    private bool TryProcessReady(CounterReplica replica)
    {
        if (!replica.Node.HasReady())
        {
            return false;
        }

        ProcessReady(replica);
        return true;
    }

    private bool TryRunAppendWorker(CounterReplica replica)
    {
        if (!replica.AppendWork.TryDequeue(out StorageAppendWork? request))
        {
            return false;
        }

        ProcessAppend(replica, request);
        return true;
    }

    private bool TryRunApplyWorker(CounterReplica replica)
    {
        if (!replica.ApplyWork.TryDequeue(out StorageApplyWork? request))
        {
            return false;
        }

        ApplyCommittedEntries(replica, request.Entries);
        _delivery.Enqueue(() =>
        {
            replica.Node.CompleteStorageWork(request.Completion);
            trace?.AcknowledgedApplication(replica, request.Entries, nameof(MessageType.MsgStorageApplyResp));
        });
        return true;
    }

    private bool TryDeliverMessage()
    {
        if (!_delivery.TryDequeue(out Action? deliver))
        {
            return false;
        }

        deliver();
        return true;
    }

    private void ProcessReady(CounterReplica replica)
    {
        Ready ready = replica.Node.Ready();
        if (asyncStorage)
        {
            DispatchAsyncStorageWork(replica, ready);
            return;
        }

        ProcessSynchronousReady(replica, ready);
    }

    private void ProcessSynchronousReady(CounterReplica replica, Ready ready)
    {
        trace?.ReceivedApplicationWork(replica.Id, ready.CommittedEntries, "Ready.CommittedEntries");
        StoreReadyBatch(replica, ready);
        EnqueueDelivery(ready.Messages);
        ApplyCommittedEntries(replica, ready.CommittedEntries);
        AcknowledgeReady(replica, ready);
    }

    private void StoreReadyBatch(CounterReplica replica, Ready ready)
    {
        replica.Persist(ready.Snapshot, ready.Entries, ready.HardState);
        trace?.Stored(replica.Id, ready.Entries, ready.HardState, nameof(Ready));
    }

    private void ApplyCommittedEntries(CounterReplica replica, IReadOnlyList<Entry> entries)
    {
        replica.Apply(entries);
        trace?.Applied(replica, entries);
    }

    private void AcknowledgeReady(CounterReplica replica, Ready ready)
    {
        replica.Node.Advance(ready);
        trace?.AcknowledgedApplication(replica, ready.CommittedEntries, nameof(RawNode.Advance));
    }

    private void DispatchAsyncStorageWork(CounterReplica replica, Ready ready)
    {
        // In this protocol, the Ready fields are diagnostic. Workers complete
        // the messages below and release responses; they never call Advance.
        EnqueueDelivery(ready.Messages);
        if (ready.AppendWork is { } append)
        {
            replica.AppendWork.Enqueue(append);
        }

        if (ready.ApplyWork is { } apply)
        {
            replica.ApplyWork.Enqueue(apply);
            trace?.ReceivedApplicationWork(replica.Id, apply.Entries, nameof(MessageType.MsgStorageApply));
        }
    }

    private void EnqueueDelivery(IEnumerable<Message> messages)
    {
        foreach (Message message in messages)
        {
            _delivery.Enqueue(() => Replicas.Single(replica => replica.Id == message.To).Node.Step(message));
        }
    }

    private void ProcessAppend(CounterReplica replica, StorageAppendWork request)
    {
        replica.Persist(request.Snapshot, request.Entries, request.HardState);
        trace?.Stored(replica.Id, request.Entries, request.HardState, nameof(MessageType.MsgStorageAppend));
        EnqueueDelivery(request.MessagesAfterPersist);
        if (request.Completion is { } completion)
        {
            _delivery.Enqueue(() => replica.Node.CompleteStorageWork(completion));
        }
    }

    private static CounterReplica[] CreateReplicas(bool asyncStorage, CounterWriteTrace? trace)
    {
        Peer[] peers = [new(1), new(2), new(3)];
        return [.. peers.Select(peer => new CounterReplica(peer.Id, peers, asyncStorage, trace))];
    }
}
