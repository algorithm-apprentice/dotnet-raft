using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Examples.Counter;

// A single host loop drives all three RawNodes. Only the host moves work
// between replicas; no background threads or sockets are needed here.
internal sealed class CounterCluster(bool asyncStorage, CounterWriteTrace? trace)
{
    private const int MaxRounds = 100;
    private readonly Queue<Message> _delivery = new();

    internal IReadOnlyList<CounterReplica> Replicas { get; } = CreateReplicas(asyncStorage, trace);

    internal void Drain(bool runApplyWorkers = true)
    {
        for (var round = 0; round < MaxRounds; round++)
        {
            var didWork = false;
            foreach (CounterReplica replica in Replicas)
            {
                if (replica.Node.HasReady())
                {
                    ProcessReady(replica);
                    didWork = true;
                }

                if (replica.AppendWork.TryDequeue(out Message? append))
                {
                    ProcessAppend(replica, append);
                    didWork = true;
                }

                if (runApplyWorkers && replica.ApplyWork.TryDequeue(out Message? apply))
                {
                    replica.Apply(apply.Entries);
                    trace?.Applied(replica, apply.Entries);
                    EnqueueDelivery(apply.Responses);
                    didWork = true;
                }
            }

            if (_delivery.TryDequeue(out Message? message))
            {
                CounterReplica destination = Replicas.Single(replica => replica.Id == message.To);
                destination.Node.Step(message);
                if (message.Type == MessageType.MsgStorageApplyResp)
                {
                    trace?.AcknowledgedApplication(destination, message.Entries, nameof(MessageType.MsgStorageApplyResp));
                }

                didWork = true;
            }

            if (!didWork)
            {
                return;
            }
        }

        throw new InvalidOperationException("The example cluster did not become idle.");
    }

    private void ProcessReady(CounterReplica replica)
    {
        Ready ready = replica.Node.Ready();
        if (asyncStorage)
        {
            // Ready's persistence/application fields are diagnostic in this mode.
            foreach (Message message in ready.Messages)
            {
                switch (message.To)
                {
                    case RaftLocalMessageTargets.AppendThread:
                        replica.AppendWork.Enqueue(message);
                        break;
                    case RaftLocalMessageTargets.ApplyThread:
                        replica.ApplyWork.Enqueue(message);
                        trace?.ReceivedApplicationWork(replica.Id, message.Entries, nameof(MessageType.MsgStorageApply));
                        break;
                    default:
                        _delivery.Enqueue(message);
                        break;
                }
            }

            return;
        }

        trace?.ReceivedApplicationWork(replica.Id, ready.CommittedEntries, "Ready.CommittedEntries");
        replica.Persist(ready.Snapshot, ready.Entries, ready.HardState);
        trace?.Stored(replica.Id, ready.Entries, ready.HardState, nameof(Ready));
        EnqueueDelivery(ready.Messages);
        replica.Apply(ready.CommittedEntries);
        trace?.Applied(replica, ready.CommittedEntries);
        replica.Node.Advance(ready);
        trace?.AcknowledgedApplication(replica, ready.CommittedEntries, nameof(RawNode.Advance));
    }

    private void EnqueueDelivery(IEnumerable<Message> messages)
    {
        foreach (Message message in messages)
        {
            _delivery.Enqueue(message);
        }
    }

    private void ProcessAppend(CounterReplica replica, Message request)
    {
        // Library-generated append requests carry all three HardState fields or none.
        HardState? hardState = request.HasTerm
            ? new HardState { Term = request.Term, Vote = request.Vote, Commit = request.Commit }
            : null;
        replica.Persist(request.Snapshot, request.Entries, hardState);
        trace?.Stored(replica.Id, request.Entries, hardState, nameof(MessageType.MsgStorageAppend));
        // The same FIFO simulates local acknowledgements and remote replies.
        // The pump steps the node in each response's own To, not request.From.
        EnqueueDelivery(request.Responses);
    }

    private static CounterReplica[] CreateReplicas(bool asyncStorage, CounterWriteTrace? trace)
    {
        Peer[] peers = [new(1), new(2), new(3)];
        return [.. peers.Select(peer => new CounterReplica(peer.Id, peers, asyncStorage, trace))];
    }
}

internal sealed class CounterReplica
{
    internal static ByteString IncrementCommand { get; } = ByteString.CopyFromUtf8("increment");

    private readonly MemoryStorage _storage = new();

    internal CounterReplica(ulong id, Peer[] peers, bool asyncStorage, CounterWriteTrace? trace)
    {
        Id = id;
        Node = RawNode.Start(new RaftConfig
        {
            Id = id,
            Storage = _storage,
            AsyncStorageWrites = asyncStorage,
            TraceSink = trace,
        }, peers);
    }

    internal ulong Id { get; }
    internal RawNode Node { get; }
    internal ulong Applied { get; private set; }
    internal int Count { get; private set; }
    internal Queue<Message> AppendWork { get; } = new();
    internal Queue<Message> ApplyWork { get; } = new();

    internal void Persist(Snapshot? snapshot, IReadOnlyList<Entry> entries, HardState? hardState)
    {
        if (snapshot is not null)
        {
            throw new InvalidOperationException("This in-memory example does not support snapshots.");
        }

        // These writes are only a simulation. A durable host must publish
        // the batch atomically and honor the required durability barrier.
        _storage.Append(entries);
        if (hardState is not null)
        {
            _storage.SetHardState(hardState);
        }
    }

    internal void Apply(IReadOnlyList<Entry> entries)
    {
        foreach (Entry entry in entries)
        {
            if (entry.Index != Applied + 1)
            {
                throw new InvalidOperationException($"Node {Id} received out-of-order application work.");
            }

            switch (entry.Type)
            {
                case EntryType.EntryConfChange:
                    // Apply the fixed bootstrap membership through the public API.
                    Node.ApplyConfChange(ProtocolConfChange.Parser.ParseFrom(entry.Data));
                    break;
                case EntryType.EntryNormal when entry.Data.IsEmpty:
                    // The leader's no-op occupies an index but does not change the counter.
                    break;
                case EntryType.EntryNormal when entry.Data.Equals(IncrementCommand):
                    Count++;
                    break;
                default:
                    throw new InvalidOperationException($"Unsupported counter entry at index {entry.Index}.");
            }

            Applied = entry.Index;
        }
    }
}
