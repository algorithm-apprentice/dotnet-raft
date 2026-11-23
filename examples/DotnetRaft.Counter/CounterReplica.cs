using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Examples.Counter;

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
