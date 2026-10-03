using DotnetRaft.Protocol;
using DotnetRaft.Read;
using DotnetRaft.Storage;

using Google.Protobuf;

namespace DotnetRaft.Tests.Interaction;

internal sealed record InteractionNodeOptions
{
    internal IReadOnlyList<ulong> Voters { get; init; } = [];

    internal IReadOnlyList<ulong> Learners { get; init; } = [];

    internal ulong SnapshotIndex { get; init; }

    internal ByteString SnapshotData { get; init; } =
        ByteString.Empty;

    internal int ElectionTick { get; init; } = 3;

    internal int HeartbeatTick { get; init; } = 1;

    internal int MaxInflightMessages { get; init; } =
        int.MaxValue;

    internal ulong MaxCommittedSizePerReady { get; init; }

    internal bool PreVote { get; init; }

    internal bool CheckQuorum { get; init; }

    internal bool DisableConfChangeValidation { get; init; }

    internal bool StepDownOnRemoval { get; init; }

    internal ReadOnlyOption ReadOnlyOption { get; init; } =
        ReadOnlyOption.Safe;

    internal bool AsyncStorageWrites { get; init; }
}

internal sealed class InteractionNode
{
    private Snapshot _applicationSnapshot;
    private readonly Queue<Message> _appendWork = new();
    private readonly Queue<Message> _applyWork = new();

    internal InteractionNode(
        DotnetRaft.RawNode rawNode,
        MemoryStorage storage,
        RaftConfig config,
        Snapshot applicationSnapshot)
    {
        ArgumentNullException.ThrowIfNull(rawNode);
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(applicationSnapshot);

        RawNode = rawNode;
        Storage = storage;
        Config = config;
        _applicationSnapshot =
            applicationSnapshot.Clone();
    }

    internal DotnetRaft.RawNode RawNode { get; }

    internal MemoryStorage Storage { get; }

    internal RaftConfig Config { get; }

    internal int AppendWorkCount => _appendWork.Count;

    internal int ApplyWorkCount => _applyWork.Count;

    internal Snapshot GetApplicationSnapshot()
    {
        return _applicationSnapshot.Clone();
    }

    internal void SetApplicationSnapshot(
        Snapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        _applicationSnapshot = snapshot.Clone();
    }

    internal void EnqueueAppendWork(Message message)
    {
        ArgumentNullException.ThrowIfNull(message);
        _appendWork.Enqueue(message.Clone());
    }

    internal void EnqueueApplyWork(Message message)
    {
        ArgumentNullException.ThrowIfNull(message);
        _applyWork.Enqueue(message.Clone());
    }

    internal bool TryDequeueAppendWork(
        out Message? message)
    {
        return _appendWork.TryDequeue(out message);
    }

    internal bool TryDequeueApplyWork(
        out Message? message)
    {
        return _applyWork.TryDequeue(out message);
    }
}

internal readonly record struct InteractionRecipient(
    ulong Id,
    bool Drop = false);
