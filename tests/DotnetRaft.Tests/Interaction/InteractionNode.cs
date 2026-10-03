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
}

internal readonly record struct InteractionRecipient(
    ulong Id,
    bool Drop = false);
