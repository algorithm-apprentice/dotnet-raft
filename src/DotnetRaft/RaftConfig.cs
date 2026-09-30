using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Read;
using DotnetRaft.Storage;

namespace DotnetRaft;

public sealed class RaftConfig
{
    public ulong Id { get; init; }

    public int ElectionTick { get; init; } = 10;

    public int HeartbeatTick { get; init; } = 1;

    public IStorage? Storage { get; init; }

    public ulong Applied { get; init; }

    public bool AsyncStorageWrites { get; init; }

    public ulong MaxSizePerMessage { get; init; } = ulong.MaxValue;

    public ulong MaxCommittedSizePerReady { get; init; }

    public ulong MaxUncommittedEntriesSize { get; init; }

    public int MaxInflightMessages { get; init; } = 256;

    public ulong MaxInflightBytes { get; init; }

    public bool CheckQuorum { get; init; }

    public bool PreVote { get; init; }

    public ReadOnlyOption ReadOnlyOption { get; init; }

    public IRaftLogger? Logger { get; init; }

    public bool DisableProposalForwarding { get; init; }

    public bool DisableConfChangeValidation { get; init; }

    public bool StepDownOnRemoval { get; init; }

    internal ValidatedRaftConfig ValidateAndNormalize()
    {
        if (Id == RaftMessageTargets.None || RaftMessageTargets.IsLocal(Id))
        {
            throw new ArgumentOutOfRangeException(
                nameof(Id),
                Id,
                "Raft ID must be a nonzero remote node ID.");
        }

        if (HeartbeatTick <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(HeartbeatTick),
                HeartbeatTick,
                "Heartbeat tick must be greater than zero.");
        }

        if (ElectionTick <= HeartbeatTick)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ElectionTick),
                ElectionTick,
                "Election tick must be greater than heartbeat tick.");
        }

        const int maxRepresentableElectionTick =
            (int.MaxValue / 2) + 1;
        if (ElectionTick > maxRepresentableElectionTick)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ElectionTick),
                ElectionTick,
                "Election tick is too large to randomize within Int32.");
        }

        ArgumentNullException.ThrowIfNull(Storage);

        if (MaxInflightMessages <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxInflightMessages),
                MaxInflightMessages,
                "Maximum inflight messages must be greater than zero.");
        }

        if (MaxInflightBytes != 0
            && MaxInflightBytes < MaxSizePerMessage)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxInflightBytes),
                MaxInflightBytes,
                "Maximum inflight bytes must be at least the maximum message size.");
        }

        if (ReadOnlyOption == ReadOnlyOption.LeaseBased
            && !CheckQuorum)
        {
            throw new ArgumentException(
                "Quorum checking must be enabled for lease-based reads.",
                nameof(CheckQuorum));
        }

        return new ValidatedRaftConfig(
            Id,
            ElectionTick,
            HeartbeatTick,
            Storage,
            Applied,
            AsyncStorageWrites,
            MaxSizePerMessage,
            MaxCommittedSizePerReady == 0
                ? MaxSizePerMessage
                : MaxCommittedSizePerReady,
            MaxUncommittedEntriesSize == 0
                ? ulong.MaxValue
                : MaxUncommittedEntriesSize,
            MaxInflightMessages,
            MaxInflightBytes == 0
                ? ulong.MaxValue
                : MaxInflightBytes,
            CheckQuorum,
            PreVote,
            ReadOnlyOption,
            Logger ?? NullRaftLogger.Instance,
            DisableProposalForwarding,
            DisableConfChangeValidation,
            StepDownOnRemoval);
    }
}
