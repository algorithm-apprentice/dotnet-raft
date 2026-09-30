using DotnetRaft.Diagnostics;
using DotnetRaft.Read;
using DotnetRaft.Storage;

namespace DotnetRaft.Core;

internal sealed record ValidatedRaftConfig(
    ulong Id,
    int ElectionTick,
    int HeartbeatTick,
    IStorage Storage,
    ulong Applied,
    bool AsyncStorageWrites,
    ulong MaxSizePerMessage,
    ulong MaxCommittedSizePerReady,
    ulong MaxUncommittedEntriesSize,
    int MaxInflightMessages,
    ulong MaxInflightBytes,
    bool CheckQuorum,
    bool PreVote,
    ReadOnlyOption ReadOnlyOption,
    IRaftLogger Logger,
    bool DisableProposalForwarding,
    bool DisableConfChangeValidation,
    bool StepDownOnRemoval);
