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
    ulong MaxSizePerMessage,
    ulong MaxCommittedSizePerReady,
    ProposalPolicy Proposals,
    int MaxInflightMessages,
    ulong MaxInflightBytes,
    ElectionPolicy Election,
    ReadOnlyOption ReadOnlyOption,
    IRaftTraceSink? TraceSink,
    IRaftLogger Logger,
    bool StepDownOnRemoval);

internal readonly record struct ElectionPolicy(bool CheckQuorum, bool PreVote);
