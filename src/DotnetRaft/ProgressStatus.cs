namespace DotnetRaft;

public readonly record struct ProgressStatus(
    ulong Match,
    ulong Next,
    ulong LastSentCommit,
    ReplicationState State,
    ulong PendingSnapshot,
    bool RecentActive,
    bool AppendFlowPaused,
    bool IsPaused,
    bool IsLearner,
    int InflightCount,
    ulong InflightBytes,
    int InflightCapacity,
    ulong MaxInflightBytes);
