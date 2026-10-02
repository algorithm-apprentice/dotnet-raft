namespace DotnetRaft;

public readonly record struct BasicStatus(
    ulong Id,
    ulong Term,
    ulong Vote,
    ulong Commit,
    ulong LeaderId,
    RaftRole Role,
    ulong Applied,
    ulong LeaderTransferee);
