namespace DotnetRaft;

/// <summary>A detached observation of the node.</summary>
/// <remarks><c>Applied</c> is Raft's acknowledgement cursor, not the application's physical recovery position.</remarks>
public readonly record struct BasicStatus(
    ulong Id,
    ulong Term,
    ulong Vote,
    ulong Commit,
    ulong LeaderId,
    RaftRole Role,
    ulong Applied,
    ulong LeaderTransferee);
