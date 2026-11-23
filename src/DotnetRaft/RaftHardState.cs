namespace DotnetRaft;

/// <summary>A complete snapshot of the node's persistent Raft metadata.</summary>
/// <param name="Term">The node's current term.</param>
/// <param name="Vote">The candidate voted for in this term, or zero if none.</param>
/// <param name="Commit">The highest committed log index known to this node.</param>
public readonly record struct RaftHardState(ulong Term, ulong Vote, ulong Commit);
