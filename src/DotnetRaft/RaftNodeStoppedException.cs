namespace DotnetRaft;

public sealed class RaftNodeStoppedException()
    : InvalidOperationException(
        "The Raft node has stopped.");
