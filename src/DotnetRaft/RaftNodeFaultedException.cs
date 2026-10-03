namespace DotnetRaft;

public sealed class RaftNodeFaultedException(
    Exception innerException)
    : InvalidOperationException(
        "The Raft node faulted and must be discarded.",
        innerException);
