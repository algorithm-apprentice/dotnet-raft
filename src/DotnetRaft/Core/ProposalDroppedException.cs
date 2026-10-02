namespace DotnetRaft.Core;

public sealed class ProposalDroppedException(string message)
    : InvalidOperationException(message);
