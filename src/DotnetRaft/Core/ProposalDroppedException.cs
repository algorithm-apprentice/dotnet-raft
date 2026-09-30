namespace DotnetRaft.Core;

internal sealed class ProposalDroppedException(string message)
    : InvalidOperationException(message);
