namespace DotnetRaft.Core;

internal sealed class RaftInvariantException(string message) : Exception(message);
