namespace DotnetRaft.Examples.KvCluster;

public sealed class KvRequestConflictException(
    Guid requestId)
    : InvalidOperationException(
        $"Request ID {requestId} is already associated with another command.");
