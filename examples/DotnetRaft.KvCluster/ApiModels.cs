namespace DotnetRaft.Examples.KvCluster;

public sealed record SetValueRequest(string Value);

public sealed record ProposalResponse(
    ulong NodeId,
    Guid RequestId,
    ulong AppliedIndex,
    ulong PhysicalApplied);

public sealed record ReadResponse(
    ulong NodeId,
    bool Found,
    string? Value,
    ulong RequiredIndex,
    ulong PhysicalApplied,
    bool Linearizable);

public sealed record ClusterStatusResponse(
    ulong NodeId,
    RaftRole Role,
    ulong Term,
    ulong LeaderId,
    ulong Commit,
    ulong LogicalApplied,
    ulong PhysicalApplied);
