namespace DotnetRaft;

public sealed record SoftState(
    ulong LeaderId,
    RaftRole Role);
