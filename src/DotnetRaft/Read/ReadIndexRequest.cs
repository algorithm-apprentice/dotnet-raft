using DotnetRaft.Protocol;

namespace DotnetRaft.Read;

internal sealed record ReadIndexRequest(
    Message Request,
    ulong Index);
