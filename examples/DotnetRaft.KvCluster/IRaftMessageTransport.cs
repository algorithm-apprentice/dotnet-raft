using DotnetRaft.Protocol;

namespace DotnetRaft.Examples.KvCluster;

public enum RaftSendResult
{
    Delivered,
    Unavailable,
}

public interface IRaftMessageTransport : IAsyncDisposable
{
    ValueTask<RaftSendResult> SendAsync(
        Message message,
        CancellationToken cancellationToken);
}
