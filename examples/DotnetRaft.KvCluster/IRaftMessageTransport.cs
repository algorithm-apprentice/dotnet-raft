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

public interface IRaftMessageReceiver
{
    ulong NodeId { get; }

    // The receiver validates peer routing before dispatch. The caller must
    // not mutate the message during the receive operation.
    ValueTask ReceiveAsync(
        Message message,
        CancellationToken cancellationToken);
}
