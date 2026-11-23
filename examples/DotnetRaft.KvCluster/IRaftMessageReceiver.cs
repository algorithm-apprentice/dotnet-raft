using DotnetRaft.Protocol;

namespace DotnetRaft.Examples.KvCluster;

public interface IRaftMessageReceiver
{
    ulong NodeId { get; }

    // The receiver validates peer routing before dispatch. The caller must
    // not mutate the message during the receive operation.
    ValueTask ReceiveAsync(
        Message message,
        CancellationToken cancellationToken);
}
