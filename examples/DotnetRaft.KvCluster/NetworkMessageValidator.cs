using DotnetRaft.Examples.KvCluster.Transport;
using DotnetRaft.Protocol;

namespace DotnetRaft.Examples.KvCluster;

public static class NetworkMessageValidator
{
    public static void Validate(
        Message message,
        ValidatedClusterOptions options)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(options);

        if (!MessageClassifier.HasKnownType(message))
        {
            throw new ArgumentException(
                "Raft message type must be present and known.",
                nameof(message));
        }

        if (!message.HasFrom
            || message.From == 0
            || !message.HasTo
            || message.To == 0)
        {
            throw new ArgumentException(
                "Raft message must carry nonzero From and To values.",
                nameof(message));
        }

        if (message.To != options.NodeId)
        {
            throw new ArgumentException(
                $"Raft message target {message.To} does not match local node {options.NodeId}.",
                nameof(message));
        }

        if (message.From == options.NodeId
            || !options.Peers.ContainsKey(
                message.From))
        {
            throw new ArgumentException(
                $"Raft message sender {message.From} is not a configured remote peer.",
                nameof(message));
        }

        if (MessageClassifier.IsLocal(message.Type))
        {
            throw new ArgumentException(
                $"{message.Type} is a local control message and cannot cross the network.",
                nameof(message));
        }
    }
}
