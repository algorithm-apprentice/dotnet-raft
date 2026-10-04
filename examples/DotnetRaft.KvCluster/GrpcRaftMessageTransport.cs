using System.Collections.Concurrent;

using DotnetRaft.Examples.KvCluster.Transport;
using DotnetRaft.Protocol;

using Google.Protobuf;

using Grpc.Core;
using Grpc.Net.Client;

namespace DotnetRaft.Examples.KvCluster;

public sealed partial class GrpcRaftMessageTransport
    : IRaftMessageTransport
{
    private readonly ConcurrentDictionary<
        ulong,
        PeerClient> clients = [];
    private readonly ILogger<GrpcRaftMessageTransport> logger;
    private readonly ValidatedClusterOptions options;

    public GrpcRaftMessageTransport(
        ValidatedClusterOptions options,
        ILogger<GrpcRaftMessageTransport> logger)
    {
        this.options = options;
        this.logger = logger;
    }

    public async ValueTask<RaftSendResult> SendAsync(
        Message message,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (!options.Peers.TryGetValue(
                message.To,
                out Uri? address)
            || message.To == options.NodeId)
        {
            throw new InvalidOperationException(
                $"Message target {message.To} is not a configured remote peer.");
        }

        PeerClient peer = clients.GetOrAdd(
            message.To,
            _ => new PeerClient(
                address,
                options.MaxTransportMessageBytes));
        ByteString payload =
            message.ToByteString();
        if (payload.Length
            > options.MaxTransportMessageBytes)
        {
            LogOversize(
                logger,
                message.Type,
                message.From,
                message.To,
                payload.Length,
                options.MaxTransportMessageBytes);
            return RaftSendResult.Unavailable;
        }

        var envelope = new RaftEnvelope
        {
            Payload = payload,
        };
        DateTime deadline =
            DateTime.UtcNow + options.TransportTimeout;

        try
        {
            await peer.Client.SendAsync(
                    envelope,
                    deadline: deadline,
                    cancellationToken: cancellationToken)
                .ResponseAsync
                .ConfigureAwait(false);
            return RaftSendResult.Delivered;
        }
        catch (RpcException exception)
            when (!cancellationToken.IsCancellationRequested
                  && exception.StatusCode is
                      StatusCode.Unavailable
                      or StatusCode.DeadlineExceeded
                      or StatusCode.ResourceExhausted)
        {
            LogUnavailable(
                logger,
                message.Type,
                message.From,
                message.To,
                exception);
            return RaftSendResult.Unavailable;
        }
    }

    public ValueTask DisposeAsync()
    {
        foreach (PeerClient peer in clients.Values)
        {
            peer.Channel.Dispose();
        }

        clients.Clear();
        return ValueTask.CompletedTask;
    }

    private sealed class PeerClient
    {
        internal PeerClient(
            Uri address,
            int maximumMessageBytes)
        {
            Channel = GrpcChannel.ForAddress(
                address,
                new GrpcChannelOptions
                {
                    MaxReceiveMessageSize =
                        maximumMessageBytes,
                    MaxSendMessageSize =
                        maximumMessageBytes,
                });
            Client = new RaftTransport.RaftTransportClient(
                Channel);
        }

        internal GrpcChannel Channel { get; }

        internal RaftTransport.RaftTransportClient Client { get; }
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Warning,
        Message = "Raft message {MessageType} from {From} to {To} was not delivered.")]
    private static partial void LogUnavailable(
        ILogger logger,
        MessageType messageType,
        ulong from,
        ulong to,
        Exception exception);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Warning,
        Message = "Raft message {MessageType} from {From} to {To} is {PayloadBytes} bytes and exceeds transport maximum {MaximumBytes}.")]
    private static partial void LogOversize(
        ILogger logger,
        MessageType messageType,
        ulong from,
        ulong to,
        int payloadBytes,
        int maximumBytes);
}
