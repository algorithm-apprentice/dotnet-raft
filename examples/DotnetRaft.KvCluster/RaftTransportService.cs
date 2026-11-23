using DotnetRaft.Examples.KvCluster.Transport;
using DotnetRaft.Protocol;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

namespace DotnetRaft.Examples.KvCluster;

public sealed class RaftTransportService
    : RaftTransport.RaftTransportBase
{
    private readonly ValidatedClusterOptions options;
    private readonly IRaftMessageReceiver receiver;

    public RaftTransportService(
        ValidatedClusterOptions options,
        IRaftMessageReceiver receiver)
    {
        this.options = options;
        this.receiver = receiver;
    }

    public override async Task<Empty> Send(
        RaftEnvelope request,
        ServerCallContext context)
    {
        EnsureRaftListener(context);
        Message message = DecodePayload(request.Payload);
        await DeliverMessageAsync(message, context.CancellationToken).ConfigureAwait(false);
        return new Empty();
    }

    private void EnsureRaftListener(ServerCallContext context)
    {
        if (context.GetHttpContext()
                .Connection.LocalPort
            != options.GrpcPort)
        {
            throw new RpcException(
                new Grpc.Core.Status(
                    StatusCode.PermissionDenied,
                    "Raft transport is not available on this listener."));
        }
    }

    private static Message DecodePayload(ByteString payload)
    {
        if (payload.IsEmpty)
        {
            throw InvalidArgument(
                "Raft envelope payload must be nonempty.");
        }

        try
        {
            return Message.Parser.ParseFrom(payload);
        }
        catch (InvalidProtocolBufferException exception)
        {
            throw InvalidArgument(
                $"Raft payload is malformed: {exception.Message}");
        }
    }

    private async Task DeliverMessageAsync(Message message, CancellationToken cancellationToken)
    {
        try
        {
            await receiver.ReceiveAsync(
                    message,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (ArgumentException exception)
        {
            throw InvalidArgument(exception.Message);
        }
        catch (RaftNodeStoppedException exception)
        {
            throw Unavailable(exception.Message);
        }
        catch (RaftNodeFaultedException exception)
        {
            throw Unavailable(exception.Message);
        }
    }

    private static RpcException InvalidArgument(
        string detail)
    {
        return new RpcException(
            new Grpc.Core.Status(
                StatusCode.InvalidArgument,
                detail));
    }

    private static RpcException Unavailable(
        string detail)
    {
        return new RpcException(
            new Grpc.Core.Status(
                StatusCode.Unavailable,
                detail));
    }
}
