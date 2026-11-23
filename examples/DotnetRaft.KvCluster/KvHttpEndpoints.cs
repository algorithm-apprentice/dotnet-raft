namespace DotnetRaft.Examples.KvCluster;

internal static class KvHttpEndpoints
{
    internal static void Map(WebApplication app, ValidatedClusterOptions options)
    {
        var httpPort = new LocalPortEndpointFilter(options.HttpPort);
        app.MapPut("/kv/{key}", PutAsync).AddEndpointFilter(httpPort);
        app.MapDelete("/kv/{key}", DeleteAsync).AddEndpointFilter(httpPort);
        app.MapGet("/kv/{key}", ReadAsync).AddEndpointFilter(httpPort);
        app.MapGet("/local/{key}", ReadLocal).AddEndpointFilter(httpPort);
        app.MapGet("/status", GetStatusAsync).AddEndpointFilter(httpPort);
        app.MapPost("/campaign", CampaignAsync).AddEndpointFilter(httpPort);
        app.MapGet("/", () => DescribeNode(options)).AddEndpointFilter(httpPort);
    }

    private static async Task<IResult> PutAsync(
        string key,
        SetValueRequest? request,
        RaftClusterHost cluster,
        HttpContext context)
    {
        if (string.IsNullOrWhiteSpace(key) || request is null || request.Value is null || request.RequestId == Guid.Empty)
        {
            return Results.BadRequest(new { error = "A nonempty key and value are required." });
        }

        try
        {
            ProposalResponse response = await cluster.PutAsync(
                key, request.Value, request.RequestId, context.RequestAborted).ConfigureAwait(false);
            return Results.Ok(response);
        }
        catch (KvRequestConflictException exception)
        {
            return ConflictResponse(exception);
        }
        catch (KvPayloadTooLargeException exception)
        {
            return PayloadTooLargeResponse(exception);
        }
        catch (TimeoutException exception)
        {
            return TimeoutResponse(exception);
        }
    }

    private static async Task<IResult> DeleteAsync(
        string key,
        Guid? requestId,
        RaftClusterHost cluster,
        HttpContext context)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return Results.BadRequest(new { error = "A nonempty key is required." });
        }

        if (requestId == Guid.Empty)
        {
            return Results.BadRequest(new { error = "Request ID must be nonempty when supplied." });
        }

        try
        {
            ProposalResponse response = await cluster.DeleteAsync(key, requestId, context.RequestAborted).ConfigureAwait(false);
            return Results.Ok(response);
        }
        catch (KvRequestConflictException exception)
        {
            return ConflictResponse(exception);
        }
        catch (KvPayloadTooLargeException exception)
        {
            return PayloadTooLargeResponse(exception);
        }
        catch (TimeoutException exception)
        {
            return TimeoutResponse(exception);
        }
    }

    private static async Task<IResult> ReadAsync(string key, RaftClusterHost cluster, HttpContext context)
    {
        try
        {
            ReadResponse response = await cluster.ReadAsync(key, context.RequestAborted).ConfigureAwait(false);
            return Results.Ok(response);
        }
        catch (TimeoutException exception)
        {
            return TimeoutResponse(exception);
        }
    }

    private static IResult ReadLocal(string key, RaftClusterHost cluster) => Results.Ok(cluster.ReadLocal(key));

    private static async Task<IResult> GetStatusAsync(RaftClusterHost cluster, HttpContext context) =>
        Results.Ok(await cluster.GetStatusAsync(context.RequestAborted).ConfigureAwait(false));

    private static async Task<IResult> CampaignAsync(RaftClusterHost cluster, HttpContext context)
    {
        await cluster.CampaignAsync(context.RequestAborted).ConfigureAwait(false);
        return Results.Accepted();
    }

    private static IResult DescribeNode(ValidatedClusterOptions options) => Results.Ok(new
    {
        sample = "dotnet-raft gRPC KV cluster",
        options.NodeId,
        options.HttpPort,
        options.GrpcPort,
        options.DataDirectory,
    });

    private static IResult ConflictResponse(KvRequestConflictException exception) =>
        Results.Conflict(new { error = exception.Message });

    private static IResult PayloadTooLargeResponse(KvPayloadTooLargeException exception) =>
        Results.Problem(exception.Message, statusCode: StatusCodes.Status413PayloadTooLarge);

    private static IResult TimeoutResponse(TimeoutException exception) =>
        Results.Problem(exception.Message, statusCode: StatusCodes.Status504GatewayTimeout);
}
