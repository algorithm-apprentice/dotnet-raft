using DotnetRaft.Examples.KvCluster;
using DotnetRaft.Storage.Sqlite;

using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);
var configured =
    new ClusterOptions();
builder.Configuration
    .GetSection(ClusterOptions.SectionName)
    .Bind(configured);
ValidatedClusterOptions options =
    configured.Validate();

builder.WebHost.ConfigureKestrel(server =>
{
    server.Limits.MaxRequestBodySize =
        options.MaxTransportMessageBytes;
    server.ListenLocalhost(
        options.HttpPort,
        endpoint =>
        {
            endpoint.Protocols =
                HttpProtocols.Http1;
        });
    server.ListenLocalhost(
        options.GrpcPort,
        endpoint =>
            endpoint.Protocols =
                HttpProtocols.Http2);
});

builder.Services.AddGrpc(grpc =>
{
    grpc.MaxReceiveMessageSize =
        options.MaxTransportMessageBytes;
    grpc.MaxSendMessageSize =
        options.MaxTransportMessageBytes;
});
builder.Services.AddSingleton(options);
builder.Services.AddSingleton<
    SqliteStorage>(
    _ => new SqliteStorage(
        Path.Combine(
            options.DataDirectory,
            "raft.db")));
builder.Services.AddSingleton<
    SqliteKeyValueStateMachine>(
    services =>
        DurableHostRecovery.OpenApplication(
            services.GetRequiredService<
                SqliteStorage>(),
            Path.Combine(
                options.DataDirectory,
                "application.db"),
            options.NodeId));
builder.Services.AddSingleton<
    PendingProposalRegistry>();
builder.Services.AddSingleton<
    PendingReadRegistry>();
builder.Services.AddSingleton<
    IRaftTickSource,
    PeriodicRaftTickSource>();
builder.Services.AddSingleton<
    IRaftMessageTransport,
    GrpcRaftMessageTransport>();
builder.Services.AddSingleton<RaftClusterHost>();
builder.Services.AddSingleton<
    IRaftMessageReceiver>(
    services =>
        services.GetRequiredService<
            RaftClusterHost>());
builder.Services.AddSingleton<IHostedService>(
    services =>
        services.GetRequiredService<
            RaftClusterHost>());

WebApplication app = builder.Build();
app.MapGrpcService<RaftTransportService>();

var httpPort =
    new LocalPortEndpointFilter(
        options.HttpPort);

app.MapPut(
        "/kv/{key}",
        async (
            string key,
            SetValueRequest? request,
            RaftClusterHost cluster,
            HttpContext context) =>
        {
            if (string.IsNullOrWhiteSpace(key)
                || request is null
                || request.Value is null
                || request.RequestId
                    == Guid.Empty)
            {
                return Results.BadRequest(new
                {
                    error =
                        "A nonempty key and value are required.",
                });
            }

            try
            {
                ProposalResponse response =
                    await cluster.PutAsync(
                            key,
                            request.Value,
                            request.RequestId,
                            context.RequestAborted)
                        .ConfigureAwait(false);
                return Results.Ok(response);
            }
            catch (KvRequestConflictException exception)
            {
                return Results.Conflict(new
                {
                    error = exception.Message,
                });
            }
            catch (KvPayloadTooLargeException exception)
            {
                return Results.Problem(
                    exception.Message,
                    statusCode:
                        StatusCodes
                            .Status413PayloadTooLarge);
            }
            catch (TimeoutException exception)
            {
                return Results.Problem(
                    exception.Message,
                    statusCode:
                        StatusCodes
                            .Status504GatewayTimeout);
            }
        })
    .AddEndpointFilter(httpPort);

app.MapDelete(
        "/kv/{key}",
        async (
            string key,
            Guid? requestId,
            RaftClusterHost cluster,
            HttpContext context) =>
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                return Results.BadRequest(new
                {
                    error =
                        "A nonempty key is required.",
                });
            }

            if (requestId == Guid.Empty)
            {
                return Results.BadRequest(new
                {
                    error =
                        "Request ID must be nonempty when supplied.",
                });
            }

            try
            {
                ProposalResponse response =
                    await cluster.DeleteAsync(
                            key,
                            requestId,
                            context.RequestAborted)
                        .ConfigureAwait(false);
                return Results.Ok(response);
            }
            catch (KvRequestConflictException exception)
            {
                return Results.Conflict(new
                {
                    error = exception.Message,
                });
            }
            catch (KvPayloadTooLargeException exception)
            {
                return Results.Problem(
                    exception.Message,
                    statusCode:
                        StatusCodes
                            .Status413PayloadTooLarge);
            }
            catch (TimeoutException exception)
            {
                return Results.Problem(
                    exception.Message,
                    statusCode:
                        StatusCodes
                            .Status504GatewayTimeout);
            }
        })
    .AddEndpointFilter(httpPort);

app.MapGet(
        "/kv/{key}",
        async (
            string key,
            RaftClusterHost cluster,
            HttpContext context) =>
        {
            try
            {
                ReadResponse response =
                    await cluster.ReadAsync(
                            key,
                            context.RequestAborted)
                        .ConfigureAwait(false);
                return Results.Ok(response);
            }
            catch (TimeoutException exception)
            {
                return Results.Problem(
                    exception.Message,
                    statusCode:
                        StatusCodes
                            .Status504GatewayTimeout);
            }
        })
    .AddEndpointFilter(httpPort);

app.MapGet(
        "/local/{key}",
        (
            string key,
            RaftClusterHost cluster) =>
            Results.Ok(
                cluster.ReadLocal(key)))
    .AddEndpointFilter(httpPort);

app.MapGet(
        "/status",
        async (
            RaftClusterHost cluster,
            HttpContext context) =>
            Results.Ok(
                await cluster.GetStatusAsync(
                        context.RequestAborted)
                    .ConfigureAwait(false)))
    .AddEndpointFilter(httpPort);

app.MapPost(
        "/campaign",
        async (
            RaftClusterHost cluster,
            HttpContext context) =>
        {
            await cluster.CampaignAsync(
                    context.RequestAborted)
                .ConfigureAwait(false);
            return Results.Accepted();
        })
    .AddEndpointFilter(httpPort);

app.MapGet(
        "/",
        () => Results.Ok(new
        {
            sample = "dotnet-raft gRPC KV cluster",
            options.NodeId,
            options.HttpPort,
            options.GrpcPort,
            options.DataDirectory,
        }))
    .AddEndpointFilter(httpPort);

await app.RunAsync();
