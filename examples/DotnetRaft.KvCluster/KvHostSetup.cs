using DotnetRaft.Storage.Sqlite;

using Microsoft.AspNetCore.Server.Kestrel.Core;

namespace DotnetRaft.Examples.KvCluster;

internal static class KvHostSetup
{
    internal static ValidatedClusterOptions ReadOptions(IConfiguration configuration)
    {
        var configured = new ClusterOptions();
        configuration.GetSection(ClusterOptions.SectionName).Bind(configured);
        return configured.Validate();
    }

    internal static void ConfigureListeners(IWebHostBuilder webHost, ValidatedClusterOptions options)
    {
        webHost.ConfigureKestrel(server =>
        {
            server.Limits.MaxRequestBodySize = options.MaxTransportMessageBytes;
            server.ListenLocalhost(options.HttpPort, endpoint => endpoint.Protocols = HttpProtocols.Http1);
            server.ListenLocalhost(options.GrpcPort, endpoint => endpoint.Protocols = HttpProtocols.Http2);
        });
    }

    internal static void AddServices(IServiceCollection services, ValidatedClusterOptions options)
    {
        AddGrpcTransport(services, options);
        services.AddSingleton(options);
        AddDurableStores(services, options);
        AddRaftHost(services);
    }

    private static void AddGrpcTransport(IServiceCollection services, ValidatedClusterOptions options)
    {
        services.AddGrpc(grpc =>
        {
            grpc.MaxReceiveMessageSize = options.MaxTransportMessageBytes;
            grpc.MaxSendMessageSize = options.MaxTransportMessageBytes;
        });
    }

    private static void AddDurableStores(IServiceCollection services, ValidatedClusterOptions options)
    {
        services.AddSingleton(_ => new SqliteStorage(Path.Combine(options.DataDirectory, "raft.db")));
        services.AddSingleton(provider => DurableHostRecovery.OpenApplication(
            provider.GetRequiredService<SqliteStorage>(),
            Path.Combine(options.DataDirectory, "application.db"),
            options.NodeId));
    }

    private static void AddRaftHost(IServiceCollection services)
    {
        services.AddSingleton<PendingProposalRegistry>();
        services.AddSingleton<PendingReadRegistry>();
        services.AddSingleton<IRaftTickSource, PeriodicRaftTickSource>();
        services.AddSingleton<IRaftMessageTransport, GrpcRaftMessageTransport>();
        services.AddSingleton<RaftClusterHost>();
        // The receiver and background service must refer to the same node owner.
        services.AddSingleton<IRaftMessageReceiver>(provider => provider.GetRequiredService<RaftClusterHost>());
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredService<RaftClusterHost>());
    }
}
