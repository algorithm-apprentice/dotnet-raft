using DotnetRaft.Examples.KvCluster;

// HTTP accepts business requests; gRPC carries Raft messages. The background
// host alone executes Ready's persistence, transport, and application work.
var builder = WebApplication.CreateBuilder(args);
ValidatedClusterOptions options = KvHostSetup.ReadOptions(builder.Configuration);
KvHostSetup.ConfigureListeners(builder.WebHost, options);
KvHostSetup.AddServices(builder.Services, options);

WebApplication app = builder.Build();
app.MapGrpcService<RaftTransportService>();
KvHttpEndpoints.Map(app, options);
await app.RunAsync();
