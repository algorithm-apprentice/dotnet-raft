namespace DotnetRaft.Examples.KvCluster;

public sealed class ClusterOptions
{
    public const string SectionName = "Raft";

    public ulong NodeId { get; init; }

    public int HttpPort { get; init; } = 7101;

    public int GrpcPort { get; init; } = 7201;

    public Dictionary<string, string> Peers { get; init; } = [];

    public int TickIntervalMilliseconds { get; init; } = 100;

    public int RequestTimeoutSeconds { get; init; } = 10;

    public int TransportTimeoutMilliseconds { get; init; } = 500;

    public bool AutomaticTicks { get; init; } = true;

    public string? DataDirectory { get; init; }

    public int SnapshotThresholdEntries
    {
        get;
        init;
    } = 1000;

    public int MaxTransportMessageBytes
    {
        get;
        init;
    } = 64 * 1024 * 1024;

    public ValidatedClusterOptions Validate()
    {
        if (NodeId == 0)
        {
            throw new InvalidOperationException(
                "Raft:NodeId must be nonzero.");
        }

        ValidatePort(HttpPort, nameof(HttpPort));
        ValidatePort(GrpcPort, nameof(GrpcPort));
        if (HttpPort == GrpcPort)
        {
            throw new InvalidOperationException(
                "Raft HTTP and gRPC ports must differ.");
        }

        if (TickIntervalMilliseconds <= 0)
        {
            throw new InvalidOperationException(
                "Raft:TickIntervalMilliseconds must be positive.");
        }

        if (RequestTimeoutSeconds <= 0)
        {
            throw new InvalidOperationException(
                "Raft:RequestTimeoutSeconds must be positive.");
        }

        if (TransportTimeoutMilliseconds <= 0)
        {
            throw new InvalidOperationException(
                "Raft:TransportTimeoutMilliseconds must be positive.");
        }

        if (SnapshotThresholdEntries <= 0)
        {
            throw new InvalidOperationException(
                "Raft:SnapshotThresholdEntries must be positive.");
        }

        if (MaxTransportMessageBytes
            <= 128 * 1024)
        {
            throw new InvalidOperationException(
                "Raft:MaxTransportMessageBytes must exceed 128 KiB.");
        }

        if (Peers.Count != 3)
        {
            throw new InvalidOperationException(
                "The educational cluster requires exactly three peers.");
        }

        var peers = new SortedDictionary<ulong, Uri>();
        foreach ((string rawId, string rawUri) in Peers)
        {
            if (!ulong.TryParse(
                    rawId,
                    System.Globalization.NumberStyles.None,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out ulong id)
                || id == 0)
            {
                throw new InvalidOperationException(
                    $"Peer ID '{rawId}' must be a nonzero unsigned integer.");
            }

            if (!Uri.TryCreate(
                    rawUri,
                    UriKind.Absolute,
                    out Uri? uri)
                || !string.Equals(
                    uri.Scheme,
                    Uri.UriSchemeHttp,
                    StringComparison.OrdinalIgnoreCase)
                || !uri.IsLoopback
                || !string.IsNullOrEmpty(uri.Query)
                || !string.IsNullOrEmpty(uri.Fragment)
                || uri.AbsolutePath != "/")
            {
                throw new InvalidOperationException(
                    $"Peer {id} must use a loopback HTTP URI without a path, query, or fragment.");
            }

            ValidatePort(uri.Port, $"peer {id}");
            if (!peers.TryAdd(id, uri))
            {
                throw new InvalidOperationException(
                    $"Peer ID {id} is duplicated.");
            }
        }

        if (!peers.TryGetValue(
                NodeId,
                out Uri? localUri))
        {
            throw new InvalidOperationException(
                $"Peer map does not contain local node {NodeId}.");
        }

        if (localUri.Port != GrpcPort)
        {
            throw new InvalidOperationException(
                $"Local peer URI port {localUri.Port} does not match Raft:GrpcPort {GrpcPort}.");
        }

        if (peers.Values
            .Select(uri => uri.Port)
            .Distinct()
            .Count() != peers.Count)
        {
            throw new InvalidOperationException(
                "Every peer must use a distinct gRPC port.");
        }

        string dataDirectory = Path.GetFullPath(
            string.IsNullOrWhiteSpace(DataDirectory)
                ? Path.Combine(
                    Environment.CurrentDirectory,
                    "data",
                    $"node-{NodeId}")
                : DataDirectory);

        return new ValidatedClusterOptions(
            NodeId,
            HttpPort,
            GrpcPort,
            peers,
            TimeSpan.FromMilliseconds(
                TickIntervalMilliseconds),
            TimeSpan.FromSeconds(
                RequestTimeoutSeconds),
            TimeSpan.FromMilliseconds(
                TransportTimeoutMilliseconds),
            AutomaticTicks,
            dataDirectory,
            SnapshotThresholdEntries,
            MaxTransportMessageBytes);
    }

    private static void ValidatePort(
        int port,
        string name)
    {
        if (port is <= 0 or > 65535)
        {
            throw new InvalidOperationException(
                $"{name} port {port} is outside 1..65535.");
        }
    }
}

public sealed record ValidatedClusterOptions(
    ulong NodeId,
    int HttpPort,
    int GrpcPort,
    IReadOnlyDictionary<ulong, Uri> Peers,
    TimeSpan TickInterval,
    TimeSpan RequestTimeout,
    TimeSpan TransportTimeout,
    bool AutomaticTicks,
    string DataDirectory,
    int SnapshotThresholdEntries,
    int MaxTransportMessageBytes);
