using System.Collections.Concurrent;
using System.Threading.Channels;

using DotnetRaft.Examples.KvCluster;
using DotnetRaft.Protocol;
using DotnetRaft.Storage.Sqlite;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;

namespace DotnetRaft.Tests.Examples;

public sealed class KvClusterIntegrationTests
{
    [Fact]
    public async Task ThreeNodeClusterReplicatesFollowerProposalAndRead()
    {
        var network = new InMemoryNetwork();
        NodeHarness[] nodes =
        [
            CreateNode(1, network),
            CreateNode(2, network),
            CreateNode(3, network),
        ];
        foreach (NodeHarness node in nodes)
        {
            network.Register(node.Host);
        }

        try
        {
            foreach (NodeHarness node in nodes)
            {
                await node.Host.StartAsync(
                    CancellationToken.None);
            }

            await WaitUntilAsync(
                () => nodes.All(node =>
                    node.State.PhysicalApplied >= 3));

            await nodes[0].Host.CampaignAsync(
                CancellationToken.None);
            await WaitUntilAsync(async () =>
                (await nodes[0].Host.GetStatusAsync(
                    CancellationToken.None)).Role
                == RaftRole.Leader);

            ProposalResponse proposal =
                await nodes[1].Host.PutAsync(
                    "color",
                    "blue",
                    CancellationToken.None);
            Assert.True(proposal.AppliedIndex >= 4);

            await WaitUntilAsync(
                () => nodes.All(node =>
                {
                    KeyValueReadResult result =
                        node.State.ReadLocal(
                            "color");
                    return result.Found
                        && result.Value == "blue";
                }));

            ReadResponse read =
                await nodes[2].Host.ReadAsync(
                    "color",
                    CancellationToken.None);
            Assert.True(read.Linearizable);
            Assert.True(read.Found);
            Assert.Equal("blue", read.Value);
            Assert.True(
                read.PhysicalApplied >=
                read.RequiredIndex);

            await nodes[1].Host.DeleteAsync(
                "color",
                requestId: null,
                CancellationToken.None);
            await WaitUntilAsync(
                () => nodes.All(node =>
                    !node.State.ReadLocal(
                        "color").Found));
        }
        finally
        {
            foreach (NodeHarness node in
                     nodes.Reverse())
            {
                await node.Host.StopAsync(
                    CancellationToken.None);
                await DisposeNodeAsync(node);
            }
        }
    }

    [Fact]
    public async Task ThreeNodeClusterRestartsDurably()
    {
        TemporaryNodeDirectory[] directories =
        [
            new(),
            new(),
            new(),
        ];
        Guid requestId = Guid.Parse(
            "11111111-2222-3333-4444-555555555555");
        try
        {
            ProposalResponse original;
            {
                var network = new InMemoryNetwork();
                NodeHarness[] nodes =
                [
                    CreateNode(
                        1,
                        network,
                        directory: directories[0]),
                    CreateNode(
                        2,
                        network,
                        directory: directories[1]),
                    CreateNode(
                        3,
                        network,
                        directory: directories[2]),
                ];
                foreach (NodeHarness node in nodes)
                {
                    network.Register(node.Host);
                    await node.Host.StartAsync(
                        CancellationToken.None);
                }

                try
                {
                    await WaitUntilAsync(
                        () => nodes.All(node =>
                            node.State
                                .PhysicalApplied
                            >= 3));
                    await nodes[0].Host
                        .CampaignAsync(
                            CancellationToken.None);
                    await WaitUntilAsync(async () =>
                        (await nodes[0].Host
                            .GetStatusAsync(
                                CancellationToken.None))
                        .Role == RaftRole.Leader);
                    original =
                        await nodes[1].Host.PutAsync(
                            "color",
                            "blue",
                            requestId,
                            CancellationToken.None);
                    await WaitUntilAsync(
                        () => nodes.All(node =>
                            node.State.ReadLocal(
                                "color").Value
                            == "blue"));
                }
                finally
                {
                    await StopAndDisposeNodesAsync(
                        nodes,
                        disposeDirectories: false);
                }
            }

            {
                var network = new InMemoryNetwork();
                NodeHarness[] nodes =
                [
                    CreateNode(
                        1,
                        network,
                        directory: directories[0]),
                    CreateNode(
                        2,
                        network,
                        directory: directories[1]),
                    CreateNode(
                        3,
                        network,
                        directory: directories[2]),
                ];
                foreach (NodeHarness node in nodes)
                {
                    network.Register(node.Host);
                    await node.Host.StartAsync(
                        CancellationToken.None);
                }

                try
                {
                    await WaitUntilAsync(
                        () => nodes.All(node =>
                            node.State.ReadLocal(
                                "color").Value
                            == "blue"));
                    await nodes[0].Host
                        .CampaignAsync(
                            CancellationToken.None);
                    await WaitUntilAsync(async () =>
                        (await nodes[0].Host
                            .GetStatusAsync(
                                CancellationToken.None))
                        .Role == RaftRole.Leader);
                    await nodes[1].Host.PutAsync(
                        "shape",
                        "circle",
                        Guid.Parse(
                            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                        CancellationToken.None);
                    ReadResponse freshRead =
                        await nodes[2].Host.ReadAsync(
                            "shape",
                            CancellationToken.None);
                    Assert.Equal(
                        "circle",
                        freshRead.Value);
                    ProposalResponse duplicate =
                        await nodes[2].Host.PutAsync(
                            "color",
                            "blue",
                            requestId,
                            CancellationToken.None);
                    Assert.True(duplicate.Duplicate);
                    Assert.Equal(
                        original.AppliedIndex,
                        duplicate.AppliedIndex);
                }
                finally
                {
                    await StopAndDisposeNodesAsync(
                        nodes,
                        disposeDirectories: false);
                }
            }
        }
        finally
        {
            foreach (TemporaryNodeDirectory directory in
                     directories)
            {
                directory.Dispose();
            }
        }
    }

    [Fact]
    public async Task ConcurrentConflictingRequestIdsReturnOneConflict()
    {
        var network = new InMemoryNetwork();
        NodeHarness[] nodes =
        [
            CreateNode(1, network),
            CreateNode(2, network),
            CreateNode(3, network),
        ];
        foreach (NodeHarness node in nodes)
        {
            network.Register(node.Host);
        }

        try
        {
            foreach (NodeHarness node in nodes)
            {
                await node.Host.StartAsync(
                    CancellationToken.None);
            }

            await WaitUntilAsync(
                () => nodes.All(node =>
                    node.State.PhysicalApplied >= 3));
            await nodes[0].Host.CampaignAsync(
                CancellationToken.None);
            await WaitUntilAsync(async () =>
                (await nodes[0].Host.GetStatusAsync(
                    CancellationToken.None)).Role
                == RaftRole.Leader);

            Guid requestId = Guid.Parse(
                "11111111-2222-3333-4444-555555555555");
            Task<ProposalResponse> first =
                nodes[1].Host.PutAsync(
                    "first",
                    "one",
                    requestId,
                    CancellationToken.None);
            Task<ProposalResponse> second =
                nodes[2].Host.PutAsync(
                    "second",
                    "two",
                    requestId,
                    CancellationToken.None);
            var successes = 0;
            var conflicts = 0;
            foreach (Task<ProposalResponse> task in
                     new[] { first, second })
            {
                try
                {
                    _ = await task;
                    successes++;
                }
                catch (KvRequestConflictException)
                {
                    conflicts++;
                }
            }

            Assert.Equal(1, successes);
            Assert.Equal(1, conflicts);
            await WaitUntilAsync(
                () => nodes.All(node =>
                {
                    bool firstFound =
                        node.State.ReadLocal(
                            "first").Found;
                    bool secondFound =
                        node.State.ReadLocal(
                            "second").Found;
                    return firstFound ^ secondFound;
                }));
        }
        finally
        {
            foreach (NodeHarness node in
                     nodes.Reverse())
            {
                await node.Host.StopAsync(
                    CancellationToken.None);
                await DisposeNodeAsync(node);
            }
        }
    }

    [Fact]
    public async Task OversizeSnapshotRetriesAfterThreshold()
    {
        var network = new InMemoryNetwork();
        NodeHarness[] nodes =
        [
            CreateNode(
                1,
                network,
                maxTransportMessageBytes:
                    256 * 1024,
                snapshotThresholdEntries: 3),
            CreateNode(
                2,
                network,
                maxTransportMessageBytes:
                    256 * 1024,
                snapshotThresholdEntries: 3),
            CreateNode(
                3,
                network,
                maxTransportMessageBytes:
                    256 * 1024,
                snapshotThresholdEntries: 3),
        ];
        foreach (NodeHarness node in nodes)
        {
            network.Register(node.Host);
        }

        try
        {
            foreach (NodeHarness node in nodes)
            {
                await node.Host.StartAsync(
                    CancellationToken.None);
            }

            await WaitUntilAsync(
                () => nodes.All(node =>
                    node.State.PhysicalApplied >= 3));
            await nodes[0].Host.CampaignAsync(
                CancellationToken.None);
            await WaitUntilAsync(async () =>
                (await nodes[0].Host.GetStatusAsync(
                    CancellationToken.None)).Role
                == RaftRole.Leader);

            string large = new(
                'x',
                100 * 1024);
            for (var index = 0;
                 index < 3;
                 index++)
            {
                await nodes[1].Host.PutAsync(
                    $"large-{index}",
                    large,
                    Guid.NewGuid(),
                    CancellationToken.None);
            }

            await WaitUntilAsync(
                () => nodes.All(node =>
                    SnapshotAttemptIndex(
                        node.Host) >= 6));
            Assert.All(
                nodes,
                node => Assert.Equal(
                    3UL,
                    node.Storage.GetSnapshot()
                        .Metadata.Index));

            await nodes[1].Host.PutAsync(
                "small",
                "value",
                Guid.NewGuid(),
                CancellationToken.None);
            await WaitUntilAsync(async () =>
            {
                foreach (NodeHarness node in nodes)
                {
                    ClusterStatusResponse status =
                        await node.Host
                            .GetStatusAsync(
                                CancellationToken.None);
                    if (status.LogicalApplied < 7)
                    {
                        return false;
                    }
                }

                return true;
            });

            Assert.All(
                nodes,
                node => Assert.Equal(
                    6UL,
                    SnapshotAttemptIndex(
                        node.Host)));
        }
        finally
        {
            await StopAndDisposeNodesAsync(
                nodes,
                disposeDirectories: true);
        }
    }

    [Fact]
    public async Task AutomaticTickLoopConsumesManualTicks()
    {
        var network = new InMemoryNetwork();
        NodeHarness node = CreateNode(
            1,
            network,
            automaticTicks: true);
        network.Register(node.Host);

        try
        {
            await node.Host.StartAsync(
                CancellationToken.None);
            await WaitUntilAsync(
                () => node.State.PhysicalApplied >= 3);

            for (var tick = 0; tick < 20; tick++)
            {
                node.TickSource.Tick();
            }

            await WaitUntilAsync(async () =>
                (await node.Host.GetStatusAsync(
                    CancellationToken.None)).Role
                == RaftRole.PreCandidate);
        }
        finally
        {
            await node.Host.StopAsync(
                CancellationToken.None);
            await DisposeNodeAsync(node);
        }
    }

    [Fact]
    public async Task ReadyLoopFaultStopsApplicationAndNode()
    {
        using var directory =
            new TemporaryNodeDirectory();
        ValidatedClusterOptions options =
            KvClusterComponentTests.Options(
                    nodeId: 1,
                    dataDirectory:
                        directory.Path)
                .Validate();
        var lifetime =
            new TestApplicationLifetime();
        var tickSource = new ManualTickSource();
        var transport = new FaultingTransport();
        var storage = new SqliteStorage(
            directory.RaftPath);
        var state =
            new SqliteKeyValueStateMachine(
                directory.ApplicationPath,
                nodeId: 1);
        var host = new RaftClusterHost(
            options,
            storage,
            state,
            new PendingProposalRegistry(),
            new PendingReadRegistry(),
            tickSource,
            transport,
            lifetime,
            NullLogger<RaftClusterHost>.Instance);

        try
        {
            await host.StartAsync(
                CancellationToken.None);
            await WaitUntilAsync(
                () => state.PhysicalApplied >= 3);
            await host.CampaignAsync(
                CancellationToken.None);
            await transport.Called.WaitAsync(
                TimeSpan.FromSeconds(5));
            await WaitUntilAsync(() =>
                host.ExecuteTask?.IsCompleted
                == true);

            Assert.True(
                lifetime.ApplicationStopping
                    .IsCancellationRequested);
            await Assert.ThrowsAsync<
                InvalidOperationException>(
                async () => await host.ExecuteTask!);
            await Assert.ThrowsAnyAsync<Exception>(
                async () => await host.GetStatusAsync(
                    CancellationToken.None));
        }
        finally
        {
            try
            {
                await host.StopAsync(
                    CancellationToken.None);
            }
            catch (InvalidOperationException)
            {
            }

            await tickSource.DisposeAsync();
            lifetime.Dispose();
            state.Dispose();
            storage.Dispose();
        }
    }

    private static NodeHarness CreateNode(
        ulong id,
        InMemoryNetwork network,
        bool automaticTicks = false,
        TemporaryNodeDirectory? directory = null,
        int maxTransportMessageBytes =
            64 * 1024 * 1024,
        int snapshotThresholdEntries = 10)
    {
        TemporaryNodeDirectory actualDirectory =
            directory
            ?? new TemporaryNodeDirectory();
        ClusterOptions configured =
            KvClusterComponentTests.Options(
                id,
                automaticTicks,
                dataDirectory:
                    actualDirectory.Path,
                maxTransportMessageBytes:
                    maxTransportMessageBytes,
                snapshotThresholdEntries:
                    snapshotThresholdEntries);
        ValidatedClusterOptions options =
            configured.Validate();
        var storage = new SqliteStorage(
            actualDirectory.RaftPath);
        var state =
            DurableHostRecovery.OpenApplication(
                storage,
                actualDirectory.ApplicationPath,
                id);
        var proposals =
            new PendingProposalRegistry();
        var reads = new PendingReadRegistry();
        var ticks = new ManualTickSource();
        InMemoryTransport transport =
            network.CreateTransport(id);
        var lifetime =
            new TestApplicationLifetime();
        var host = new RaftClusterHost(
            options,
            storage,
            state,
            proposals,
            reads,
            ticks,
            transport,
            lifetime,
            NullLogger<RaftClusterHost>.Instance);
        return new NodeHarness(
            host,
            state,
            transport,
            ticks,
            lifetime,
            storage,
            actualDirectory);
    }

    private static ulong SnapshotAttemptIndex(
        RaftClusterHost host)
    {
        return Assert.IsType<ulong>(
            typeof(RaftClusterHost)
                .GetField(
                    "lastSnapshotAttemptIndex",
                    System.Reflection.BindingFlags
                        .Instance
                    | System.Reflection.BindingFlags
                        .NonPublic)
                ?.GetValue(host));
    }

    private static async Task WaitUntilAsync(
        Func<bool> condition)
    {
        using var timeout =
            new CancellationTokenSource(
                TimeSpan.FromSeconds(5));
        while (!condition())
        {
            await Task.Delay(
                10,
                timeout.Token);
        }
    }

    private static async Task WaitUntilAsync(
        Func<Task<bool>> condition)
    {
        using var timeout =
            new CancellationTokenSource(
                TimeSpan.FromSeconds(5));
        while (!await condition())
        {
            await Task.Delay(
                10,
                timeout.Token);
        }
    }

    private static async Task DisposeNodeAsync(
        NodeHarness node,
        bool disposeDirectory = true)
    {
        await node.Transport.DisposeAsync();
        await node.TickSource.DisposeAsync();
        node.State.Dispose();
        node.Storage.Dispose();
        node.Lifetime.Dispose();
        if (disposeDirectory)
        {
            node.Directory.Dispose();
        }
    }

    private static async Task StopAndDisposeNodesAsync(
        IEnumerable<NodeHarness> nodes,
        bool disposeDirectories)
    {
        foreach (NodeHarness node in nodes.Reverse())
        {
            try
            {
                await node.Host.StopAsync(
                    CancellationToken.None);
            }
            finally
            {
                await DisposeNodeAsync(
                    node,
                    disposeDirectories);
            }
        }
    }

    private sealed record NodeHarness(
    RaftClusterHost Host,
    SqliteKeyValueStateMachine State,
    InMemoryTransport Transport,
    ManualTickSource TickSource,
    TestApplicationLifetime Lifetime,
    SqliteStorage Storage,
    TemporaryNodeDirectory Directory);

    private sealed class TemporaryNodeDirectory
        : IDisposable
    {
        internal TemporaryNodeDirectory()
        {
            Path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "dotnet-raft-kv-cluster-tests",
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Path);
        }

        internal string Path { get; }

        internal string RaftPath =>
            System.IO.Path.Combine(
                Path,
                "raft.db");

        internal string ApplicationPath =>
            System.IO.Path.Combine(
                Path,
                "application.db");

        public void Dispose()
        {
            if (Directory.Exists(Path))
            {
                Directory.Delete(
                    Path,
                    recursive: true);
            }
        }
    }

    private sealed class InMemoryNetwork
    {
        private readonly ConcurrentDictionary<
            ulong,
            IRaftMessageReceiver> receivers = [];

        internal InMemoryTransport CreateTransport(
            ulong senderId)
        {
            return new InMemoryTransport(
                senderId,
                receivers);
        }

        internal void Register(
            RaftClusterHost receiver)
        {
            Assert.True(
                receivers.TryAdd(
                    receiver.NodeId,
                    receiver));
        }
    }

    private sealed class InMemoryTransport
        : IRaftMessageTransport
    {
        private readonly ConcurrentDictionary<
            ulong,
            IRaftMessageReceiver> receivers;
        private readonly ulong senderId;

        internal InMemoryTransport(
            ulong senderId,
            ConcurrentDictionary<
                ulong,
                IRaftMessageReceiver> receivers)
        {
            this.senderId = senderId;
            this.receivers = receivers;
        }

        public async ValueTask<RaftSendResult> SendAsync(
            Message message,
            CancellationToken cancellationToken)
        {
            Assert.Equal(senderId, message.From);
            if (!receivers.TryGetValue(
                    message.To,
                    out IRaftMessageReceiver? receiver))
            {
                return RaftSendResult.Unavailable;
            }

            await receiver.ReceiveAsync(
                    message.Clone(),
                    cancellationToken)
                .ConfigureAwait(false);
            return RaftSendResult.Delivered;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FaultingTransport
        : IRaftMessageTransport
    {
        private readonly TaskCompletionSource called =
            new(
                TaskCreationOptions
                    .RunContinuationsAsynchronously);

        internal Task Called => called.Task;

        public ValueTask<RaftSendResult> SendAsync(
            Message message,
            CancellationToken cancellationToken)
        {
            called.TrySetResult();
            return ValueTask.FromException<
                RaftSendResult>(
                new InvalidOperationException(
                    "transport fault"));
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ManualTickSource
        : IRaftTickSource
    {
        private readonly Channel<bool> ticks =
            Channel.CreateUnbounded<bool>();

        internal void Tick()
        {
            Assert.True(
                ticks.Writer.TryWrite(true));
        }

        public ValueTask<bool> WaitForNextTickAsync(
            CancellationToken cancellationToken)
        {
            return ticks.Reader.ReadAsync(
                cancellationToken);
        }

        public ValueTask DisposeAsync()
        {
            ticks.Writer.TryComplete();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestApplicationLifetime
        : IHostApplicationLifetime,
          IDisposable
    {
        private readonly CancellationTokenSource
            started = new();
        private readonly CancellationTokenSource
            stopped = new();
        private readonly CancellationTokenSource
            stopping = new();

        internal TestApplicationLifetime()
        {
            started.Cancel();
        }

        public CancellationToken ApplicationStarted =>
            started.Token;

        public CancellationToken ApplicationStopping =>
            stopping.Token;

        public CancellationToken ApplicationStopped =>
            stopped.Token;

        public void StopApplication()
        {
            stopping.Cancel();
            stopped.Cancel();
        }

        public void Dispose()
        {
            started.Dispose();
            stopping.Dispose();
            stopped.Dispose();
        }
    }
}
