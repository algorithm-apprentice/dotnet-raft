using System.Collections.Concurrent;
using System.Threading.Channels;

using DotnetRaft.Examples.KvCluster;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

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
        }
        finally
        {
            foreach (NodeHarness node in
                     nodes.Reverse())
            {
                await node.Host.StopAsync(
                    CancellationToken.None);
                await node.Transport.DisposeAsync();
                await node.TickSource.DisposeAsync();
                node.Lifetime.Dispose();
            }
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
            await node.Transport.DisposeAsync();
            await node.TickSource.DisposeAsync();
            node.Lifetime.Dispose();
        }
    }

    [Fact]
    public async Task ReadyLoopFaultStopsApplicationAndNode()
    {
        ValidatedClusterOptions options =
            KvClusterComponentTests.Options(
                    nodeId: 1)
                .Validate();
        var lifetime =
            new TestApplicationLifetime();
        var tickSource = new ManualTickSource();
        var transport = new FaultingTransport();
        var state = new KeyValueStateMachine();
        var host = new RaftClusterHost(
            options,
            new MemoryStorage(),
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
        }
    }

    private static NodeHarness CreateNode(
        ulong id,
        InMemoryNetwork network,
        bool automaticTicks = false)
    {
        ValidatedClusterOptions options =
            KvClusterComponentTests.Options(
                    id,
                    automaticTicks)
                .Validate();
        var storage = new MemoryStorage();
        var state = new KeyValueStateMachine();
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
            lifetime);
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

    private sealed record NodeHarness(
        RaftClusterHost Host,
        KeyValueStateMachine State,
        InMemoryTransport Transport,
        ManualTickSource TickSource,
        TestApplicationLifetime Lifetime);

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
