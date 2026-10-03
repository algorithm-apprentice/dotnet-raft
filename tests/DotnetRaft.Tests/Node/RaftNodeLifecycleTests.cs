using DotnetRaft.Diagnostics;

using static DotnetRaft.Tests.Node.RaftNodeTestSupport;

namespace DotnetRaft.Tests.Node;

public sealed class RaftNodeLifecycleTests
{
    [Fact]
    public async Task StopUnblocksWaitersAndIsIdempotent()
    {
        (RaftNode node, _) = RestartNode();
        Task proposal = node.ProposeAsync(
                "value"u8.ToArray())
            .AsTask();
        Task<Ready> ready = node.WaitForReadyAsync()
            .AsTask();

        await node.StopAsync();
        await node.StopAsync();

        await Assert.ThrowsAsync<RaftNodeStoppedException>(
            async () => await proposal);
        await Assert.ThrowsAsync<RaftNodeStoppedException>(
            async () => await ready);
        await node.Completion;
        await Assert.ThrowsAsync<RaftNodeStoppedException>(
            async () => await node.CampaignAsync()
                .AsTask());
        node.Tick();
    }

    [Fact]
    public async Task StopWithOutstandingReadyTerminates()
    {
        (RaftNode node, _) = RestartNode();
        await node.CampaignAsync();
        _ = await WaitReadyAsync(node);

        await node.StopAsync()
            .AsTask()
            .WaitAsync(TimeSpan.FromSeconds(2));

        await Assert.ThrowsAsync<RaftNodeStoppedException>(
            async () => await node.AdvanceAsync()
                .AsTask());
    }

    [Fact]
    public async Task StopTransitionWinsOverLaterCancellation()
    {
        var trace = new BlockingTraceSink();
        (RaftNode node, _) = RestartNode(
            traceSink: trace);
        using var cancellation =
            new CancellationTokenSource();

        Task claimed = node.CampaignAsync().AsTask();
        Assert.True(
            trace.Entered.Wait(
                TimeSpan.FromSeconds(2)));
        Task<Status> pending =
            node.GetStatusAsync(
                    cancellation.Token)
                .AsTask();
        Task stop = node.StopAsync().AsTask();

        cancellation.Cancel();
        trace.Release.Set();

        await claimed.WaitAsync(
            TimeSpan.FromSeconds(2));
        await Assert.ThrowsAsync<RaftNodeStoppedException>(
            async () => await pending);
        await stop.WaitAsync(
            TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task TraceFailureFaultsNodeAndPublishesStatus()
    {
        var trace = new CallbackTraceSink
        {
            OnTrace = traceEvent =>
            {
                if (traceEvent.Type
                    == RaftTraceEventType.MessageReceived)
                {
                    throw new InvalidOperationException(
                        "trace failed");
                }
            },
        };
        (RaftNode node, _) = RestartNode(
            traceSink: trace);

        RaftTracingException trigger =
            await Assert.ThrowsAsync<RaftTracingException>(
                async () => await node.CampaignAsync()
                    .AsTask());
        Assert.IsType<InvalidOperationException>(
            trigger.InnerException);

        RaftNodeFaultedException completion =
            await Assert.ThrowsAsync<RaftNodeFaultedException>(
                async () => await node.Completion);
        Assert.NotNull(completion.InnerException);
        Assert.NotNull(node.TerminalStatus);
        await Assert.ThrowsAsync<RaftNodeFaultedException>(
            async () => await node.GetStatusAsync()
                .AsTask());
        await Assert.ThrowsAsync<RaftNodeFaultedException>(
            async () => await node.StopAsync()
                .AsTask());
    }

    [Fact]
    public async Task SameNodeCallbackReentryIsRejected()
    {
        var trace = new CallbackTraceSink();
        (RaftNode node, _) = RestartNode(
            traceSink: trace);
        Exception? rejection = null;
        trace.OnTrace = traceEvent =>
        {
            if (traceEvent.Type
                != RaftTraceEventType.MessageReceived)
            {
                return;
            }

            try
            {
                node.GetStatusAsync()
                    .AsTask()
                    .GetAwaiter()
                    .GetResult();
            }
            catch (Exception exception)
            {
                rejection = exception;
            }
        };
        try
        {
            await node.CampaignAsync();

            Assert.IsType<InvalidOperationException>(
                rejection);
            Assert.False(node.Completion.IsCompleted);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task UnexpectedLoggerFailureFaultsAfterMutation()
    {
        var logger = new RecordingLogger();
        (RaftNode node, _) = RestartNode(
            logger: logger);
        logger.ThrowOnInformation = true;

        InvalidOperationException trigger =
            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await node.CampaignAsync()
                    .AsTask());
        Assert.Contains(
            "Injected information",
            trigger.Message,
            StringComparison.Ordinal);
        await Assert.ThrowsAsync<RaftNodeFaultedException>(
            async () => await node.Completion);
        Assert.Equal(
            RaftRole.Candidate,
            node.TerminalStatus?.Basic.Role);
    }

    [Fact]
    public async Task EnqueueStopRacesAlwaysComplete()
    {
        for (var iteration = 0;
             iteration < 20;
             iteration++)
        {
            (RaftNode node, _) = RestartNode();
            Task<Status> status =
                node.GetStatusAsync().AsTask();
            Task stop = node.StopAsync().AsTask();
            try
            {
                _ = await status.WaitAsync(
                    TimeSpan.FromSeconds(2));
            }
            catch (RaftNodeStoppedException)
            {
            }

            await stop.WaitAsync(
                TimeSpan.FromSeconds(2));
            await node.Completion;
        }
    }
}
