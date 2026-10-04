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
        _ = await node.GetStatusAsync();

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
        await Assert.ThrowsAsync<RaftNodeStoppedException>(
            async () => await node.ProposeAsync(
                    "stopped"u8.ToArray())
                .AsTask());
        await Assert.ThrowsAsync<RaftNodeStoppedException>(
            async () => await node.WaitForReadyAsync()
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
            .WaitAsync(DeadlockGuardTimeout);

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

        try
        {
            Task claimed = node.CampaignAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    DeadlockGuardTimeout));
            Task<Status> pending =
                node.GetStatusAsync(
                        cancellation.Token)
                    .AsTask();
            Task stop = node.StopAsync().AsTask();

            cancellation.Cancel();
            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    DeadlockGuardTimeout));

            await claimed.WaitAsync(
                DeadlockGuardTimeout);
            await Assert.ThrowsAsync<RaftNodeStoppedException>(
                async () => await pending);
            await stop.WaitAsync(
                DeadlockGuardTimeout);
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task StopDrainsEveryUnclaimedLane()
    {
        var trace = new BlockingTraceSink();
        var logger = new RecordingLogger();
        (RaftNode node, _) = RestartNode(
            traceSink: trace,
            logger: logger);

        try
        {
            Task claimed = node.CampaignAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    DeadlockGuardTimeout));
            Task<Status> command =
                node.GetStatusAsync().AsTask();
            Task proposal =
                node.ProposeAsync("queued"u8.ToArray())
                    .AsTask();
            Task<Ready> ready =
                node.WaitForReadyAsync().AsTask();
            for (var tick = 0; tick < 400; tick++)
            {
                node.Tick();
            }

            Task stop = node.StopAsync().AsTask();
            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    DeadlockGuardTimeout));

            await claimed.WaitAsync(
                DeadlockGuardTimeout);
            await Assert.ThrowsAsync<RaftNodeStoppedException>(
                async () => await command);
            await Assert.ThrowsAsync<RaftNodeStoppedException>(
                async () => await proposal);
            await Assert.ThrowsAsync<RaftNodeStoppedException>(
                async () => await ready);
            await stop.WaitAsync(
                DeadlockGuardTimeout);

            Assert.True(command.IsCompleted);
            Assert.True(proposal.IsCompleted);
            Assert.True(ready.IsCompleted);
            Assert.NotNull(node.TerminalStatus);
            Assert.DoesNotContain(
                logger.Events,
                item =>
                    item.Level
                    == RaftLogLevel.Warning);
            node.Tick();
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task ClaimedFaultOverridesConcurrentStop()
    {
        var trace = new BlockingTraceSink(
            afterRelease: _ =>
                throw new InvalidOperationException(
                    "fault after stop"));
        (RaftNode node, _) = RestartNode(
            traceSink: trace);

        try
        {
            Task claimed = node.CampaignAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    DeadlockGuardTimeout));
            Task<Status> command =
                node.GetStatusAsync().AsTask();
            Task proposal =
                node.ProposeAsync("queued"u8.ToArray())
                    .AsTask();
            Task<Ready> ready =
                node.WaitForReadyAsync().AsTask();
            Task stop = node.StopAsync().AsTask();

            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    DeadlockGuardTimeout));

            RaftTracingException trigger =
                await Assert.ThrowsAsync<RaftTracingException>(
                    async () => await claimed);
            Assert.IsType<InvalidOperationException>(
                trigger.InnerException);
            await Assert.ThrowsAsync<RaftNodeFaultedException>(
                async () => await command);
            await Assert.ThrowsAsync<RaftNodeFaultedException>(
                async () => await proposal);
            await Assert.ThrowsAsync<RaftNodeFaultedException>(
                async () => await ready);
            await Assert.ThrowsAsync<RaftNodeFaultedException>(
                async () => await stop);
            await Assert.ThrowsAsync<RaftNodeFaultedException>(
                async () => await node.Completion);
            await Assert.ThrowsAsync<RaftNodeFaultedException>(
                async () => await node.GetStatusAsync()
                    .AsTask());
            Assert.NotNull(node.TerminalStatus);
        }
        finally
        {
            trace.Release.Set();
            try
            {
                await node.StopAsync();
            }
            catch (RaftNodeFaultedException)
            {
            }
        }
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
        Assert.Same(
            trigger,
            completion.InnerException);
        Assert.NotNull(node.TerminalStatus);
        RaftNodeFaultedException future =
            await Assert.ThrowsAsync<RaftNodeFaultedException>(
            async () => await node.GetStatusAsync()
                .AsTask());
        Assert.Same(
            trigger,
            future.InnerException);
        await Assert.ThrowsAsync<RaftNodeFaultedException>(
            async () => await node.ProposeAsync(
                    "faulted"u8.ToArray())
                .AsTask());
        await Assert.ThrowsAsync<RaftNodeFaultedException>(
            async () => await node.WaitForReadyAsync()
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
        var rejections = new List<Exception>();
        trace.OnTrace = traceEvent =>
        {
            if (traceEvent.Type
                != RaftTraceEventType.MessageReceived)
            {
                return;
            }

            try
            {
                _ = node.Completion;
            }
            catch (Exception exception)
            {
                rejections.Add(exception);
            }

            try
            {
                node.Tick();
            }
            catch (Exception exception)
            {
                rejections.Add(exception);
            }

            try
            {
                _ = node.WaitForReadyAsync().AsTask();
            }
            catch (Exception exception)
            {
                rejections.Add(exception);
            }

            try
            {
                _ = node.AdvanceAsync().AsTask();
            }
            catch (Exception exception)
            {
                rejections.Add(exception);
            }

            try
            {
                _ = node.ProposeAsync(
                        "reentrant"u8.ToArray())
                    .AsTask();
            }
            catch (Exception exception)
            {
                rejections.Add(exception);
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
                rejections.Add(exception);
            }

            try
            {
                _ = node.StopAsync().AsTask();
            }
            catch (Exception exception)
            {
                rejections.Add(exception);
            }

            try
            {
                _ = node.DisposeAsync().AsTask();
            }
            catch (Exception exception)
            {
                rejections.Add(exception);
            }
        };
        try
        {
            await node.CampaignAsync();

            Assert.Equal(8, rejections.Count);
            Assert.All(
                rejections,
                rejection =>
                {
                    InvalidOperationException typed =
                        Assert.IsType<InvalidOperationException>(
                            rejection);
                    Assert.Equal(
                        "RaftNode methods cannot re-enter the same node from a trace or logger callback.",
                        typed.Message);
                });
            Assert.False(node.Completion.IsCompleted);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task ArgumentLoggerFailureFaultsAfterMutation()
    {
        var logger = new RecordingLogger();
        (RaftNode node, _) = RestartNode(
            logger: logger);
        logger.InformationFailure =
            new ArgumentException(
                "Injected argument logger failure.");

        RaftLoggingException trigger =
            await Assert.ThrowsAsync<RaftLoggingException>(
                async () => await node.CampaignAsync()
                    .AsTask());
        ArgumentException loggerFailure =
            Assert.IsType<ArgumentException>(
                trigger.InnerException);
        Assert.Equal(
            "Injected argument logger failure.",
            loggerFailure.Message);
        RaftNodeFaultedException completion =
            await Assert.ThrowsAsync<RaftNodeFaultedException>(
            async () => await node.Completion);
        Assert.Same(
            trigger,
            completion.InnerException);
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
                    DeadlockGuardTimeout);
            }
            catch (RaftNodeStoppedException)
            {
            }

            await stop.WaitAsync(
                DeadlockGuardTimeout);
            await node.Completion;
        }
    }

    [Fact]
    public async Task DisposeStopsNode()
    {
        (RaftNode node, _) = RestartNode();

        await node.DisposeAsync();
        await node.Completion;
        Assert.NotNull(node.TerminalStatus);
        await Assert.ThrowsAsync<RaftNodeStoppedException>(
            async () => await node.GetStatusAsync()
                .AsTask());
    }
}
