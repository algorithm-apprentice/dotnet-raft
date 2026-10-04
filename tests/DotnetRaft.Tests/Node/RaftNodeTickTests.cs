using DotnetRaft.Diagnostics;
using DotnetRaft.Storage;

using static DotnetRaft.Tests.Node.RaftNodeTestSupport;

namespace DotnetRaft.Tests.Node;

public sealed class RaftNodeTickTests
{
    [Fact]
    public async Task CommandFloodDoesNotStarveTicks()
    {
        (RaftNode node, _) = RestartNode(
            electionTick: 2);
        try
        {
            Task<Status>[] statuses =
                Enumerable.Range(0, 500)
                    .Select(_ =>
                        node.GetStatusAsync().AsTask())
                    .ToArray();
            for (var tick = 0; tick < 4; tick++)
            {
                node.Tick();
            }

            await Task.WhenAll(statuses)
                .WaitAsync(TimeSpan.FromSeconds(2));
            Ready ready = await WaitReadyAsync(node);
            Assert.Equal(
                RaftRole.Candidate,
                ready.SoftState?.Role);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task TickOverflowLogsOnceFromOwnerLoop()
    {
        var trace = new BlockingTraceSink();
        var logger = new RecordingLogger();
        (RaftNode node, _) = RestartNode(
            traceSink: trace,
            logger: logger);
        try
        {
            Task campaign =
                node.CampaignAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    TimeSpan.FromSeconds(2)));
            int callerThread =
                Environment.CurrentManagedThreadId;

            for (var tick = 0; tick < 400; tick++)
            {
                node.Tick();
            }

            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    TimeSpan.FromSeconds(2)));
            await campaign.WaitAsync(
                TimeSpan.FromSeconds(2));
            Assert.True(
                logger.WarningWritten.Wait(
                    TimeSpan.FromSeconds(2)));

            (
                RaftLogLevel Level,
                string Message,
                int ThreadId) warning =
                Assert.Single(
                    logger.Events,
                    item =>
                        item.Level
                        == RaftLogLevel.Warning);
            Assert.Contains(
                "missed one or more ticks",
                warning.Message,
                StringComparison.Ordinal);
            Assert.NotEqual(
                callerThread,
                warning.ThreadId);
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task TickWarningLoggerFailureFaultsNode()
    {
        var trace = new BlockingTraceSink();
        var logger = new RecordingLogger
        {
            ThrowOnWarning = true,
        };
        (RaftNode node, _) = RestartNode(
            traceSink: trace,
            logger: logger);

        try
        {
            Task campaign =
                node.CampaignAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    TimeSpan.FromSeconds(2)));
            for (var tick = 0; tick < 400; tick++)
            {
                node.Tick();
            }

            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    TimeSpan.FromSeconds(2)));
            await campaign.WaitAsync(
                TimeSpan.FromSeconds(2));
            await Assert.ThrowsAsync<RaftNodeFaultedException>(
                async () => await node.Completion);
            Assert.True(
                logger.WarningWritten.IsSet);
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
    public async Task TickOverflowSkipsDisabledWarning()
    {
        var trace = new BlockingTraceSink();
        var logger = new RecordingLogger
        {
            WarningEnabled = false,
        };
        (RaftNode node, _) = RestartNode(
            traceSink: trace,
            logger: logger);
        try
        {
            Task campaign =
                node.CampaignAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    TimeSpan.FromSeconds(2)));
            for (var tick = 0; tick < 400; tick++)
            {
                node.Tick();
            }

            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    TimeSpan.FromSeconds(2)));
            await campaign.WaitAsync(
                TimeSpan.FromSeconds(2));
            _ = await node.GetStatusAsync()
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(2));

            Assert.DoesNotContain(
                logger.Events,
                item =>
                    item.Level
                    == RaftLogLevel.Warning);
            Assert.False(node.Completion.IsCompleted);
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task AcceptedTickDoesNotEmitMissedWarning()
    {
        var trace = new BlockingTraceSink();
        var logger = new RecordingLogger();
        (RaftNode node, _) = RestartNode(
            traceSink: trace,
            logger: logger);
        try
        {
            Task campaign =
                node.CampaignAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    TimeSpan.FromSeconds(2)));

            node.Tick();
            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    TimeSpan.FromSeconds(2)));
            await campaign.WaitAsync(
                TimeSpan.FromSeconds(2));
            _ = await node.GetStatusAsync();

            Assert.DoesNotContain(
                logger.Events,
                item =>
                    item.Level
                    == RaftLogLevel.Warning);
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task StartedNodeUsesConfiguredWarningLogger()
    {
        var block = 0;
        var trace = new BlockingTraceSink(
            traceEvent =>
                Volatile.Read(ref block) != 0
                && traceEvent.Type
                    == RaftTraceEventType.MessageReceived);
        var logger = new RecordingLogger();
        var node = RaftNode.Start(
            new RaftConfig
            {
                Id = 1,
                ElectionTick = 10,
                HeartbeatTick = 1,
                Storage = new MemoryStorage(),
                TraceSink = trace,
                Logger = logger,
            },
            [new Peer(1)]);
        try
        {
            Volatile.Write(ref block, 1);
            Task campaign =
                node.CampaignAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    TimeSpan.FromSeconds(2)));
            for (var tick = 0; tick < 400; tick++)
            {
                node.Tick();
            }

            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    TimeSpan.FromSeconds(2)));
            await campaign.WaitAsync(
                TimeSpan.FromSeconds(2));
            Assert.True(
                logger.WarningWritten.Wait(
                    TimeSpan.FromSeconds(2)));
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }
}
