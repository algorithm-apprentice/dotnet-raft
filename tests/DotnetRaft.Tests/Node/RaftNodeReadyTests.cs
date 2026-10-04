using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using static DotnetRaft.Tests.Node.RaftNodeTestSupport;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Node;

public sealed class RaftNodeReadyTests
{
    [Fact]
    public async Task StartDeliversBootstrapReady()
    {
        var storage = new MemoryStorage();
        var node = RaftNode.Start(
            new RaftConfig
            {
                Id = 1,
                Storage = storage,
                ElectionTick = 10,
                HeartbeatTick = 1,
            },
            [new Peer(1)]);
        try
        {
            Ready ready = await WaitReadyAsync(node);

            Assert.Single(ready.Entries);
            Entry committed =
                Assert.Single(ready.CommittedEntries);
            Assert.Equal(
                EntryType.EntryConfChange,
                committed.Type);
            Assert.Equal(1UL, ready.HardState?.Term);
            storage.Append(ready.Entries);
            storage.SetHardState(ready.HardState!);
            await node.ApplyConfChangeAsync(
                ProtocolConfChange.Parser.ParseFrom(
                    committed.Data));
            await node.AdvanceAsync();
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task RestartDeliversCommittedWork()
    {
        MemoryStorage storage = CreateStorage([1]);
        storage.Append(
        [
            new Entry
            {
                Index = 3,
                Term = 1,
                Data = ByteString.CopyFromUtf8("value"),
            },
        ]);
        storage.SetHardState(
            new HardState
            {
                Term = 1,
                Commit = 3,
            });
        var node = RaftNode.Restart(
            new RaftConfig
            {
                Id = 1,
                Storage = storage,
                Applied = 2,
                ElectionTick = 10,
                HeartbeatTick = 1,
            });
        try
        {
            Ready ready = await WaitReadyAsync(node);

            Assert.Equal(
                3UL,
                Assert.Single(
                    ready.CommittedEntries).Index);
            Assert.Null(ready.HardState);
            Assert.Empty(ready.Entries);
            await node.AdvanceAsync();
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task OnlyOneReadyWaitOrBatchIsAllowed()
    {
        (RaftNode node, _) = RestartNode();
        try
        {
            using var cancellation =
                new CancellationTokenSource();
            Task<Ready> first = node.WaitForReadyAsync(
                    cancellation.Token)
                .AsTask();

            InvalidOperationException secondWait =
                await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await node.WaitForReadyAsync()
                    .AsTask()
                    .WaitAsync(DeadlockGuardTimeout));
            Assert.Equal(
                "Only one Ready wait or outstanding batch is allowed.",
                secondWait.Message);

            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await first);

            await node.CampaignAsync();
            Ready ready = await WaitReadyAsync(node);
            InvalidOperationException outstandingWait =
                await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await node.WaitForReadyAsync()
                    .AsTask()
                    .WaitAsync(DeadlockGuardTimeout));
            Assert.Equal(
                "Only one Ready wait or outstanding batch is allowed.",
                outstandingWait.Message);
            await node.AdvanceAsync();
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task CanceledReadyWaitDoesNotAcceptBatch()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode();
        try
        {
            using var cancellation =
                new CancellationTokenSource();
            Task<Ready> canceled = node.WaitForReadyAsync(
                    cancellation.Token)
                .AsTask();
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await canceled);

            await node.CampaignAsync();
            Ready ready = await WaitReadyAsync(node);
            Assert.Equal(
                RaftRole.Candidate,
                ready.SoftState?.Role);
            await PersistAndAdvanceAsync(
                node,
                storage,
                ready);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task CancellationBeforeOwnerDispatchSkipsReadyRequest()
    {
        var trace = new BlockingTraceSink();
        (RaftNode node, _) = RestartNode(
            traceSink: trace);
        using var cancellation =
            new CancellationTokenSource();
        try
        {
            Task campaign =
                node.CampaignAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    DeadlockGuardTimeout));
            Task<Ready> canceled =
                node.WaitForReadyAsync(
                        cancellation.Token)
                    .AsTask();

            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await canceled);
            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    DeadlockGuardTimeout));
            await campaign.WaitAsync(
                DeadlockGuardTimeout);

            Ready ready = await WaitReadyAsync(node);
            Assert.NotNull(ready.SoftState);
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task ReadyClaimWinsOverCancellationAndStop()
    {
        var trace = new BlockingTraceSink(
            traceEvent =>
                traceEvent.Type
                == RaftTraceEventType.ReadyAccepted);
        (RaftNode node, _) = RestartNode(
            traceSink: trace);
        using var cancellation =
            new CancellationTokenSource();
        try
        {
            await node.CampaignAsync();
            Task<Ready> readyTask =
                node.WaitForReadyAsync(
                        cancellation.Token)
                    .AsTask();
            Assert.True(
                trace.Entered.Wait(
                    DeadlockGuardTimeout));

            cancellation.Cancel();
            Task stop = node.StopAsync().AsTask();
            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    DeadlockGuardTimeout));

            Ready ready = await readyTask.WaitAsync(
                DeadlockGuardTimeout);
            Assert.NotNull(ready.SoftState);
            await stop.WaitAsync(
                DeadlockGuardTimeout);
            await Assert.ThrowsAsync<RaftNodeStoppedException>(
                async () => await node.AdvanceAsync()
                    .AsTask());
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task AdvanceClaimWinsOverLaterCancellation()
    {
        var block = 0;
        var trace = new BlockingTraceSink(
            traceEvent =>
                Volatile.Read(ref block) != 0
                && traceEvent.Type
                    == RaftTraceEventType.MessageReceived);
        (RaftNode node, _) = RestartNode(
            traceSink: trace);
        using var cancellation =
            new CancellationTokenSource();
        try
        {
            await node.CampaignAsync();
            _ = await WaitReadyAsync(node);

            Volatile.Write(ref block, 1);
            Task advance = node.AdvanceAsync(
                    cancellation.Token)
                .AsTask();
            Assert.True(
                trace.Entered.Wait(
                    DeadlockGuardTimeout));
            cancellation.Cancel();
            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    DeadlockGuardTimeout));

            await advance.WaitAsync(
                DeadlockGuardTimeout);
            Assert.False(node.Completion.IsCompleted);
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task CanceledQueuedAdvanceNeverObservesMissingBatch()
    {
        var trace = new BlockingTraceSink();
        (RaftNode node, _) = RestartNode(
            traceSink: trace);
        using var cancellation =
            new CancellationTokenSource();
        try
        {
            Task campaign =
                node.CampaignAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    DeadlockGuardTimeout));
            Task advance = node.AdvanceAsync(
                    cancellation.Token)
                .AsTask();

            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await advance);
            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    DeadlockGuardTimeout));
            await campaign.WaitAsync(
                DeadlockGuardTimeout);
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task ReadyCallbackFailurePreservesTriggerException()
    {
        var trace = new BlockingTraceSink(
            traceEvent =>
                traceEvent.Type
                == RaftTraceEventType.ReadyAccepted,
            _ => throw new InvalidOperationException(
                "ready trace failed"));
        (RaftNode node, _) = RestartNode(
            traceSink: trace);
        try
        {
            await node.CampaignAsync();
            Task<Ready> ready =
                node.WaitForReadyAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    DeadlockGuardTimeout));
            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    DeadlockGuardTimeout));

            RaftTracingException trigger =
                await Assert.ThrowsAsync<RaftTracingException>(
                    async () => await ready);
            Assert.IsType<InvalidOperationException>(
                trigger.InnerException);
            await Assert.ThrowsAsync<RaftNodeFaultedException>(
                async () => await node.Completion);
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
    public async Task AdvanceCallbackFailurePreservesTriggerException()
    {
        var block = 0;
        var trace = new BlockingTraceSink(
            traceEvent =>
                Volatile.Read(ref block) != 0
                && traceEvent.Type
                    == RaftTraceEventType.MessageReceived,
            _ => throw new InvalidOperationException(
                "advance trace failed"));
        (RaftNode node, _) = RestartNode(
            traceSink: trace);
        try
        {
            await node.CampaignAsync();
            _ = await WaitReadyAsync(node);

            Volatile.Write(ref block, 1);
            Task advance = node.AdvanceAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    DeadlockGuardTimeout));
            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    DeadlockGuardTimeout));

            RaftTracingException trigger =
                await Assert.ThrowsAsync<RaftTracingException>(
                    async () => await advance);
            Assert.IsType<InvalidOperationException>(
                trigger.InnerException);
            await Assert.ThrowsAsync<RaftNodeFaultedException>(
                async () => await node.Completion);
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
    public async Task AdvanceLoggerFailurePreservesWrappedTrigger()
    {
        var logger = new RecordingLogger();
        (RaftNode node, _) = RestartNode(
            logger: logger);
        try
        {
            await node.CampaignAsync();
            _ = await WaitReadyAsync(node);
            logger.ThrowOnInformation = true;

            RaftLoggingException trigger =
                await Assert.ThrowsAsync<RaftLoggingException>(
                    async () => await node.AdvanceAsync()
                        .AsTask());
            InvalidOperationException loggerFailure =
                Assert.IsType<InvalidOperationException>(
                    trigger.InnerException);
            Assert.Contains(
                "Injected information",
                loggerFailure.Message,
                StringComparison.Ordinal);
            RaftNodeFaultedException completion =
                await Assert.ThrowsAsync<RaftNodeFaultedException>(
                async () => await node.Completion);
            Assert.Same(
                trigger,
                completion.InnerException);
            Assert.NotNull(node.TerminalStatus);
        }
        finally
        {
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
    public async Task MissingAndDuplicateAdvanceFail()
    {
        (RaftNode node, _) = RestartNode();
        try
        {
            InvalidOperationException missing =
                await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await node.AdvanceAsync()
                    .AsTask());
            Assert.Equal(
                "No Ready is awaiting advancement.",
                missing.Message);

            await node.CampaignAsync();
            _ = await WaitReadyAsync(node);
            await node.AdvanceAsync();

            InvalidOperationException duplicate =
                await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await node.AdvanceAsync()
                    .AsTask());
            Assert.Equal(
                "No Ready is awaiting advancement.",
                duplicate.Message);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task CommittedPaginationRemainsGapFree()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode(maxCommittedSizePerReady: 1);
        try
        {
            await BecomeSingletonLeaderAsync(
                node,
                storage);
            await node.ProposeAsync("a"u8.ToArray());
            await node.ProposeAsync("b"u8.ToArray());
            await node.ProposeAsync("c"u8.ToArray());

            Ready append = await WaitReadyAsync(node);
            Assert.Equal(3, append.Entries.Count);
            await PersistAndAdvanceAsync(
                node,
                storage,
                append);

            var indexes = new List<ulong>();
            for (var page = 0; page < 3; page++)
            {
                Ready ready = await WaitReadyAsync(node);
                Entry entry =
                    Assert.Single(
                        ready.CommittedEntries);
                indexes.Add(entry.Index);
                await PersistAndAdvanceAsync(
                    node,
                    storage,
                    ready);
            }

            Assert.Equal(
                [4UL, 5UL, 6UL],
                indexes);
        }
        finally
        {
            await node.StopAsync();
        }
    }
}
