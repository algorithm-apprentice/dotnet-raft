using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Tests.Core;

using static DotnetRaft.Tests.Node.RaftNodeTestSupport;

namespace DotnetRaft.Tests.Node;

public sealed class RaftNodeSchedulingTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StableLogReadKeepsQueuedStatusPending(bool asyncStorageWrites)
    {
        var storage = new CoreTestStorage
        {
            InitialState = new StorageState(
                new HardState { Term = 1, Commit = 1 },
                new ConfState { Voters = { 1, 2 } }),
        };
        storage.LogStorage.Append([new Entry { Index = 1, Term = 1 }]);
        var readEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseRead = new ManualResetEventSlim(false);
        await using var node = RaftNode.Restart(new RaftConfig
        {
            Id = 1,
            Storage = storage,
            Applied = 1,
            AsyncStorageWrites = asyncStorageWrites,
        });

        // The incoming append must match index 1 against the stable log.
        // Signal from inside GetTerm, not from a trace callback before it.
        storage.GetTermOverride = index =>
        {
            if (index == 1)
            {
                readEntered.TrySetResult();
                releaseRead.Wait();
            }

            return storage.LogStorage.GetTerm(index);
        };
        var append = new Message
        {
            Type = MessageType.MsgApp,
            From = 2,
            To = 1,
            Term = 1,
            Index = 1,
            LogTerm = 1,
            Commit = 2,
        };
        append.Entries.Add(new Entry { Index = 2, Term = 1 });

        try
        {
            Task step = node.StepAsync(append).AsTask();
            await readEntered.Task.WaitAsync(DeadlockGuardTimeout);

            Task<Status> queuedStatus = node.GetStatusAsync().AsTask();
            Assert.False(step.IsCompleted);
            Assert.False(queuedStatus.IsCompleted);

            releaseRead.Set();
            await step.WaitAsync(DeadlockGuardTimeout);
            Status status = await queuedStatus.WaitAsync(DeadlockGuardTimeout);
            Assert.Equal(2UL, status.Basic.Commit);
        }
        finally
        {
            releaseRead.Set();
        }
    }

    [Fact]
    public async Task EligibleLanesReceiveTurnsBeforeBacklogsDrain()
    {
        const int CommandCount = 32;
        const int ProposalCount = 32;

        var block = 0;
        using var candidateObserved =
            new ManualResetEventSlim(false);
        var trace = new BlockingTraceSink(
            traceEvent =>
                Volatile.Read(ref block) != 0
                && traceEvent.Type
                    == RaftTraceEventType.MessageReceived
                && traceEvent.Message?.Type
                    == MessageType.MsgAppResp,
            onTrace: traceEvent =>
            {
                if (traceEvent.Type
                    == RaftTraceEventType.BecameCandidate)
                {
                    candidateObserved.Set();
                }
            });
        (RaftNode node, MemoryStorage storage) =
            RestartNode(
                voters: [1, 2],
                electionTick: 2,
                traceSink: trace);
        using var proposalCancellation =
            new CancellationTokenSource();
        try
        {
            await node.StepAsync(
                new Message
                {
                    From = 2,
                    To = 1,
                    Type = MessageType.MsgHeartbeat,
                    Term = 1,
                });
            await PersistAndAdvanceAsync(
                node,
                storage,
                await WaitReadyAsync(node));
            trace.Events.Clear();

            Volatile.Write(ref block, 1);
            Task blocker = node.StepAsync(
                    AppResponse())
                .AsTask();
            Assert.True(
                trace.Entered.Wait(
                    DeadlockGuardTimeout));
            Task[] commands =
            [
                .. Enumerable.Range(0, CommandCount)
                    .Select(_ =>
                        node.StepAsync(
                                AppResponse())
                            .AsTask()),
            ];
            Task[] proposals =
            [
                .. Enumerable.Range(0, ProposalCount)
                    .Select(index =>
                        node.ProposeAsync(
                                BitConverter.GetBytes(index),
                                proposalCancellation.Token)
                            .AsTask()),
            ];
            for (var tick = 0; tick < 4; tick++)
            {
                node.Tick();
            }

            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    DeadlockGuardTimeout));
            Assert.True(
                candidateObserved.Wait(
                    DeadlockGuardTimeout));

            RaftTraceEvent[] events =
                trace.Events.ToArray();
            int candidateIndex = Array.FindIndex(
                events,
                traceEvent =>
                    traceEvent.Type
                    == RaftTraceEventType.BecameCandidate);
            Assert.True(candidateIndex >= 0);
            RaftTraceEvent[] beforeCandidate =
            [
                .. events.Take(candidateIndex),
            ];
            int commandsBeforeCandidate =
                beforeCandidate.Count(
                    traceEvent =>
                        traceEvent.Message?.Type
                        == MessageType.MsgAppResp);
            int proposalsBeforeCandidate =
                beforeCandidate.Count(
                    traceEvent =>
                        traceEvent.Message?.Type
                        == MessageType.MsgProp);
            Assert.InRange(
                commandsBeforeCandidate,
                1,
                CommandCount);
            Assert.InRange(
                proposalsBeforeCandidate,
                1,
                ProposalCount - 1);

            await blocker.WaitAsync(
                DeadlockGuardTimeout);
            await Task.WhenAll(commands)
                .WaitAsync(DeadlockGuardTimeout);
            int completedBeforeCancellation =
                proposals.Count(
                    proposal =>
                        proposal.IsCompletedSuccessfully);
            Assert.InRange(
                completedBeforeCancellation,
                1,
                ProposalCount - 1);

            proposalCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await Task.WhenAll(proposals));
            Assert.All(
                proposals,
                proposal => Assert.True(
                    proposal.IsCompleted));
        }
        finally
        {
            proposalCancellation.Cancel();
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task SchedulerDoesNotRepeatALaneWithinARound()
    {
        const int WorkCount = 16;

        var enabled = 0;
        var beatCount = 0;
        using var fourBeatsObserved =
            new ManualResetEventSlim(false);
        var trace = new BlockingTraceSink(
            traceEvent =>
                Volatile.Read(ref enabled) != 0
                && traceEvent.Type
                    == RaftTraceEventType.MessageReceived
                && traceEvent.Message?.Type
                    == MessageType.MsgUnreachable,
            onTrace: traceEvent =>
            {
                if (Volatile.Read(ref enabled) != 0
                    && traceEvent.Type
                        == RaftTraceEventType.MessageReceived
                    && traceEvent.Message?.Type
                        == MessageType.MsgBeat
                    && Interlocked.Increment(
                        ref beatCount) == 4)
                {
                    fourBeatsObserved.Set();
                }
            });
        (RaftNode node, MemoryStorage storage) =
            RestartNode(traceSink: trace);
        try
        {
            await BecomeSingletonLeaderAsync(
                node,
                storage);
            trace.Events.Clear();
            Volatile.Write(ref enabled, 1);

            Task blocker = node.ReportUnreachableAsync(2)
                .AsTask();
            Assert.True(
                trace.Entered.Wait(
                    DeadlockGuardTimeout));
            Task[] commands =
            [
                .. Enumerable.Range(0, WorkCount)
                    .Select(index =>
                        node.ReportUnreachableAsync(
                                (ulong)index + 3)
                            .AsTask()),
            ];
            Task[] proposals =
            [
                .. Enumerable.Range(0, WorkCount)
                    .Select(index =>
                        node.ProposeAsync(
                                BitConverter.GetBytes(index))
                            .AsTask()),
            ];
            for (var tick = 0; tick < 4; tick++)
            {
                node.Tick();
            }

            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    DeadlockGuardTimeout));
            Assert.True(
                fourBeatsObserved.Wait(
                    DeadlockGuardTimeout));
            await blocker.WaitAsync(
                DeadlockGuardTimeout);

            MessageType[] messageTypes =
            [
                .. trace.Events
                    .Where(
                        traceEvent =>
                            traceEvent.Type
                            == RaftTraceEventType.MessageReceived
                            && traceEvent.Message.HasValue)
                    .Select(
                        traceEvent =>
                            traceEvent.Message!.Value.Type),
            ];
            int[] beats =
            [
                .. messageTypes
                    .Select(
                        (type, index) =>
                            (type, index))
                    .Where(
                        item =>
                            item.type
                            == MessageType.MsgBeat)
                    .Take(4)
                    .Select(item => item.index),
            ];
            Assert.Equal(4, beats.Length);

            var sawMissingLane = false;
            var sawRepeatedAcrossBoundary = false;
            for (var gap = 0;
                 gap < beats.Length - 1;
                 gap++)
            {
                MessageType[] between =
                    messageTypes[
                        (beats[gap] + 1)..beats[gap + 1]];
                int commandCount = between.Count(
                    type =>
                        type
                        == MessageType.MsgUnreachable);
                int proposalCount = between.Count(
                    type =>
                        type
                        == MessageType.MsgProp);
                Assert.InRange(commandCount, 0, 2);
                Assert.InRange(proposalCount, 0, 2);
                sawMissingLane |=
                    commandCount == 0
                    || proposalCount == 0;
                sawRepeatedAcrossBoundary |=
                    commandCount == 2
                    || proposalCount == 2;
            }

            Assert.True(sawMissingLane);
            Assert.True(sawRepeatedAcrossBoundary);
            await Task.WhenAll(commands)
                .WaitAsync(DeadlockGuardTimeout);
            await Task.WhenAll(proposals)
                .WaitAsync(DeadlockGuardTimeout);
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task RequestContinuationsDoNotRunInlineOnOwner()
    {
        var hupCount = 0;
        using var secondCampaignObserved =
            new ManualResetEventSlim(false);
        var trace = new BlockingTraceSink(
            onTrace: traceEvent =>
            {
                if (traceEvent.Type
                        != RaftTraceEventType.MessageReceived
                    || traceEvent.Message?.Type
                        != MessageType.MsgHup)
                {
                    return;
                }

                if (Interlocked.Increment(
                        ref hupCount) == 2)
                {
                    secondCampaignObserved.Set();
                }
            });
        (RaftNode node, _) = RestartNode(
            traceSink: trace);
        using var continuationCancellation =
            new CancellationTokenSource();
        try
        {
            Task firstCampaign =
                node.CampaignAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    DeadlockGuardTimeout));
            Task<bool> continuation =
                firstCampaign.ContinueWith(
                    _ =>
                    {
                        try
                        {
                            node.CampaignAsync(
                                    continuationCancellation.Token)
                                .AsTask()
                                .GetAwaiter()
                                .GetResult();
                            return true;
                        }
                        catch (OperationCanceledException)
                        {
                            return false;
                        }
                    },
                    CancellationToken.None,
                    TaskContinuationOptions
                        .ExecuteSynchronously,
                    TaskScheduler.Default);

            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    DeadlockGuardTimeout));
            bool observed =
                secondCampaignObserved.Wait(
                    DeadlockGuardTimeout);
            if (!observed)
            {
                continuationCancellation.Cancel();
            }

            Assert.True(observed);
            Assert.True(
                await continuation.WaitAsync(
                    DeadlockGuardTimeout));
        }
        finally
        {
            continuationCancellation.Cancel();
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    private static Message AppResponse()
    {
        return new Message
        {
            From = 2,
            To = 1,
            Type = MessageType.MsgAppResp,
            Term = 1,
        };
    }
}
