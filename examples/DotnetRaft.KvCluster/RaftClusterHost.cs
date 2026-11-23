using System.Runtime.ExceptionServices;

using DotnetRaft.Protocol;
using DotnetRaft.Storage.Sqlite;

using Google.Protobuf;

namespace DotnetRaft.Examples.KvCluster;

public sealed partial class RaftClusterHost
    : BackgroundService,
      IRaftMessageReceiver
{
    private const int TransportEnvelopeReserve =
        64 * 1024;
    private const int MaxApplicationBatchEntries = 64;
    private readonly IHostApplicationLifetime applicationLifetime;
    private readonly FixedMembershipConfiguration
        fixedMembership;
    private readonly ILogger<RaftClusterHost> logger;
    private readonly RaftNode node;
    private readonly ValidatedClusterOptions options;
    private readonly PendingProposalRegistry pendingProposals;
    private readonly PendingReadRegistry pendingReads;
    private readonly ulong[] peerIds;
    private readonly SqliteKeyValueStateMachine
        stateMachine;
    private readonly SqliteStorage storage;
    private ulong lastSnapshotAttemptIndex;
    private readonly TaskCompletionSource startupReady =
        new(
            TaskCreationOptions
                .RunContinuationsAsynchronously);
    private readonly IRaftTickSource tickSource;
    private readonly IRaftMessageTransport transport;

    public RaftClusterHost(
        ValidatedClusterOptions options,
        SqliteStorage storage,
        SqliteKeyValueStateMachine stateMachine,
        PendingProposalRegistry pendingProposals,
        PendingReadRegistry pendingReads,
        IRaftTickSource tickSource,
        IRaftMessageTransport transport,
        IHostApplicationLifetime applicationLifetime,
        ILogger<RaftClusterHost> logger)
    {
        this.options = options;
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.pendingProposals = pendingProposals;
        this.pendingReads = pendingReads;
        this.tickSource = tickSource;
        this.transport = transport;
        this.applicationLifetime =
            applicationLifetime;
        this.logger = logger;
        peerIds = [.. options.Peers.Keys.Order()];
        fixedMembership =
            new FixedMembershipConfiguration(
                peerIds);

        DurableRecoveryState recovery =
            DurableHostRecovery.Reconcile(
                storage,
                stateMachine,
                fixedMembership,
                MaximumPayloadBytes);
        node = CreateNode(recovery);
        lastSnapshotAttemptIndex =
            recovery.Applied;
        if (recovery.Applied >= 3)
        {
            startupReady.TrySetResult();
        }
    }

    public ulong NodeId => options.NodeId;

    private int MaximumPayloadBytes =>
        options.MaxTransportMessageBytes - TransportEnvelopeReserve;

    private RaftNode CreateNode(DurableRecoveryState recovery)
    {
        var config = new RaftConfig
        {
            Id = options.NodeId,
            ElectionTick = 10,
            HeartbeatTick = 1,
            Storage = storage,
            PreVote = true,
            CheckQuorum = true,
            Applied = recovery.Applied,
            MaxSizePerMessage = (ulong)MaximumPayloadBytes,
        };
        return recovery.StartNew
            ? RaftNode.Start(config, peerIds.Select(id => new Peer(id)))
            : RaftNode.Restart(config);
    }

    public async ValueTask ReceiveAsync(
        Message message,
        CancellationToken cancellationToken)
    {
        NetworkMessageValidator.Validate(message, options);
        await WaitUntilReadyAsync(
                cancellationToken)
            .ConfigureAwait(false);
        await node.StepAsync(
                message,
                cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask CampaignAsync(
        CancellationToken cancellationToken)
    {
        await WaitUntilReadyAsync(
                cancellationToken)
            .ConfigureAwait(false);
        await node.CampaignAsync(
                cancellationToken)
            .ConfigureAwait(false);
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        using var linked =
            CancellationTokenSource
                .CreateLinkedTokenSource(
                    stoppingToken);
        Task readyLoop = RunReadyLoopAsync(
            linked.Token);
        Task tickLoop = options.AutomaticTicks
            ? RunTickLoopAsync(linked.Token)
            : Task.Delay(
                Timeout.InfiniteTimeSpan,
                linked.Token);
        Task completion = node.Completion;
        Exception? failure = null;

        try
        {
            await WatchHostLoopsAsync(readyLoop, tickLoop, completion, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            failure = exception;
        }
        finally
        {
            linked.Cancel();
            Exception? loopFailure =
                await ObserveLoopFailuresAsync(
                        readyLoop,
                        tickLoop)
                    .ConfigureAwait(false);
            failure ??= loopFailure;
            Exception? nodeFailure = await StopAndDisposeNodeAsync(stoppingToken).ConfigureAwait(false);
            failure ??= nodeFailure;
        }

        FinishHostLifetime(failure, stoppingToken);
    }

    private static async Task WatchHostLoopsAsync(
        Task readyLoop, Task tickLoop, Task completion, CancellationToken stoppingToken)
    {
        Task first = await Task.WhenAny(readyLoop, tickLoop, completion).ConfigureAwait(false);
        await first.ConfigureAwait(false);
        if (!stoppingToken.IsCancellationRequested)
        {
            throw new InvalidOperationException("A Raft host loop stopped unexpectedly.");
        }
    }

    private async Task<Exception?> StopAndDisposeNodeAsync(CancellationToken stoppingToken)
    {
        Exception? failure = null;
        try
        {
            await node.StopAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failure = exception;
        }

        try
        {
            await node.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failure ??= exception;
        }
        finally
        {
            if (!stoppingToken.IsCancellationRequested)
            {
                applicationLifetime.StopApplication();
            }
        }

        return failure;
    }

    private void FinishHostLifetime(Exception? failure, CancellationToken stoppingToken)
    {
        if (failure is not null)
        {
            startupReady.TrySetException(failure);
            ExceptionDispatchInfo.Capture(
                    failure)
                .Throw();
        }

        startupReady.TrySetCanceled(
            stoppingToken);
    }

    private void MaybeCreateSnapshot()
    {
        ulong applied =
            stateMachine.PhysicalApplied;
        if (applied == 0)
        {
            return;
        }

        Snapshot current =
            storage.GetSnapshot();
        if (current.Metadata.Index >= applied)
        {
            return;
        }

        ConfState configuration =
            stateMachine.ConfState;
        if (!IsSnapshotDue(current, applied, configuration))
        {
            return;
        }

        if (!CanRetrySnapshot(current, applied))
        {
            return;
        }

        ByteString snapshotData =
            stateMachine.CreateSnapshotData();
        lastSnapshotAttemptIndex = applied;
        if (snapshotData.Length > MaximumPayloadBytes)
        {
            return;
        }

        PublishSnapshot(applied, configuration, snapshotData);
    }

    private bool IsSnapshotDue(Snapshot current, ulong applied, ConfState configuration)
    {
        bool configurationMismatch = !current.Metadata.ConfState.Equals(configuration);
        ulong distance = applied - current.Metadata.Index;
        return configurationMismatch || distance >= (ulong)options.SnapshotThresholdEntries;
    }

    private bool CanRetrySnapshot(Snapshot current, ulong applied)
    {
        return !(lastSnapshotAttemptIndex > current.Metadata.Index
            && applied - lastSnapshotAttemptIndex < (ulong)options.SnapshotThresholdEntries);
    }

    private void PublishSnapshot(ulong applied, ConfState configuration, ByteString snapshotData)
    {
        if (DurableHostRecovery.PublishSnapshotAndCompact(storage, applied, configuration, snapshotData))
        {
            LogSnapshotCompacted(logger, applied);
        }
    }

    private void TryCompleteStartup()
    {
        if (startupReady.Task.IsCompleted
            || stateMachine.PhysicalApplied < 3)
        {
            return;
        }

        fixedMembership
            .ValidateRecoveredConfiguration(
                stateMachine.PhysicalApplied,
                stateMachine.ConfState);
        Snapshot snapshot =
            storage.GetSnapshot();
        if (snapshot.Metadata.Index
                < stateMachine.PhysicalApplied
            || !snapshot.Metadata.ConfState.Equals(
                stateMachine.ConfState))
        {
            return;
        }

        startupReady.TrySetResult();
    }

    private async Task WaitUntilReadyAsync(
        CancellationToken cancellationToken)
    {
        await startupReady.Task.WaitAsync(
                cancellationToken)
            .ConfigureAwait(false);
    }

    private void EnsureReady()
    {
        if (!startupReady.Task
                .IsCompletedSuccessfully)
        {
            throw new InvalidOperationException(
                "The durable Raft host is still recovering.");
        }
    }

    private static async Task<Exception?>
        ObserveLoopFailuresAsync(
        params Task[] tasks)
    {
        Exception? failure = null;
        foreach (Task task in tasks)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception exception)
            {
                failure ??= exception;
            }
        }

        return failure;
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Warning,
        Message = "Ignoring read state at index {Index} with context length {ContextLength}.")]
    private static partial void LogUnknownReadContext(
        ILogger logger,
        ulong index,
        int contextLength);

    [LoggerMessage(EventId = 2, Level = LogLevel.Information, Message = "Compacted Raft log through snapshot index {Index}.")]
    private static partial void LogSnapshotCompacted(ILogger logger, ulong index);

    [LoggerMessage(EventId = 3, Level = LogLevel.Information, Message = "Restored incoming Raft snapshot at index {Index}.")]
    private static partial void LogIncomingSnapshotRestored(ILogger logger, ulong index);
}
