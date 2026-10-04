using System.Runtime.ExceptionServices;

using DotnetRaft.Protocol;
using DotnetRaft.Storage.Sqlite;

using Google.Protobuf;

using ProtocolConfChange =
    DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Examples.KvCluster;

public sealed partial class RaftClusterHost
    : BackgroundService,
      IRaftMessageReceiver
{
    private const int TransportEnvelopeReserve =
        64 * 1024;
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
                options.MaxTransportMessageBytes
                - TransportEnvelopeReserve);
        var config = new RaftConfig
        {
            Id = options.NodeId,
            ElectionTick = 10,
            HeartbeatTick = 1,
            Storage = storage,
            PreVote = true,
            CheckQuorum = true,
            Applied = recovery.Applied,
            MaxSizePerMessage = (ulong)(
                options.MaxTransportMessageBytes
                - TransportEnvelopeReserve),
        };
        node = recovery.StartNew
            ? RaftNode.Start(
                config,
                peerIds.Select(
                    id => new Peer(id)))
            : RaftNode.Restart(config);
        lastSnapshotAttemptIndex =
            recovery.Applied;
        if (recovery.Applied >= 3)
        {
            startupReady.TrySetResult();
        }
    }

    public ulong NodeId => options.NodeId;

    public async ValueTask ReceiveAsync(
        Message message,
        CancellationToken cancellationToken)
    {
        await WaitUntilReadyAsync(
                cancellationToken)
            .ConfigureAwait(false);
        NetworkMessageValidator.Validate(
            message,
            options);
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

    public async Task<ProposalResponse> PutAsync(
        string key,
        string value,
        Guid? requestId,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(value);
        return await MutateAsync(
                new KvCommand(
                    requestId ?? Guid.NewGuid(),
                    KvCommandType.Set,
                    key,
                    value),
                cancellationToken)
            .ConfigureAwait(false);
    }

    public Task<ProposalResponse> PutAsync(
        string key,
        string value,
        CancellationToken cancellationToken)
    {
        return PutAsync(
            key,
            value,
            requestId: null,
            cancellationToken);
    }

    public async Task<ProposalResponse> DeleteAsync(
        string key,
        Guid? requestId,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        return await MutateAsync(
                new KvCommand(
                    requestId ?? Guid.NewGuid(),
                    KvCommandType.Delete,
                    key,
                    null),
                cancellationToken)
            .ConfigureAwait(false);
    }

    public async Task<ReadResponse> ReadAsync(
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        await WaitUntilReadyAsync(
                cancellationToken)
            .ConfigureAwait(false);
        Guid requestId = Guid.NewGuid();
        Task<ulong> barrier =
            pendingReads.Register(requestId);
        using CancellationTokenSource timeout =
            CreateRequestTimeout(cancellationToken);
        try
        {
            await node.ReadIndexAsync(
                    requestId.ToByteArray(),
                    timeout.Token)
                .ConfigureAwait(false);
            ulong index = await barrier.WaitAsync(
                    timeout.Token)
                .ConfigureAwait(false);
            KeyValueReadResult result =
                stateMachine.ReadAtLeast(
                    key,
                    index);
            return new ReadResponse(
                options.NodeId,
                result.Found,
                result.Value,
                index,
                result.PhysicalApplied,
                Linearizable: true);
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Read {requestId} did not complete within {options.RequestTimeout}.");
        }
        finally
        {
            pendingReads.Remove(requestId);
        }
    }

    public ReadResponse ReadLocal(string key)
    {
        EnsureReady();
        KeyValueReadResult result =
            stateMachine.ReadLocal(key);
        return new ReadResponse(
            NodeId: options.NodeId,
            Found: result.Found,
            Value: result.Value,
            RequiredIndex: result.PhysicalApplied,
            PhysicalApplied:
                result.PhysicalApplied,
            Linearizable: false);
    }

    public async Task<ClusterStatusResponse> GetStatusAsync(
        CancellationToken cancellationToken)
    {
        await WaitUntilReadyAsync(
                cancellationToken)
            .ConfigureAwait(false);
        Status status = await node.GetStatusAsync(
                cancellationToken)
            .ConfigureAwait(false);
        return new ClusterStatusResponse(
            options.NodeId,
            status.Basic.Role,
            status.Basic.Term,
            status.Basic.LeaderId,
            status.Basic.Commit,
            status.Basic.Applied,
            stateMachine.PhysicalApplied);
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
            Task first = await Task.WhenAny(
                    readyLoop,
                    tickLoop,
                    completion)
                .ConfigureAwait(false);
            await first.ConfigureAwait(false);
            if (!stoppingToken.IsCancellationRequested)
            {
                throw new InvalidOperationException(
                    "A Raft host loop stopped unexpectedly.");
            }
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
            try
            {
                await node.StopAsync()
                    .ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failure ??= exception;
            }

            try
            {
                await node.DisposeAsync()
                    .ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failure ??= exception;
            }
            finally
            {
                if (!stoppingToken
                    .IsCancellationRequested)
                {
                    applicationLifetime
                        .StopApplication();
                }
            }
        }

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

    private async Task RunTickLoopAsync(
        CancellationToken cancellationToken)
    {
        while (await tickSource.WaitForNextTickAsync(
                   cancellationToken)
               .ConfigureAwait(false))
        {
            node.Tick();
        }
    }

    private async Task RunReadyLoopAsync(
        CancellationToken cancellationToken)
    {
        while (true)
        {
            Ready ready =
                await node.WaitForReadyAsync(
                        cancellationToken)
                    .ConfigureAwait(false);
            await ProcessReadyAsync(
                    ready,
                    cancellationToken)
                .ConfigureAwait(false);
            await node.AdvanceAsync(
                    cancellationToken)
                .ConfigureAwait(false);
            MaybeCreateSnapshot();
            TryCompleteStartup();
        }
    }

    private async Task ProcessReadyAsync(
        Ready ready,
        CancellationToken cancellationToken)
    {
        storage.PersistReady(ready);

        foreach (Message message in ready.Messages)
        {
            RaftSendResult result =
                await transport.SendAsync(
                        message,
                        cancellationToken)
                    .ConfigureAwait(false);
            if (message.Type == MessageType.MsgSnap)
            {
                await node.ReportSnapshotAsync(
                        message.To,
                        result == RaftSendResult.Delivered
                            ? SnapshotStatus.Success
                            : SnapshotStatus.Failure,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            else if (result == RaftSendResult.Unavailable)
            {
                await node.ReportUnreachableAsync(
                        message.To,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        if (ready.Snapshot is not null)
        {
            stateMachine.Restore(
                ready.Snapshot.Metadata.Index,
                ready.Snapshot.Data,
                ready.Snapshot.Metadata
                    .ConfState);
            HardState hardState =
                storage.GetHardState()
                ?? throw new InvalidDataException(
                    "Persisted snapshot has no HardState.");
            if (hardState.Commit
                < ready.Snapshot.Metadata.Index)
            {
                hardState.Commit =
                    ready.Snapshot.Metadata.Index;
                storage.SetHardState(hardState);
            }

            storage.AcknowledgeApplicationSnapshot(
                ready.Snapshot.Metadata.Index);
            CompletePendingProposalsFromDurableState();
        }

        foreach (Entry entry in ready.CommittedEntries)
        {
            await ApplyEntryAsync(
                    entry,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        foreach (ReadState readState in ready.ReadStates)
        {
            CompleteRead(readState);
        }

        pendingReads.CompleteThrough(
            stateMachine.PhysicalApplied);
    }

    private async Task ApplyEntryAsync(
        Entry entry,
        CancellationToken cancellationToken)
    {
        if (fixedMembership.IsBootstrapIndex(
                entry.Index))
        {
            ProtocolConfChange bootstrap =
                fixedMembership
                    .ValidateBootstrapEntry(entry);
            _ = await node.ApplyConfChangeAsync(
                    bootstrap,
                    cancellationToken)
                .ConfigureAwait(false);
            stateMachine.ApplyConfiguration(
                entry.Index,
                fixedMembership
                    .ExpectedConfState(
                        entry.Index));
            return;
        }

        switch (entry.Type)
        {
            case EntryType.EntryNormal:
                if (entry.Data.IsEmpty)
                {
                    stateMachine.AdvanceNoOp(
                        entry.Index);
                    return;
                }

                KvApplyResult result =
                    stateMachine.ApplyCommand(
                        entry.Index,
                        entry.Data);
                pendingProposals.Complete(
                    result);
                return;
            case EntryType.EntryConfChange:
                stateMachine.AdvanceNoOp(
                    entry.Index);
                return;
            case EntryType.EntryConfChangeV2:
                stateMachine.AdvanceNoOp(
                    entry.Index);
                return;
            default:
                throw new InvalidDataException(
                    $"Unknown committed entry type {entry.Type}.");
        }
    }

    private void CompleteRead(ReadState readState)
    {
        if (readState.RequestContext.Length !=
            16)
        {
            LogUnknownReadContext(
                logger,
                readState.Index,
                readState.RequestContext.Length);
            return;
        }

        var requestId = new Guid(
            readState.RequestContext.Span);
        pendingReads.CompleteOrDefer(
            requestId,
            readState.Index,
            stateMachine.PhysicalApplied);
    }

    private async Task<ProposalResponse> MutateAsync(
        KvCommand command,
        CancellationToken cancellationToken)
    {
        await WaitUntilReadyAsync(
                cancellationToken)
            .ConfigureAwait(false);
        KvRequestResolution? resolved =
            stateMachine.ResolveRequest(command);
        if (resolved is not null)
        {
            if (resolved.Conflict)
            {
                throw new KvRequestConflictException(
                    command.RequestId);
            }

            return new ProposalResponse(
                options.NodeId,
                command.RequestId,
                command.Type,
                resolved.ResultIndex,
                stateMachine.PhysicalApplied,
                Duplicate: true);
        }

        using PendingProposalRegistry
            .PendingProposalRegistration registration =
            pendingProposals.Register(command);
        using CancellationTokenSource timeout =
            CreateRequestTimeout(cancellationToken);
        try
        {
            byte[] payload =
                DurableKvCommandCodec.Encode(
                    command);
            int maximumPayload =
                options.MaxTransportMessageBytes
                - TransportEnvelopeReserve;
            if (payload.Length > maximumPayload)
            {
                throw new KvPayloadTooLargeException(
                    payload.Length,
                    maximumPayload);
            }

            if (registration.IsOwner)
            {
                PendingProposalRegistry
                    .PendingProposalSubmission?
                    submission =
                        registration
                            .BeginSubmission();
                if (submission is not null)
                {
                    _ = SubmitProposalAsync(
                        payload,
                        submission);
                }
            }

            KvApplyResult result =
                await registration.Task.WaitAsync(
                        timeout.Token)
                    .ConfigureAwait(false);
            if (result.Conflict)
            {
                throw new KvRequestConflictException(
                    command.RequestId);
            }

            return new ProposalResponse(
                options.NodeId,
                command.RequestId,
                command.Type,
                result.ResultIndex,
                result.PhysicalApplied,
                result.Duplicate);
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Proposal {command.RequestId} did not apply within {options.RequestTimeout}.");
        }
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
        bool configurationMismatch =
            !current.Metadata.ConfState.Equals(
                configuration);
        ulong distance =
            applied - current.Metadata.Index;
        if (!configurationMismatch
            && distance
                < (ulong)options
                    .SnapshotThresholdEntries)
        {
            return;
        }

        if (lastSnapshotAttemptIndex
                > current.Metadata.Index
            && applied
                - lastSnapshotAttemptIndex
                < (ulong)options
                    .SnapshotThresholdEntries)
        {
            return;
        }

        ByteString snapshotData =
            stateMachine.CreateSnapshotData();
        lastSnapshotAttemptIndex = applied;
        if (snapshotData.Length
            > options.MaxTransportMessageBytes
              - TransportEnvelopeReserve)
        {
            return;
        }

        storage.CreateSnapshot(
            applied,
            configuration,
            snapshotData);
        ulong compacted =
            storage.GetFirstIndex() - 1;
        if (compacted < applied)
        {
            storage.Compact(applied);
        }
    }

    private async Task SubmitProposalAsync(
        byte[] payload,
        PendingProposalRegistry
            .PendingProposalSubmission submission)
    {
        try
        {
            await node.ProposeAsync(
                    payload,
                    submission.CancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (submission.CancellationToken
                .IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            submission.Fail(exception);
        }
    }

    private void
        CompletePendingProposalsFromDurableState()
    {
        pendingProposals.CompleteResolved(
            stateMachine.ResolveAppliedRequest);
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

    private CancellationTokenSource CreateRequestTimeout(
        CancellationToken cancellationToken)
    {
        CancellationTokenSource timeout =
            CancellationTokenSource
                .CreateLinkedTokenSource(
                    cancellationToken);
        timeout.CancelAfter(
            options.RequestTimeout);
        return timeout;
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
}
