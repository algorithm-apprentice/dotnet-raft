using System.Runtime.ExceptionServices;

using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using ProtocolConfChange =
    DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Examples.KvCluster;

public sealed partial class RaftClusterHost
    : BackgroundService,
      IRaftMessageReceiver
{
    private readonly IHostApplicationLifetime applicationLifetime;
    private readonly FixedMembershipConfiguration
        fixedMembership;
    private readonly ILogger<RaftClusterHost> logger;
    private readonly RaftNode node;
    private readonly ValidatedClusterOptions options;
    private readonly PendingProposalRegistry pendingProposals;
    private readonly PendingReadRegistry pendingReads;
    private readonly ulong[] peerIds;
    private readonly KeyValueStateMachine stateMachine;
    private readonly MemoryStorage storage;
    private readonly IRaftTickSource tickSource;
    private readonly IRaftMessageTransport transport;

    public RaftClusterHost(
        ValidatedClusterOptions options,
        MemoryStorage storage,
        KeyValueStateMachine stateMachine,
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

        var config = new RaftConfig
        {
            Id = options.NodeId,
            ElectionTick = 10,
            HeartbeatTick = 1,
            Storage = storage,
            PreVote = true,
            CheckQuorum = true,
        };
        node = RaftNode.Start(
            config,
            peerIds.Select(id => new Peer(id)));
    }

    public ulong NodeId => options.NodeId;

    public async ValueTask ReceiveAsync(
        Message message,
        CancellationToken cancellationToken)
    {
        NetworkMessageValidator.Validate(
            message,
            options);
        await node.StepAsync(
                message,
                cancellationToken)
            .ConfigureAwait(false);
    }

    public ValueTask CampaignAsync(
        CancellationToken cancellationToken)
    {
        return node.CampaignAsync(
            cancellationToken);
    }

    public async Task<ProposalResponse> PutAsync(
        string key,
        string value,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(value);

        Guid requestId = Guid.NewGuid();
        Task<ulong> applied =
            pendingProposals.Register(requestId);
        using CancellationTokenSource timeout =
            CreateRequestTimeout(cancellationToken);
        try
        {
            await node.ProposeAsync(
                    KvCommandCodec.Encode(
                        new KvSetCommand(
                            requestId,
                            key,
                            value)),
                    timeout.Token)
                .ConfigureAwait(false);
            ulong index = await applied.WaitAsync(
                    timeout.Token)
                .ConfigureAwait(false);
            return new ProposalResponse(
                options.NodeId,
                requestId,
                index,
                stateMachine.PhysicalApplied);
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Proposal {requestId} did not apply within {options.RequestTimeout}.");
        }
        finally
        {
            pendingProposals.Remove(requestId);
        }
    }

    public async Task<ReadResponse> ReadAsync(
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
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
            ExceptionDispatchInfo.Capture(
                    failure)
                .Throw();
        }
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
        }
    }

    private async Task ProcessReadyAsync(
        Ready ready,
        CancellationToken cancellationToken)
    {
        if (ready.Snapshot is not null)
        {
            storage.ApplySnapshot(ready.Snapshot);
        }

        storage.Append(ready.Entries);
        if (ready.HardState is not null)
        {
            storage.SetHardState(ready.HardState);
        }

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
                ready.Snapshot.Data);
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
            await node.ApplyConfChangeAsync(
                    bootstrap,
                    cancellationToken)
                .ConfigureAwait(false);
            stateMachine.AdvanceNoOp(
                entry.Index);
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

                KvSetCommand command =
                    stateMachine.ApplySet(
                        entry.Index,
                        entry.Data);
                pendingProposals.Complete(
                    command.RequestId,
                    entry.Index);
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

        stateMachine.EnsureApplied(
            readState.Index);
        var requestId = new Guid(
            readState.RequestContext.Span);
        pendingReads.Complete(
            requestId,
            readState.Index);
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
