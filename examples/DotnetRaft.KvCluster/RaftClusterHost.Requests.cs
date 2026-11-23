using Google.Protobuf;

namespace DotnetRaft.Examples.KvCluster;

public sealed partial class RaftClusterHost
{
    public async Task<ProposalResponse> PutAsync(
        string key, string value, Guid? requestId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(value);
        var command = new KvCommand(requestId ?? Guid.NewGuid(), KvCommandType.Set, key, value);
        return await MutateAsync(command, cancellationToken).ConfigureAwait(false);
    }

    public Task<ProposalResponse> PutAsync(string key, string value, CancellationToken cancellationToken) =>
        PutAsync(key, value, requestId: null, cancellationToken);

    public async Task<ProposalResponse> DeleteAsync(string key, Guid? requestId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var command = new KvCommand(requestId ?? Guid.NewGuid(), KvCommandType.Delete, key, null);
        return await MutateAsync(command, cancellationToken).ConfigureAwait(false);
    }

    public async Task<ReadResponse> ReadAsync(string key, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        await WaitUntilReadyAsync(cancellationToken).ConfigureAwait(false);
        Guid requestId = Guid.NewGuid();
        Task<ulong> barrier = pendingReads.Register(requestId);
        using CancellationTokenSource timeout = CreateRequestTimeout(cancellationToken);
        try
        {
            ulong index = await ConfirmReadIndexAsync(requestId, barrier, timeout.Token).ConfigureAwait(false);
            return ReadConfirmedValue(key, index);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"Read {requestId} did not complete within {options.RequestTimeout}.");
        }
        finally
        {
            pendingReads.Remove(requestId);
        }
    }

    private async Task<ulong> ConfirmReadIndexAsync(
        Guid requestId, Task<ulong> barrier, CancellationToken cancellationToken)
    {
        // The Ready consumer resolves the barrier only after local application
        // reaches the quorum-confirmed index. Do not wait inside that consumer.
        await node.ReadIndexAsync(requestId.ToByteArray(), cancellationToken).ConfigureAwait(false);
        return await barrier.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private ReadResponse ReadConfirmedValue(string key, ulong index)
    {
        KeyValueReadResult result = stateMachine.ReadAtLeast(key, index);
        return new ReadResponse(options.NodeId, result.Found, result.Value, index, result.PhysicalApplied, Linearizable: true);
    }

    public ReadResponse ReadLocal(string key)
    {
        EnsureReady();
        KeyValueReadResult result = stateMachine.ReadLocal(key);
        return new ReadResponse(options.NodeId, result.Found, result.Value,
            result.PhysicalApplied, result.PhysicalApplied, Linearizable: false);
    }

    public async Task<ClusterStatusResponse> GetStatusAsync(CancellationToken cancellationToken)
    {
        await WaitUntilReadyAsync(cancellationToken).ConfigureAwait(false);
        Status status = await node.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        return new ClusterStatusResponse(options.NodeId, status.Basic.Role, status.Basic.Term,
            status.Basic.LeaderId, status.Basic.Commit, status.Basic.Applied, stateMachine.PhysicalApplied);
    }

    private async Task<ProposalResponse> MutateAsync(KvCommand command, CancellationToken cancellationToken)
    {
        await WaitUntilReadyAsync(cancellationToken).ConfigureAwait(false);
        byte[] payload = DurableKvCommandCodec.Encode(command);
        ByteString fingerprint = DurableKvCommandCodec.Fingerprint(payload);
        KvApplyResult? existing = stateMachine.ResolveAppliedRequest(command.RequestId, fingerprint);
        if (existing is not null)
        {
            return CreateProposalResponse(command, existing);
        }

        PendingProposalRegistry.PendingProposalRegistration registration;
        try
        {
            registration = pendingProposals.Register(command.RequestId, fingerprint);
        }
        catch (KvRequestConflictException)
        {
            // A durable result wins over a conflict with another waiter.
            existing = stateMachine.ResolveAppliedRequest(command.RequestId, fingerprint);
            if (existing is null)
            {
                throw;
            }

            return CreateProposalResponse(command, existing);
        }

        return await WaitForApplicationAsync(command, fingerprint, payload, registration, cancellationToken).ConfigureAwait(false);
    }

    private async Task<ProposalResponse> WaitForApplicationAsync(
        KvCommand command,
        ByteString fingerprint,
        byte[] payload,
        PendingProposalRegistry.PendingProposalRegistration registration,
        CancellationToken cancellationToken)
    {
        using PendingProposalRegistry.PendingProposalRegistration pending = registration;
        using CancellationTokenSource timeout = CreateRequestTimeout(cancellationToken);
        try
        {
            SubmitIfOwner(command.RequestId, fingerprint, payload, pending);
            KvApplyResult result = await pending.Task.WaitAsync(timeout.Token).ConfigureAwait(false);
            return CreateProposalResponse(command, result);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"Proposal {command.RequestId} did not apply within {options.RequestTimeout}.");
        }
    }

    private void SubmitIfOwner(
        Guid requestId,
        ByteString fingerprint,
        byte[] payload,
        PendingProposalRegistry.PendingProposalRegistration pending)
    {
        if (pending.IsOwner && pending.BeginSubmission() is { } submission)
        {
            _ = SubmitProposalAsync(requestId, fingerprint, payload, submission);
        }
    }

    private async Task SubmitProposalAsync(
        Guid requestId,
        ByteString fingerprint,
        byte[] payload,
        PendingProposalRegistry.PendingProposalSubmission submission)
    {
        try
        {
            // A prior attempt can finish between lookup and registration.
            if (TryCompleteAppliedProposal(requestId, fingerprint))
            {
                return;
            }

            EnsurePayloadFits(payload);
            await node.ProposeAsync(payload, submission.CancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (submission.CancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            submission.Fail(exception);
        }
    }

    private bool TryCompleteAppliedProposal(Guid requestId, ByteString fingerprint)
    {
        KvApplyResult? applied = stateMachine.ResolveAppliedRequest(requestId, fingerprint);
        if (applied is null)
        {
            return false;
        }

        pendingProposals.Complete(applied);
        return true;
    }

    private void EnsurePayloadFits(byte[] payload)
    {
        if (payload.Length > MaximumPayloadBytes)
        {
            throw new KvPayloadTooLargeException(payload.Length, MaximumPayloadBytes);
        }
    }

    private ProposalResponse CreateProposalResponse(KvCommand command, KvApplyResult result)
    {
        if (result.Conflict)
        {
            throw new KvRequestConflictException(command.RequestId);
        }

        return new ProposalResponse(options.NodeId, command.RequestId, command.Type,
            result.ResultIndex, result.PhysicalApplied, result.Duplicate);
    }

    private void CompletePendingProposalsFromDurableState() =>
        pendingProposals.CompleteResolved(stateMachine.ResolveAppliedRequest);

    private CancellationTokenSource CreateRequestTimeout(CancellationToken cancellationToken)
    {
        CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(options.RequestTimeout);
        return timeout;
    }
}
