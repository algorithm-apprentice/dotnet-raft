using DotnetRaft.Protocol;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Examples.KvCluster;

public sealed partial class RaftClusterHost
{
    private async Task RunTickLoopAsync(CancellationToken cancellationToken)
    {
        while (await tickSource.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            node.Tick();
        }
    }

    private async Task RunReadyLoopAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            Ready ready = await node.WaitForReadyAsync(cancellationToken).ConfigureAwait(false);
            await ProcessReadyAsync(ready, cancellationToken).ConfigureAwait(false);
            await node.AdvanceAsync(cancellationToken).ConfigureAwait(false);
            MaybeCreateSnapshot();
            TryCompleteStartup();
        }
    }

    private async Task ProcessReadyAsync(Ready ready, CancellationToken cancellationToken)
    {
        // Keep the host contract visible: save before sending, restore before
        // applying, and finish physical application before acknowledging Ready.
        storage.PersistReady(ready);
        await SendMessagesAsync(ready.Messages, cancellationToken).ConfigureAwait(false);
        RestoreReceivedSnapshot(ready.Snapshot);
        await ApplyCommittedEntriesAsync(ready.CommittedEntries, cancellationToken).ConfigureAwait(false);
        CompleteConfirmedReads(ready.ReadStates);
    }

    private async Task SendMessagesAsync(IEnumerable<Message> messages, CancellationToken cancellationToken)
    {
        foreach (Message message in messages)
        {
            RaftSendResult result = await transport.SendAsync(message, cancellationToken).ConfigureAwait(false);
            await ReportDeliveryAsync(message, result, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ReportDeliveryAsync(
        Message message,
        RaftSendResult result,
        CancellationToken cancellationToken)
    {
        if (message.Type == MessageType.MsgSnap)
        {
            SnapshotStatus status = result == RaftSendResult.Delivered
                ? SnapshotStatus.Success
                : SnapshotStatus.Failure;
            await node.ReportSnapshotAsync(message.To, status, cancellationToken).ConfigureAwait(false);
        }
        else if (result == RaftSendResult.Unavailable)
        {
            await node.ReportUnreachableAsync(message.To, cancellationToken).ConfigureAwait(false);
        }
    }

    private void RestoreReceivedSnapshot(Snapshot? snapshot)
    {
        if (snapshot is null)
        {
            return;
        }

        DurableHostRecovery.CompletePendingSnapshot(storage, stateMachine, snapshot, recovering: false);
        LogIncomingSnapshotRestored(logger, snapshot.Metadata.Index);
        CompletePendingProposalsFromDurableState();
    }

    private async Task ApplyCommittedEntriesAsync(IReadOnlyList<Entry> entries, CancellationToken cancellationToken)
    {
        int next = 0;
        while (next < entries.Count)
        {
            if (!IsOrdinaryApplicationEntry(entries[next]))
            {
                await ApplyConfigurationEntryAsync(entries[next], cancellationToken).ConfigureAwait(false);
                next++;
                continue;
            }

            int start = next;
            next = FindApplicationBatchEnd(entries, start);
            ApplyApplicationBatch(entries, start, next);
        }
    }

    private int FindApplicationBatchEnd(IReadOnlyList<Entry> entries, int start)
    {
        int end = start + 1;
        while (end < entries.Count && end - start < MaxApplicationBatchEntries
            && IsOrdinaryApplicationEntry(entries[end]))
        {
            end++;
        }

        return end;
    }

    private void ApplyApplicationBatch(IReadOnlyList<Entry> entries, int start, int end)
    {
        // Results become visible only after the complete application transaction
        // commits. A batch never crosses a configuration entry.
        foreach (KvApplyResult result in stateMachine.ApplyNormalEntries(entries, start, end))
        {
            pendingProposals.Complete(result);
        }
    }

    private bool IsOrdinaryApplicationEntry(Entry entry) =>
        entry.Type == EntryType.EntryNormal && !fixedMembership.IsBootstrapIndex(entry.Index);

    private async Task ApplyConfigurationEntryAsync(Entry entry, CancellationToken cancellationToken)
    {
        if (fixedMembership.IsBootstrapIndex(entry.Index))
        {
            await ApplyBootstrapEntryAsync(entry, cancellationToken).ConfigureAwait(false);
            return;
        }

        switch (entry.Type)
        {
            case EntryType.EntryConfChange:
            case EntryType.EntryConfChangeV2:
                stateMachine.AdvanceNoOp(entry.Index);
                return;
            default:
                throw new InvalidDataException($"Unknown committed entry type {entry.Type}.");
        }
    }

    private async Task ApplyBootstrapEntryAsync(Entry entry, CancellationToken cancellationToken)
    {
        ProtocolConfChange bootstrap = fixedMembership.ValidateBootstrapEntry(entry);
        _ = await node.ApplyConfChangeAsync(bootstrap, cancellationToken).ConfigureAwait(false);
        stateMachine.ApplyConfiguration(entry.Index, fixedMembership.ExpectedConfState(entry.Index));
    }

    private void CompleteConfirmedReads(IEnumerable<ReadState> readStates)
    {
        foreach (ReadState readState in readStates)
        {
            CompleteRead(readState);
        }

        pendingReads.CompleteThrough(stateMachine.PhysicalApplied);
    }

    private void CompleteRead(ReadState readState)
    {
        if (readState.RequestContext.Length != 16)
        {
            LogUnknownReadContext(logger, readState.Index, readState.RequestContext.Length);
            return;
        }

        var requestId = new Guid(readState.RequestContext.Span);
        pendingReads.CompleteOrDefer(requestId, readState.Index, stateMachine.PhysicalApplied);
    }
}
