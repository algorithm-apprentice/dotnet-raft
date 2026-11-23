using DotnetRaft.Protocol;
using DotnetRaft.Read;
using DotnetRaft.Storage;
using DotnetRaft.Tracker;

namespace DotnetRaft.Core;

internal sealed partial class RaftCore
{
    private void HandleFollowerProposal(
        Message message,
        ParsedProposal? proposal)
    {
        if (proposal is null)
        {
            ParsedProposal.RequireEntriesForSyntheticProposal(message);
        }

        if (LeaderId == RaftMessageTargets.None)
        {
            throw new ProposalDroppedException(
                "The follower has no known leader.");
        }

        if (proposals.Policy.DisableForwarding)
        {
            throw new ProposalDroppedException(
                "Proposal forwarding is disabled.");
        }

        Message forwarded = message.Clone();
        forwarded.To = LeaderId;
        Send(forwarded);
    }

    private void HandleLeaderProposal(
        Message message,
        ParsedProposal? proposal)
    {
        if (proposal is null)
        {
            ParsedProposal.RequireEntriesForSyntheticProposal(message);
        }

        if (!Tracker.Progress.ContainsKey(Id))
        {
            throw new ProposalDroppedException(
                "The leader has no local replication progress.");
        }

        if (LeaderTransferee !=
            RaftMessageTargets.None)
        {
            throw new ProposalDroppedException(
                "The leader is transferring leadership.");
        }

        proposal ??= ParsedProposal.Parse(message.Entries, fromPeer: false, Log.Format);
        (
            Entry[] entries,
            ulong pendingConfigurationIndex) =
            PrepareProposalEntries(
                proposal);
        ProposalAdmissionPlan plan =
            AppendLeaderEntries(
                entries,
                pendingConfigurationIndex,
                proposal);
        if (!plan.Accepted)
        {
            throw new ProposalDroppedException(
                "The proposal exceeds the uncommitted entry size limit.");
        }

        proposals.PublishPending(plan);
        BroadcastAppend();
    }

    private ProposalAdmissionPlan AppendLeaderEntries(
        Entry[] appended,
        ulong pendingConfigurationIndex,
        ParsedProposal? proposal = null)
    {
        if (appended.Length == 0)
        {
            throw new RaftInvariantException(
                "At least one leader entry is required.");
        }

        ulong lastIndex = Log.LastIndex;
        if (lastIndex == ulong.MaxValue)
        {
            throw new RaftInvariantException(
                "The log has no representable successor index.");
        }

        ulong availableIndexes =
            ulong.MaxValue - lastIndex - 1;
        if ((ulong)appended.Length > availableIndexes)
        {
            throw new RaftInvariantException(
                $"Appending {appended.Length} entries after index {lastIndex} would leave no representable successor.");
        }

        ProposalAdmissionPlan plan =
            proposals.Prepare(
                appended,
                pendingConfigurationIndex);
        if (!plan.Accepted)
        {
            return plan;
        }

        for (var offset = 0; offset < appended.Length; offset++)
        {
            Entry entry = appended[offset];
            entry.Term = Term;
            entry.Index =
                lastIndex + (ulong)offset + 1;
        }

        if (proposal is not null)
        {
            TraceConfigurationProposals(appended, proposal);
        }

        ulong newLastIndex = Log.Append(appended);
        proposals.CommitQuota(plan);
        TraceEntriesAppended(appended);
        // Queue the leader's own acknowledgement behind persistence, just
        // like a follower's. Appending to the unstable log is not evidence.
        Send(new Message
        {
            To = Id,
            Type = MessageType.MsgAppResp,
            Index = newLastIndex,
        });
        return plan;
    }

    private (
        Entry[] Entries,
        ulong PendingConfigurationIndex)
        PrepareProposalEntries(
            ParsedProposal proposal)
    {
        Entry[] owned = proposal.CopyEntries();
        ulong candidatePending =
            proposals.PendingConfigurationIndex;
        if (!proposal.HasConfigurations)
        {
            return (owned, candidatePending);
        }

        ulong lastIndex = Log.LastIndex;

        for (var offset = 0; offset < owned.Length; offset++)
        {
            if (proposal.WantsLeaveJointAt(offset) is not bool wantsLeaveJoint)
            {
                continue;
            }

            bool alreadyJoint =
                Tracker.Config.Voters.Outgoing.Count > 0;
            if (!ProposalAdmission.CanAcceptConfiguration(
                    candidatePending,
                    Log.Applied,
                    alreadyJoint,
                    wantsLeaveJoint)
                && !proposals.Policy.DisableConfigurationValidation)
            {
                owned[offset] = new Entry();
                continue;
            }

            candidatePending =
                ProposalAdmission
                    .PrepareConfigurationIndex(
                    lastIndex,
                    offset);
        }

        return (owned, candidatePending);
    }

    private void BroadcastAppend()
    {
        Tracker.Visit((id, progress) =>
        {
            if (id != Id)
            {
                MaybeSendAppend(
                    id,
                    progress,
                    sendIfEmpty: true);
            }
        });
    }

    private bool MaybeSendAppend(
        ulong to,
        Progress progress,
        bool sendIfEmpty)
    {
        if (progress.IsPaused)
        {
            return false;
        }

        ulong previousIndex = progress.Next - 1;
        ulong previousTerm = 0;
        IReadOnlyList<Entry> entries = [];
        try
        {
            previousTerm = Log.GetTerm(previousIndex);
            entries =
                progress.State == ProgressState.Replicate
                && progress.Inflights.IsFull
                    ? []
                    : Log.GetEntries(
                        progress.Next,
                        MaxMessageSize);
        }
        catch (StorageException exception)
            when (exception.Error == StorageError.Compacted)
        {
            return MaybeSendSnapshot(to, progress);
        }

        if (entries.Count == 0 && !sendIfEmpty)
        {
            return false;
        }

        ulong payloadSize = 0;
        try
        {
            payloadSize = EntrySizing.PayloadSize(entries);
        }
        catch (OverflowException exception)
        {
            throw new RaftInvariantException(
                $"Append payload size overflowed: {exception.Message}");
        }

        var message = new Message
        {
            To = to,
            Type = MessageType.MsgApp,
            Index = previousIndex,
            LogTerm = previousTerm,
            Commit = Log.Committed,
        };
        message.Entries.AddRange(entries);
        Send(message);
        progress.SentEntries(entries.Count, payloadSize);
        progress.RecordSentCommit(Log.Committed);
        return true;
    }

    private void HandleAppendEntries(Message message, LogSlice? append)
    {
        EntryId previous = append?.Previous ?? new EntryId(message.LogTerm, message.Index);
        if (previous.Index < Log.Committed)
        {
            Send(new Message
            {
                To = message.From,
                Type = MessageType.MsgAppResp,
                Index = Log.Committed,
            });
            return;
        }

        LogSlice slice = append ?? LogSlice.ParseInternal(message.Term, previous, message.Entries);
        ulong previousCommit = Log.Committed;
        if (Log.MaybeAppendEntries(
                slice,
                out ulong lastNewIndex,
                out ulong firstAppendedIndex,
                out int appendedCount))
        {
            if (appendedCount > 0)
            {
                TraceEntriesAppended(
                    appendedCount,
                    firstAppendedIndex,
                    lastNewIndex);
            }

            Log.CommitTo(
                Math.Min(
                    message.Commit,
                    lastNewIndex));
            TraceCommitAdvanced(previousCommit);
            Send(new Message
            {
                To = message.From,
                Type = MessageType.MsgAppResp,
                Index = lastNewIndex,
            });
            return;
        }

        ulong hintIndex = Math.Min(
            previous.Index,
            Log.LastIndex);
        EntryId hint = Log.FindConflictByTerm(
            hintIndex,
            previous.Term);
        Send(new Message
        {
            To = message.From,
            Type = MessageType.MsgAppResp,
            Index = previous.Index,
            Reject = true,
            RejectHint = hint.Index,
            LogTerm = hint.Term,
        });
    }

    private void HandleAppendResponse(
        Message message)
    {
        if (!Tracker.Progress.TryGetValue(
                message.From,
                out Progress? progress))
        {
            return;
        }

        if (message.Reject)
        {
            progress.RecentActive = true;
            if (message.Index == 0)
            {
                return;
            }

            ulong nextProbeIndex = message.RejectHint;
            if (message.LogTerm > 0)
            {
                nextProbeIndex = Log.FindConflictByTerm(
                    message.RejectHint,
                    message.LogTerm).Index;
            }

            if (progress.MaybeDecrementTo(
                    message.Index,
                    nextProbeIndex))
            {
                if (progress.State ==
                    ProgressState.Replicate)
                {
                    progress.BecomeProbe();
                }

                MaybeSendAppend(
                    message.From,
                    progress,
                    sendIfEmpty: true);
            }

            return;
        }

        if (message.Index > Log.LastIndex)
        {
            throw new RaftInvariantException(
                $"Append acknowledgement {message.Index} exceeds last index {Log.LastIndex}.");
        }

        progress.RecentActive = true;
        bool updated = progress.MaybeUpdate(
            message.Index);
        if (!updated
            && !(progress.State == ProgressState.Probe
                 && progress.Match == message.Index))
        {
            return;
        }

        switch (progress.State)
        {
            case ProgressState.Probe:
                progress.BecomeReplicate();
                break;
            case ProgressState.Replicate:
                progress.Inflights.FreeThrough(
                    message.Index);
                break;
            case ProgressState.Snapshot:
                if (progress.Match >= Log.FirstIndex - 1)
                {
                    progress.BecomeProbe();
                    progress.BecomeReplicate();
                }

                break;
            default:
                throw new RaftInvariantException(
                    $"Unknown progress state {progress.State}.");
        }

        if (MaybeCommit())
        {
            BroadcastAppend();
        }
        else if (message.From != Id
                 && progress.CanBumpCommit(
                     Log.Committed))
        {
            MaybeSendAppend(
                message.From,
                progress,
                sendIfEmpty: true);
        }

        if (message.From != Id)
        {
            while (MaybeSendAppend(
                       message.From,
                       progress,
                       sendIfEmpty: false))
            {
            }
        }

        if (message.From == LeaderTransferee
            && progress.Match == Log.LastIndex)
        {
            SendTimeoutNow(message.From);
        }
    }

    private bool MaybeCommit()
    {
        ulong previousCommit = Log.Committed;
        bool committed = Log.MaybeCommit(new EntryId(
            Term: Term,
            Index: Tracker.CommittedIndex));
        if (committed)
        {
            TraceCommitAdvanced(previousCommit);
            ReleasePendingReadIndexMessages();
        }

        return committed;
    }

    private void BroadcastHeartbeat()
    {
        RaftBytes context =
            reads.GetHeartbeatContext();
        Tracker.Visit((id, progress) =>
        {
            if (id == Id)
            {
                return;
            }

            Send(new Message
            {
                To = id,
                Type = MessageType.MsgHeartbeat,
                Commit = Math.Min(progress.Match, Log.Committed),
                Context = context,
            });
            progress.RecordSentCommit(
                Math.Min(progress.Match, Log.Committed));
        });
    }

    private void HandleHeartbeat(Message message)
    {
        ulong previousCommit = Log.Committed;
        Log.CommitTo(message.Commit);
        TraceCommitAdvanced(previousCommit);
        Send(new Message
        {
            To = message.From,
            Type = MessageType.MsgHeartbeatResp,
            Context = message.Context,
        });
    }

    private void HandleHeartbeatResponse(
        ulong from,
        ReadAcknowledgement readAcknowledgement)
    {
        if (!Tracker.Progress.TryGetValue(
                from,
                out Progress? progress))
        {
            return;
        }

        progress.RecentActive = true;
        progress.AppendFlowPaused = false;
        if (progress.Match < Log.LastIndex
            || progress.State == ProgressState.Probe)
        {
            MaybeSendAppend(
                from,
                progress,
                sendIfEmpty: true);
        }

        if (readAcknowledgement.IsPresent)
        {
            ReadIndexRequest[] completed =
                reads.Acknowledge(
                    from,
                    readAcknowledgement,
                    Tracker.Config.Voters);
            CompleteReadIndexRequests(completed);
        }
    }

    private void HandleUnreachable(
        Message message)
    {
        if (!Tracker.Progress.TryGetValue(
                message.From,
                out Progress? progress))
        {
            return;
        }

        if (progress.State == ProgressState.Replicate)
        {
            progress.BecomeProbe();
        }
    }

    private bool MaybeSendSnapshot(
        ulong to,
        Progress progress)
    {
        if (!progress.RecentActive)
        {
            return false;
        }

        Snapshot snapshot;
        try
        {
            snapshot = Log.GetSnapshot();
        }
        catch (StorageException exception)
            when (exception.Error ==
                  StorageError.SnapshotTemporarilyUnavailable)
        {
            return false;
        }

        ulong snapshotIndex = snapshot.Metadata.Index;
        if (snapshotIndex == 0)
        {
            throw new RaftInvariantException(
                "Cannot send an empty snapshot.");
        }

        progress.BecomeSnapshot(snapshotIndex);
        Send(new Message
        {
            To = to,
            Type = MessageType.MsgSnap,
            Snapshot = snapshot,
        });
        return true;
    }

    private void HandleSnapshotStatus(
        Message message)
    {
        if (!Tracker.Progress.TryGetValue(
                message.From,
                out Progress? progress)
            || progress.State != ProgressState.Snapshot)
        {
            return;
        }

        progress.ReportSnapshot(!message.Reject);
    }
}
