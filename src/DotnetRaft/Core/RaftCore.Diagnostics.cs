using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Tracker;

namespace DotnetRaft.Core;

internal sealed partial class RaftCore
{
    internal BasicStatus GetBasicStatus()
    {
        return new BasicStatus(
            Id,
            Term,
            Vote,
            Log.Committed,
            LeaderId,
            Role,
            Log.Applied,
            LeaderTransferee);
    }

    internal ConfigurationStatus GetConfigurationStatus()
    {
        ConfState state = Tracker.ToConfState();
        return new ConfigurationStatus(
            state.Voters,
            state.VotersOutgoing,
            state.Learners,
            state.LearnersNext,
            state.AutoLeave);
    }

    internal KeyValuePair<ulong, ProgressStatus>[]
        GetProgressStatuses()
    {
        return
        [
            .. Tracker.Progress
                .OrderBy(pair => pair.Key)
                .Select(pair =>
                    new KeyValuePair<ulong, ProgressStatus>(
                        pair.Key,
                        ToStatus(pair.Value))),
        ];
    }

    private static ProgressStatus ToStatus(
        Progress progress)
    {
        return new ProgressStatus(
            progress.Match,
            progress.Next,
            progress.LastSentCommit,
            progress.State switch
            {
                ProgressState.Probe =>
                    ReplicationState.Probe,
                ProgressState.Replicate =>
                    ReplicationState.Replicate,
                ProgressState.Snapshot =>
                    ReplicationState.Snapshot,
                _ => throw new RaftInvariantException(
                    $"Unknown progress state {progress.State}."),
            },
            progress.PendingSnapshot,
            progress.RecentActive,
            progress.AppendFlowPaused,
            progress.IsPaused,
            progress.IsLearner,
            progress.Inflights.Count,
            progress.Inflights.Bytes,
            progress.Inflights.Capacity,
            progress.Inflights.MaxBytes);
    }

    internal void TraceReadyAccepted()
    {
        Trace(RaftTraceEventType.ReadyAccepted);
    }

    internal bool TracingEnabled => traceSink is not null;

    internal void TraceStorageCompletion(StorageCompletion completion)
    {
        if (TracingEnabled)
        {
            TraceMessage(RaftTraceEventType.MessageReceived, completion.DescribeReceipt());
        }
    }

    internal void TraceSyntheticMessageSent(
        Message message)
    {
        ArgumentNullException.ThrowIfNull(message);
        TraceMessage(
            RaftTraceEventType.MessageSent,
            message);
    }

    private void TraceConfigurationProposals(
        Entry[] entries,
        ParsedProposal proposal)
    {
        if (traceSink is null)
        {
            return;
        }

        for (int offset = 0; offset < entries.Length; offset++)
        {
            if (entries[offset].Type != EntryType.EntryNormal
                && proposal.DescribeConfigurationAt(offset) is { } detail)
            {
                Trace(
                    RaftTraceEventType.ConfigurationProposed,
                    detail: detail);
            }
        }
    }

    private void TraceEntriesAppended(
        Entry[] entries)
    {
        if (traceSink is null || entries.Length == 0)
        {
            return;
        }

        TraceEntriesAppended(
            entries.Length,
            entries[0].Index,
            entries[^1].Index);
    }

    private void TraceEntriesAppended(
        int count,
        ulong firstIndex,
        ulong lastIndex)
    {
        if (traceSink is null || count == 0)
        {
            return;
        }

        string detail = string.Create(
            System.Globalization.CultureInfo.InvariantCulture,
            $"count:{count} first:{firstIndex} last:{lastIndex}");
        Trace(
            RaftTraceEventType.EntriesAppended,
            detail: detail);
    }

    private void TraceCommitAdvanced(
        ulong previousCommit)
    {
        if (traceSink is null
            || Log.Committed <= previousCommit)
        {
            return;
        }

        Trace(RaftTraceEventType.CommitAdvanced);
    }

    private void TraceConfigurationApplied(
        ConfState state)
    {
        if (traceSink is null)
        {
            return;
        }

        Trace(
            RaftTraceEventType.ConfigurationApplied,
            detail:
                RaftDescriptions.DescribeConfState(state));
    }

    private void TraceMessage(
        RaftTraceEventType type,
        Message message)
    {
        if (traceSink is null)
        {
            return;
        }

        ulong snapshotIndex =
            message.Snapshot?.Metadata?.Index ?? 0;
        Trace(
            type,
            new RaftTraceMessage(
                message.Type,
                message.From,
                message.To,
                message.Term,
                message.LogTerm,
                message.Index,
                message.Commit,
                message.Vote,
                message.Reject,
                message.RejectHint,
                message.Entries.Count,
                snapshotIndex));
    }

    private void Trace(
        RaftTraceEventType type,
        RaftTraceMessage? message = null,
        string? detail = null)
    {
        if (traceSink is null)
        {
            return;
        }

        var traceEvent = new RaftTraceEvent(
            type,
            GetBasicStatus(),
            Log.LastIndex,
            GetConfigurationStatus(),
            message,
            detail);
        try
        {
            traceSink.Trace(traceEvent);
        }
        catch (Exception exception)
        {
            throw new RaftTracingException(
                $"Raft trace sink failed while emitting {type}.",
                exception);
        }
    }

    private void LogInformation(string message)
    {
        RaftLogging.Write(
            Logger,
            RaftLogLevel.Information,
            message);
    }

    private void LogDebug(string message)
    {
        RaftLogging.Write(
            Logger,
            RaftLogLevel.Debug,
            message);
    }

    private void LogError(string message)
    {
        RaftLogging.Write(
            Logger,
            RaftLogLevel.Error,
            message);
    }

    private void LogWarning(string message)
    {
        RaftLogging.Write(
            Logger,
            RaftLogLevel.Warning,
            message);
    }
}
