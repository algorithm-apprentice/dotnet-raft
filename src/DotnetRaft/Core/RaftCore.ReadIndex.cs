using DotnetRaft.Protocol;
using DotnetRaft.Read;
using DotnetRaft.Storage;

namespace DotnetRaft.Core;

internal sealed partial class RaftCore
{
    private void HandleFollowerReadIndex(
        Message message)
    {
        if (LeaderId == RaftMessageTargets.None)
        {
            return;
        }

        Message forwarded = message.Clone();
        forwarded.To = LeaderId;
        Send(forwarded);
    }

    private void HandleLeaderReadIndex(
        Message message)
    {
        ReadIndexCoordinator.ValidateRequest(message);
        // Even a sole voter can have a stale commit index at election time.
        if (!HasCommittedEntryInCurrentTerm())
        {
            reads.Gate(message);
            return;
        }

        if ((Tracker.IsSingleton || reads.Option == ReadOnlyOption.LeaseBased)
            && Tracker.IsVoter(Id))
        {
            RespondToReadIndex(
                message,
                Log.Committed);
        }
        else
        {
            TrackReadIndex(message);
        }
    }

    private void TrackReadIndex(Message message)
    {
        ReadIndexAdvanceResult result =
            reads.TrackSafeRead(
                Log.Committed,
                message,
                Id,
                Tracker.Config.Voters);
        CompleteReadIndexRequests(
            result.Completed);
        if (result.HeartbeatRequired)
        {
            BroadcastHeartbeat();
        }
    }

    private void ReleasePendingReadIndexMessages()
    {
        if (Role != RaftRole.Leader
            || !HasCommittedEntryInCurrentTerm())
        {
            return;
        }

        while (reads.TryDequeueGated(
                   out Message? request))
        {
            HandleLeaderReadIndex(request);
        }
    }

    private bool HasCommittedEntryInCurrentTerm()
    {
        try
        {
            return Log.GetTerm(Log.Committed) == Term;
        }
        catch (StorageException exception)
            when (exception.Error is
                  StorageError.Compacted or
                  StorageError.Unavailable)
        {
            return false;
        }
    }

    private void ReevaluateReadOnlyAfterConfigurationChange()
    {
        ReadIndexAdvanceResult result =
            reads.Reevaluate(
                Tracker.Config.Voters);
        CompleteReadIndexRequests(
            result.Completed);
        if (result.HeartbeatRequired)
        {
            BroadcastHeartbeat();
        }
    }

    private void CompleteReadIndexRequests(
        ReadIndexRequest[] requests)
    {
        foreach (ReadIndexRequest request in
                 requests)
        {
            RespondToReadIndex(
                request.Request,
                request.Index);
        }
    }

    private void RespondToReadIndex(
        Message request,
        ulong index)
    {
        Message? response = reads.Complete(
            request,
            index,
            Id);
        if (response is not null)
        {
            Send(response);
        }
    }

    private void HandleReadIndexResponse(Message message)
    {
        if (!reads.TryCompleteResponse(message))
        {
            LogError(
                $"{Id:x} ignored {MessageType.MsgReadIndexResp} " +
                $"with {message.Entries.Count} entries; expected exactly one entry.");
            return;
        }
    }

    internal ReadState[] TakeReadStates()
    {
        return reads.TakeReadStates();
    }

    internal ReadState[] PeekReadStates()
    {
        return reads.PeekReadStates();
    }
}
