using System.Diagnostics.CodeAnalysis;

using DotnetRaft.Protocol;
using DotnetRaft.Quorum;
using DotnetRaft.Read;

using Google.Protobuf;

namespace DotnetRaft.Core;

internal readonly record struct ReadIndexAdvanceResult(
    ReadIndexRequest[] Completed,
    bool HeartbeatRequired);

internal sealed class ReadIndexCoordinator
{
    private readonly Queue<Message> gatedRequests = [];
    private readonly List<ReadState> readStates = [];
    private ReadOnlyTracker readOnly;

    internal ReadIndexCoordinator(
        ReadOnlyOption option)
    {
        readOnly = new ReadOnlyTracker(option);
    }

    internal ReadOnlyOption Option =>
        readOnly.Option;

    internal int PendingCount =>
        readOnly.PendingCount;

    internal int GatedCount =>
        gatedRequests.Count;

    internal bool HasReadStates =>
        readStates.Count > 0;

    internal static void ValidateRequest(Message message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (message.Entries.Count != 1)
        {
            throw new RaftInvariantException(
                $"{MessageType.MsgReadIndex} must contain exactly one entry.");
        }
    }

    internal void Gate(Message message)
    {
        ArgumentNullException.ThrowIfNull(message);
        gatedRequests.Enqueue(message.Clone());
    }

    internal bool TryDequeueGated(
        [NotNullWhen(true)]
        out Message? message)
    {
        return gatedRequests.TryDequeue(
            out message);
    }

    internal ReadIndexAdvanceResult TrackSafeRead(
        ulong commitIndex,
        Message message,
        ulong localId,
        JointConfig voters)
    {
        ReadAcknowledgement local = readOnly.AddRequest(
            commitIndex,
            message);
        readOnly.ReceiveAcknowledgement(
            localId,
            local);
        ReadIndexRequest[] completed =
            readOnly.Advance(voters);
        return new ReadIndexAdvanceResult(
            completed,
            readOnly.PendingCount > 0);
    }

    internal ReadIndexRequest[] Acknowledge(
        ulong from,
        ReadAcknowledgement acknowledgement,
        JointConfig voters)
    {
        readOnly.ReceiveAcknowledgement(
            from,
            acknowledgement);
        return readOnly.Advance(voters);
    }

    internal ReadIndexAdvanceResult Reevaluate(
        JointConfig voters)
    {
        ReadIndexRequest[] completed =
            readOnly.Advance(voters);
        return new ReadIndexAdvanceResult(
            completed,
            readOnly.PendingCount > 0);
    }

    internal Message? Complete(
        Message request,
        ulong index,
        ulong localId)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.From == RaftMessageTargets.None
            || request.From == localId)
        {
            readStates.Add(new ReadState(
                index,
                request.Entries[0].Data));
            return null;
        }

        var response = new Message
        {
            To = request.From,
            Type = MessageType.MsgReadIndexResp,
            Index = index,
        };
        response.Entries.Add(
            request.Entries.Select(
                entry => entry.Clone()));
        return response;
    }

    internal bool TryCompleteResponse(
        Message message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (message.Entries.Count != 1)
        {
            return false;
        }

        readStates.Add(new ReadState(
            message.Index,
            message.Entries[0].Data));
        return true;
    }

    internal ByteString GetHeartbeatContext()
    {
        return readOnly.GetHeartbeatContext();
    }

    internal ReadAcknowledgement? ParseAcknowledgement(ByteString context, bool fromPeer)
    {
        return readOnly.ParseAcknowledgement(context, fromPeer);
    }

    internal void Reset()
    {
        readOnly =
            new ReadOnlyTracker(readOnly.Option);
    }

    internal void AddRequestForTesting(
        ulong commitIndex,
        Message request)
    {
        readOnly.AddRequest(
            commitIndex,
            request);
    }

    internal ReadState[] TakeReadStates()
    {
        ReadState[] taken = [.. readStates];
        readStates.Clear();
        return taken;
    }

    internal void ClearReadStates() => readStates.Clear();

    internal ReadState[] PeekReadStates()
    {
        return
        [
            .. readStates.Select(
                state => new ReadState(
                    state.Index,
                    state.RequestContext)),
        ];
    }
}
