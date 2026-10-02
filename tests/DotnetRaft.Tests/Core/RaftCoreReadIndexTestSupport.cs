using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Read;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

internal static class RaftCoreReadIndexTestSupport
{
    internal static RaftCore NewLeader(
        IEnumerable<ulong> voters,
        IEnumerable<ulong>? outgoingVoters = null,
        IEnumerable<ulong>? learners = null,
        IEnumerable<ulong>? learnersNext = null,
        bool commitCurrentTerm = true,
        ReadOnlyOption readOnlyOption = ReadOnlyOption.Safe,
        bool checkQuorum = false,
        IRaftLogger? logger = null)
    {
        RaftCore core = Create(
            voters: voters,
            outgoingVoters: outgoingVoters,
            learners: learners,
            learnersNext: learnersNext,
            checkQuorum: checkQuorum,
            readOnlyOption: readOnlyOption,
            logger: logger).Core;
        core.BecomeCandidate();
        core.BecomeLeader();

        Message selfAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(selfAck);
        core.TakeMessages();

        if (!commitCurrentTerm)
        {
            return core;
        }

        foreach (ulong voterId in core.Tracker.VoterNodes())
        {
            if (voterId == core.Id
                || HasCommittedCurrentTermEntry(core))
            {
                continue;
            }

            core.Step(AppResponse(
                core,
                voterId,
                core.Log.LastIndex));
            core.TakeMessages();
        }

        Assert.True(HasCommittedCurrentTermEntry(core));
        return core;
    }

    internal static Message ReadRequest(
        string context,
        ulong from = 1,
        ulong to = 1)
    {
        var request = new Message
        {
            From = from,
            To = to,
            Type = MessageType.MsgReadIndex,
        };
        request.Entries.Add(new Entry
        {
            Data = ByteString.CopyFromUtf8(context),
        });
        return request;
    }

    internal static Message AppResponse(
        RaftCore core,
        ulong from,
        ulong index)
    {
        return new Message
        {
            From = from,
            To = core.Id,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = index,
        };
    }

    internal static Message HeartbeatResponse(
        RaftCore core,
        ulong from,
        ByteString context,
        ulong? term = null)
    {
        return new Message
        {
            From = from,
            To = core.Id,
            Term = term ?? core.Term,
            Type = MessageType.MsgHeartbeatResp,
            Context = context,
        };
    }

    internal static ByteString LatestHeartbeatContext(
        IEnumerable<Message> messages)
    {
        return messages
            .Where(message =>
                message.Type == MessageType.MsgHeartbeat)
            .Last()
            .Context;
    }

    private static bool HasCommittedCurrentTermEntry(
        RaftCore core)
    {
        return core.Log.Committed > 0
            && core.Log.GetTerm(core.Log.Committed) ==
                core.Term;
    }
}
