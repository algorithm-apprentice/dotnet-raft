using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

namespace DotnetRaft.Tests.Core;

public enum ElectionTestRole
{
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

internal sealed record ElectionCoreFixture(
    RaftCore Core,
    CoreTestStorage Storage);

internal static class RaftCoreElectionTestSupport
{
    internal static ElectionCoreFixture Create(
        ulong id = 1,
        IEnumerable<ulong>? voters = null,
        IEnumerable<ulong>? outgoingVoters = null,
        IEnumerable<ulong>? learners = null,
        IEnumerable<Entry>? entries = null,
        ulong term = 0,
        ulong vote = 0,
        ulong commit = 0,
        ulong applied = 0,
        int electionTick = 10,
        int heartbeatTick = 1,
        ulong maxCommittedSizePerReady = 0,
        ulong maxSizePerMessage = ulong.MaxValue,
        ulong maxUncommittedEntriesSize = 0,
        int maxInflightMessages = 256,
        ulong maxInflightBytes = 0,
        bool checkQuorum = false,
        bool disableProposalForwarding = false,
        Func<int, int>? randomOffset = null)
    {
        var storage = new CoreTestStorage();
        if (entries is not null)
        {
            storage.LogStorage.Append(entries);
        }

        var confState = new ConfState();
        confState.Voters.Add(voters ?? [1UL, 2UL, 3UL]);
        if (outgoingVoters is not null)
        {
            confState.VotersOutgoing.Add(outgoingVoters);
        }

        if (learners is not null)
        {
            confState.Learners.Add(learners);
        }

        storage.InitialState = new StorageState(
            new HardState
            {
                Term = term,
                Vote = vote,
                Commit = commit,
            },
            confState);

        var core = new RaftCore(
            new RaftConfig
            {
                Id = id,
                Storage = storage,
                Applied = applied,
                ElectionTick = electionTick,
                HeartbeatTick = heartbeatTick,
                MaxCommittedSizePerReady =
                    maxCommittedSizePerReady,
                MaxSizePerMessage = maxSizePerMessage,
                MaxUncommittedEntriesSize =
                    maxUncommittedEntriesSize,
                MaxInflightMessages = maxInflightMessages,
                MaxInflightBytes = maxInflightBytes,
                CheckQuorum = checkQuorum,
                DisableProposalForwarding =
                    disableProposalForwarding,
            },
            randomOffset ?? (_ => 0));

        return new ElectionCoreFixture(core, storage);
    }

    internal static Entry EntryAt(
        ulong index,
        ulong term,
        EntryType type = EntryType.EntryNormal)
    {
        return new Entry
        {
            Index = index,
            Term = term,
            Type = type,
        };
    }

    internal static Message Hup(ulong id)
    {
        return new Message
        {
            From = id,
            To = id,
            Type = MessageType.MsgHup,
        };
    }

    internal static Message VoteResponse(
        RaftCore core,
        ulong from,
        bool granted,
        ulong? term = null)
    {
        return new Message
        {
            From = from,
            To = core.Id,
            Term = term ?? core.Term,
            Type = MessageType.MsgVoteResp,
            Reject = !granted,
        };
    }

    internal static Message LeaderMessage(
        RaftCore core,
        MessageType type,
        ulong term)
    {
        ulong lastIndex = core.Log.LastIndex;
        return new Message
        {
            From = 2,
            To = core.Id,
            Term = term,
            Type = type,
            Index = lastIndex,
            LogTerm = core.Log.GetTerm(lastIndex),
            Commit = core.Log.Committed,
            Snapshot = new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = lastIndex,
                    Term = core.Log.GetTerm(lastIndex),
                    ConfState = new ConfState
                    {
                        Voters = { 2, 3 },
                    },
                },
            },
        };
    }

    internal static void EnterRole(
        RaftCore core,
        ElectionTestRole role)
    {
        switch (role)
        {
            case ElectionTestRole.Follower:
                return;
            case ElectionTestRole.PreCandidate:
                core.BecomePreCandidate();
                return;
            case ElectionTestRole.Candidate:
                core.BecomeCandidate();
                return;
            case ElectionTestRole.Leader:
                core.BecomeCandidate();
                core.BecomeLeader();
                return;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(role),
                    role,
                    null);
        }
    }

    internal static Message[] StepAcceptedSelfMessages(
        RaftCore core)
    {
        Message[] accepted = core.TakeMessagesAfterAppend();
        var remote = new List<Message>();

        foreach (Message message in accepted)
        {
            if (message.To == core.Id)
            {
                core.Step(message);
            }
            else
            {
                remote.Add(message);
            }
        }

        return [.. remote];
    }
}
