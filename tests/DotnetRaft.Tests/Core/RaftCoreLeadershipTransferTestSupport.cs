using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Tracker;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

internal static class RaftCoreLeadershipTransferTestSupport
{
    internal static ElectionCoreFixture CreateLeader(
        ulong id = 1,
        IEnumerable<ulong>? voters = null,
        IEnumerable<ulong>? outgoingVoters = null,
        IEnumerable<ulong>? learners = null,
        IEnumerable<ulong>? learnersNext = null,
        IEnumerable<Entry>? entries = null,
        ulong term = 0,
        ulong commit = 0,
        ulong applied = 0,
        int electionTick = 10,
        int heartbeatTick = 1,
        bool checkQuorum = false,
        bool preVote = false,
        bool stepDownOnRemoval = false)
    {
        ElectionCoreFixture fixture = Create(
            id: id,
            voters: voters ?? [1UL, 2UL, 3UL],
            outgoingVoters: outgoingVoters,
            learners: learners,
            learnersNext: learnersNext,
            entries: entries,
            term: term,
            commit: commit,
            applied: applied,
            electionTick: electionTick,
            heartbeatTick: heartbeatTick,
            checkQuorum: checkQuorum,
            preVote: preVote,
            stepDownOnRemoval: stepDownOnRemoval);
        fixture.Core.BecomeCandidate();
        fixture.Core.BecomeLeader();
        fixture.Core.Step(Assert.Single(
            fixture.Core.TakeMessagesAfterAppend()));
        fixture.Core.TakeMessages();
        return fixture;
    }

    internal static ElectionCoreFixture CreateCompactedLeader(
        IEnumerable<ulong>? voters = null)
    {
        var storage = new CoreTestStorage();
        var state = new ConfState();
        state.Voters.Add(voters ?? [1UL, 2UL]);
        storage.LogStorage.ApplySnapshot(new Snapshot
        {
            Data = ByteString.CopyFromUtf8("snapshot"),
            Metadata = new SnapshotMetadata
            {
                Index = 5,
                Term = 1,
                ConfState = state.Clone(),
            },
        });
        storage.LogStorage.Append(
        [
            EntryAt(6, 1),
            EntryAt(7, 1),
        ]);
        storage.InitialState = new StorageState(
            new HardState
            {
                Term = 1,
                Commit = 5,
            },
            state);
        var core = new RaftCore(
            new RaftConfig
            {
                Id = 1,
                Storage = storage,
                Applied = 5,
            },
            _ => 0);
        core.BecomeCandidate();
        core.BecomeLeader();
        core.Step(Assert.Single(
            core.TakeMessagesAfterAppend()));
        core.TakeMessages();
        return new ElectionCoreFixture(core, storage);
    }

    internal static void MakeUpToDate(
        RaftCore core,
        ulong id)
    {
        Progress progress = core.Tracker.Progress[id];
        progress.MaybeUpdate(core.Log.LastIndex);
        progress.BecomeReplicate();
    }

    internal static Message Transfer(
        ulong transferee,
        ulong recipient = 1,
        ulong term = 0)
    {
        return new Message
        {
            From = transferee,
            To = recipient,
            Term = term,
            Type = MessageType.MsgTransferLeader,
        };
    }

    internal static Message TimeoutNow(
        RaftCore core,
        ulong from,
        ulong? term = null)
    {
        return new Message
        {
            From = from,
            To = core.Id,
            Term = term ?? core.Term,
            Type = MessageType.MsgTimeoutNow,
        };
    }

    internal static Message Proposal(
        ulong id,
        params Entry[] entries)
    {
        var proposal = new Message
        {
            From = id,
            To = id,
            Type = MessageType.MsgProp,
        };
        proposal.Entries.Add(entries);
        return proposal;
    }

    internal static Entry NormalEntry(string data = "command")
    {
        return new Entry
        {
            Data = ByteString.CopyFromUtf8(data),
        };
    }
}
