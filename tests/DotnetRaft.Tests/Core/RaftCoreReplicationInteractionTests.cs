using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Tracker;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreReplicationInteractionTests
{
    [Fact]
    public void ThreeNodeElectionAndProposalConvergeDeterministically()
    {
        var network = new DeterministicNetwork(
            Create(voters: [1, 2, 3], id: 1).Core,
            Create(voters: [1, 2, 3], id: 2).Core,
            Create(voters: [1, 2, 3], id: 3).Core);

        network.Deliver(Hup(1));

        Assert.Equal(RaftRole.Leader, network[1].Role);
        Assert.Equal(RaftRole.Follower, network[2].Role);
        Assert.Equal(RaftRole.Follower, network[3].Role);
        Assert.All(
            network.Nodes,
            core => Assert.Equal(1UL, core.Log.Committed));

        network.Deliver(Proposal(1, "command"));

        Assert.All(network.Nodes, core =>
        {
            Assert.Equal(2UL, core.Log.LastIndex);
            Assert.Equal(2UL, core.Log.Committed);
            Entry[] entries = [.. core.Log.GetAllEntries()];
            Assert.Equal([1UL, 2UL], entries.Select(entry => entry.Index));
            Assert.Equal([1UL, 1UL], entries.Select(entry => entry.Term));
            Assert.True(entries[0].Data.IsEmpty);
            Assert.Equal("command", entries[1].Data.ToStringUtf8());
        });
    }

    [Fact]
    public void TermAwareRetriesRepairDivergentFollowerSuffix()
    {
        RaftCore leader = Create(
            id: 1,
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
                EntryAt(3, 5),
            ],
            term: 5).Core;
        RaftCore follower = Create(
            id: 2,
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 4),
                EntryAt(3, 4),
                EntryAt(4, 4),
            ],
            term: 5).Core;
        var network = new DeterministicNetwork(leader, follower);

        network.Deliver(Hup(1));

        Assert.Equal(RaftRole.Leader, leader.Role);
        Assert.Equal(6UL, leader.Term);
        Assert.Equal(4UL, leader.Log.LastIndex);
        Assert.Equal(4UL, follower.Log.LastIndex);
        Assert.Equal(
            leader.Log.GetAllEntries(),
            follower.Log.GetAllEntries());
        Assert.Equal(4UL, leader.Log.Committed);
        Assert.Equal(4UL, follower.Log.Committed);
        Assert.Equal(2, network.RejectedAppendCount);
    }

    [Fact]
    public void TermAwareHintsSkipLongDivergentSuffixInTwoRejections()
    {
        var leaderEntries = new List<Entry>
        {
            EntryAt(1, 1),
        };
        var followerEntries = new List<Entry>
        {
            EntryAt(1, 1),
        };
        for (ulong index = 2; index <= 101; index++)
        {
            leaderEntries.Add(EntryAt(index, 2));
            followerEntries.Add(EntryAt(index, 3));
        }

        leaderEntries.Add(EntryAt(102, 5));
        RaftCore leader = Create(
            id: 1,
            voters: [1, 2],
            entries: leaderEntries,
            term: 5).Core;
        RaftCore follower = Create(
            id: 2,
            voters: [1, 2],
            entries: followerEntries,
            term: 5).Core;
        var network = new DeterministicNetwork(leader, follower);

        network.Deliver(Hup(1));

        Assert.Equal(RaftRole.Leader, leader.Role);
        Assert.Equal(2, network.RejectedAppendCount);
        Assert.Equal(103UL, leader.Log.LastIndex);
        Assert.Equal(103UL, follower.Log.LastIndex);
        Assert.Equal(
            leader.Log.GetAllEntries(),
            follower.Log.GetAllEntries());
        Assert.Equal(103UL, leader.Log.Committed);
        Assert.Equal(103UL, follower.Log.Committed);
    }

    [Fact]
    public void BoundedPipelineReplicatesAndCommitsMultiEntryProposal()
    {
        var network = new DeterministicNetwork(
            Create(
                voters: [1, 2],
                id: 1,
                maxSizePerMessage: 0,
                maxInflightMessages: 2).Core,
            Create(
                voters: [1, 2],
                id: 2,
                maxSizePerMessage: 0,
                maxInflightMessages: 2).Core);

        network.Deliver(Hup(1));
        network.Deliver(Proposal(
            1,
            "one",
            "two",
            "three",
            "four",
            "five"));

        Assert.All(network.Nodes, core =>
        {
            Assert.Equal(6UL, core.Log.LastIndex);
            Assert.Equal(6UL, core.Log.Committed);
            Assert.Equal(
                ["one", "two", "three", "four", "five"],
                core.Log.GetEntries(2)
                    .Select(entry => entry.Data.ToStringUtf8()));
        });
        Progress remote = network[1].Tracker.Progress[2];
        Assert.Equal(ProgressState.Replicate, remote.State);
        Assert.Equal(6UL, remote.Match);
        Assert.Equal(7UL, remote.Next);
        Assert.Equal(0, remote.Inflights.Count);
    }

    [Fact]
    public void CompactedSlowVoterRecoversThroughPersistedSnapshot()
    {
        ElectionCoreFixture leader = CreateCompactedFixture(
            id: 1,
            voters: [1, 2]);
        ElectionCoreFixture follower = Create(
            id: 2,
            voters: [1, 2],
            term: 1);
        var network = new DeterministicNetwork(
            leader,
            follower);

        network.Deliver(Hup(1));

        Assert.Equal(RaftRole.Leader, leader.Core.Role);
        Assert.Equal(1, network.SnapshotMessageCount);
        Assert.Equal(5UL, follower.Storage.LogStorage
            .GetSnapshot().Metadata.Index);
        Assert.False(follower.Core.Log.HasUnstableSnapshot);
        Assert.Equal(8UL, follower.Core.Log.LastIndex);
        Assert.Equal(8UL, follower.Core.Log.Committed);
        Assert.Equal(
            leader.Core.Log.GetAllEntries(),
            follower.Core.Log.GetAllEntries());
        Progress progress = leader.Core.Tracker.Progress[2];
        Assert.Equal(ProgressState.Replicate, progress.State);
        Assert.Equal(8UL, progress.Match);
    }

    [Fact]
    public void CompactedSlowLearnerRecoversWithoutAffectingQuorum()
    {
        ElectionCoreFixture leader = CreateCompactedFixture(
            id: 1,
            voters: [1, 2],
            learners: [3]);
        ElectionCoreFixture voter = CreateCompactedFixture(
            id: 2,
            voters: [1, 2],
            learners: [3]);
        ElectionCoreFixture learner = Create(
            id: 3,
            voters: [1, 2],
            learners: [3],
            term: 1);
        var network = new DeterministicNetwork(
            leader,
            voter,
            learner);

        network.Deliver(Hup(1));

        Assert.Equal(RaftRole.Leader, leader.Core.Role);
        Assert.Equal(1, network.SnapshotMessageCount);
        Assert.True(learner.Core.IsLearner);
        Assert.Equal(5UL, learner.Storage.LogStorage
            .GetSnapshot().Metadata.Index);
        Assert.False(learner.Core.Log.HasUnstableSnapshot);
        Assert.Equal(8UL, learner.Core.Log.LastIndex);
        Assert.Equal(8UL, learner.Core.Log.Committed);
        Assert.Equal(
            leader.Core.Log.GetAllEntries(),
            learner.Core.Log.GetAllEntries());
        Progress progress = leader.Core.Tracker.Progress[3];
        Assert.True(progress.IsLearner);
        Assert.Equal(ProgressState.Replicate, progress.State);
        Assert.Equal(8UL, progress.Match);
    }

    private static ElectionCoreFixture CreateCompactedFixture(
        ulong id,
        IEnumerable<ulong> voters,
        IEnumerable<ulong>? learners = null)
    {
        var storage = new CoreTestStorage();
        var state = new ConfState();
        state.Voters.Add(voters);
        if (learners is not null)
        {
            state.Learners.Add(learners);
        }

        storage.LogStorage.ApplySnapshot(new Snapshot
        {
            Data = ByteString.CopyFromUtf8("state"),
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
                Id = id,
                Storage = storage,
                Applied = 5,
            },
            _ => 0);
        return new ElectionCoreFixture(core, storage);
    }

    private static Message Proposal(ulong id, string data)
    {
        return Proposal(id, [data]);
    }

    private static Message Proposal(
        ulong id,
        params string[] entries)
    {
        var message = new Message
        {
            From = id,
            To = id,
            Type = MessageType.MsgProp,
        };
        foreach (string data in entries)
        {
            message.Entries.Add(new Entry
            {
                Data = ByteString.CopyFromUtf8(data),
            });
        }

        return message;
    }

    private sealed class DeterministicNetwork
    {
        private readonly Dictionary<ulong, RaftCore> nodes;
        private readonly Dictionary<ulong, CoreTestStorage> storages = [];
        private readonly Queue<Message> messages = new();

        internal DeterministicNetwork(params RaftCore[] nodes)
        {
            this.nodes = nodes.ToDictionary(core => core.Id);
        }

        internal DeterministicNetwork(
            params ElectionCoreFixture[] fixtures)
        {
            nodes = fixtures.ToDictionary(
                fixture => fixture.Core.Id,
                fixture => fixture.Core);
            storages = fixtures.ToDictionary(
                fixture => fixture.Core.Id,
                fixture => fixture.Storage);
        }

        internal IEnumerable<RaftCore> Nodes =>
            nodes.Values.OrderBy(core => core.Id);

        internal int RejectedAppendCount { get; private set; }

        internal int SnapshotMessageCount { get; private set; }

        internal RaftCore this[ulong id] => nodes[id];

        internal void Deliver(Message message)
        {
            Step(message);
            while (messages.TryDequeue(out Message? next))
            {
                Step(next);
            }
        }

        private void Step(Message message)
        {
            if (message.Type == MessageType.MsgAppResp &&
                message.Reject)
            {
                RejectedAppendCount++;
            }

            if (message.Type == MessageType.MsgSnap)
            {
                SnapshotMessageCount++;
            }

            RaftCore core = nodes[message.To];
            core.Step(message);
            CompleteReadyBatches(core);
        }

        private void CompleteReadyBatches(RaftCore core)
        {
            while (true)
            {
                Message[] immediate = core.TakeMessages();
                Message[] afterAppend =
                    core.TakeMessagesAfterAppend();
                if (immediate.Length == 0 &&
                    afterAppend.Length == 0)
                {
                    return;
                }

                if (afterAppend.Length > 0)
                {
                    PersistReadyBatch(core);
                }

                foreach (Message message in immediate)
                {
                    messages.Enqueue(message);
                }

                foreach (Message message in afterAppend)
                {
                    if (message.To == core.Id)
                    {
                        core.Step(message);
                    }
                    else
                    {
                        messages.Enqueue(message);
                    }
                }
            }
        }

        private void PersistReadyBatch(RaftCore core)
        {
            if (!storages.TryGetValue(
                    core.Id,
                    out CoreTestStorage? storage))
            {
                return;
            }

            Snapshot? snapshot =
                core.Log.GetNextUnstableSnapshot();
            Entry[] entries =
                [.. core.Log.GetNextUnstableEntries()];
            core.Log.AcceptUnstable();

            HardState hardState = core.HardState;
            storage.LogStorage.SetHardState(hardState);
            storage.InitialState = new StorageState(
                hardState.Clone(),
                core.Tracker.ToConfState());
            if (snapshot is not null)
            {
                storage.LogStorage.ApplySnapshot(snapshot);
            }

            if (entries.Length > 0)
            {
                storage.LogStorage.Append(entries);
            }

            if (snapshot is not null)
            {
                core.Log.AcknowledgeSnapshot(
                    snapshot.Metadata.Index);
            }

            if (entries.Length > 0)
            {
                core.Log.StableTo(
                    EntryId.From(entries[^1]));
            }
        }
    }
}
