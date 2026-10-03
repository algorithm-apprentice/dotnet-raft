using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreMembershipInteractionTests
{
    [Fact]
    public void V1VoterAdditionConvergesAndStartsReplication()
    {
        ElectionCoreFixture leader = Create(
            id: 1,
            voters: [1]);
        ElectionCoreFixture joining = Create(
            id: 2,
            voters: [1]);
        var network = new MembershipNetwork(
            leader,
            joining);

        network.Deliver(Hup(1));
        network.Deliver(Proposal(
            1,
            new Entry
            {
                Type = EntryType.EntryConfChange,
                Data = new ProtocolConfChange
                {
                    Type = ConfChangeType.ConfChangeAddNode,
                    NodeId = 2,
                }.ToByteString(),
            }));

        Assert.Equal(RaftRole.Leader, leader.Core.Role);
        Assert.Equal([1UL, 2UL], leader.Core.Tracker.VoterNodes());
        Assert.Equal([1UL, 2UL], joining.Core.Tracker.VoterNodes());
        Assert.Equal(
            leader.Core.Log.LastIndex,
            joining.Core.Log.LastIndex);
        Assert.Equal(
            leader.Core.Log.Committed,
            joining.Core.Log.Committed);
        Assert.Equal(
            leader.Core.Log.Applied,
            joining.Core.Log.Applied);
        Assert.Equal(
            leader.Core.Log.LastIndex,
            leader.Core.Tracker.Progress[2].Match);
    }

    [Fact]
    public void V2AutoJointChangeConvergesToFinalConfiguration()
    {
        ElectionCoreFixture leader = Create(
            id: 1,
            voters: [1]);
        ElectionCoreFixture voter = Create(
            id: 2,
            voters: [1]);
        ElectionCoreFixture learner = Create(
            id: 3,
            voters: [1]);
        var network = new MembershipNetwork(
            leader,
            voter,
            learner);
        var change = new ConfChangeV2();
        change.Changes.Add(new ConfChangeSingle
        {
            Type = ConfChangeType.ConfChangeAddNode,
            NodeId = 2,
        });
        change.Changes.Add(new ConfChangeSingle
        {
            Type = ConfChangeType.ConfChangeAddLearnerNode,
            NodeId = 3,
        });

        network.Deliver(Hup(1));
        network.Deliver(Proposal(
            1,
            new Entry
            {
                Type = EntryType.EntryConfChangeV2,
                Data = change.ToByteString(),
            }));

        var expected = new ConfState
        {
            Voters = { 1, 2 },
            Learners = { 3 },
        };
        Assert.All(network.Nodes, core =>
        {
            Assert.True(expected.IsEquivalentTo(
                core.Tracker.ToConfState()));
            Assert.False(core.Tracker.Config.AutoLeave);
            Assert.Equal(
                leader.Core.Log.LastIndex,
                core.Log.LastIndex);
            Assert.Equal(
                leader.Core.Log.Committed,
                core.Log.Committed);
            Assert.Equal(
                leader.Core.Log.Applied,
                core.Log.Applied);
        });
        Entry exit = Assert.Single(
            leader.Core.Log.GetAllEntries(),
            entry =>
                entry.Type ==
                    EntryType.EntryConfChangeV2
                && entry.Index ==
                    leader.Core.PendingConfigurationIndex);
        Assert.Equal(
            new ConfChangeV2(),
            ConfChangeV2.Parser.ParseFrom(exit.Data));
        Assert.Equal(
            exit.Index,
            leader.Core.PendingConfigurationIndex);
        Assert.True(learner.Core.Tracker.IsLearner(learner.Core.Id));
    }

    [Fact]
    public void RemovedLeaderDoesNotCountTowardTrailingEntryCommit()
    {
        ElectionCoreFixture leader = Create(
            id: 1,
            voters: [1, 2, 3, 4],
            maxSizePerMessage: 0);
        ElectionCoreFixture voter2 = Create(
            id: 2,
            voters: [1, 2, 3, 4],
            maxSizePerMessage: 0);
        ElectionCoreFixture voter3 = Create(
            id: 3,
            voters: [1, 2, 3, 4],
            maxSizePerMessage: 0);
        ElectionCoreFixture voter4 = Create(
            id: 4,
            voters: [1, 2, 3, 4],
            maxSizePerMessage: 0);
        var network = new MembershipNetwork(
            leader,
            voter2,
            voter3,
            voter4);

        network.Deliver(Hup(1));
        network.Submit(Proposal(
            1,
            new Entry
            {
                Type = EntryType.EntryConfChange,
                Data = new ProtocolConfChange
                {
                    Type = ConfChangeType.ConfChangeRemoveNode,
                    NodeId = 1,
                }.ToByteString(),
            }));
        network.Submit(Proposal(
            1,
            new Entry
            {
                Data = ByteString.CopyFromUtf8("trailing"),
            }));

        network.DrainWhere(message =>
            message.To == 2 || message.From == 2);
        Assert.True(network.DeliverNext(message =>
            message.To == 3
            && message.Type == MessageType.MsgApp
            && message.Entries.Any(entry =>
                entry.Type ==
                    EntryType.EntryConfChange)));
        network.DrainWhere(message =>
            message.From == 3 && message.To == 1);

        Assert.Equal(RaftRole.Leader, leader.Core.Role);
        Assert.False(
            leader.Core.Tracker.Progress.ContainsKey(1));
        Assert.Equal([2UL, 3UL, 4UL], leader.Core.Tracker.VoterNodes());
        Assert.Equal(2UL, leader.Core.Log.Committed);
        Assert.Equal(2UL, leader.Core.Log.Applied);
        Assert.Equal(3UL, leader.Core.Log.LastIndex);
        Assert.Equal(3UL, leader.Core.Tracker.Progress[2].Match);
        Assert.Equal(2UL, leader.Core.Tracker.Progress[3].Match);

        Assert.True(network.DeliverNext(message =>
            message.To == 3
            && message.Type == MessageType.MsgApp
            && message.Entries.Any(entry =>
                entry.Index == 3)));
        network.DrainWhere(message =>
            message.From == 3 && message.To == 1);

        Assert.Equal(3UL, leader.Core.Log.Committed);
        Assert.Equal(3UL, leader.Core.Log.Applied);
    }

    private static Message Proposal(
        ulong id,
        Entry entry)
    {
        var message = new Message
        {
            From = id,
            To = id,
            Type = MessageType.MsgProp,
        };
        message.Entries.Add(entry);
        return message;
    }

    private sealed class MembershipNetwork
    {
        private readonly Dictionary<ulong, RaftCore> nodes;
        private readonly Dictionary<ulong, CoreTestStorage> storages;
        private readonly Queue<Message> messages = new();

        internal MembershipNetwork(
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

        internal void Deliver(Message message)
        {
            Step(message);
            while (messages.TryDequeue(out Message? next))
            {
                Step(next);
            }
        }

        internal void Submit(Message message)
        {
            Step(message);
        }

        internal bool DeliverNext(
            Func<Message, bool> predicate)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            int count = messages.Count;
            for (var index = 0; index < count; index++)
            {
                Message next = messages.Dequeue();
                if (predicate(next))
                {
                    Step(next);
                    return true;
                }

                messages.Enqueue(next);
            }

            return false;
        }

        internal void DrainWhere(
            Func<Message, bool> predicate)
        {
            while (DeliverNext(predicate))
            {
            }
        }

        private void Step(Message message)
        {
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
                PersistReadyBatch(core);
                bool applied = ApplyCommitted(core);

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

                if (immediate.Length == 0
                    && afterAppend.Length == 0
                    && !applied)
                {
                    return;
                }
            }
        }

        private void PersistReadyBatch(RaftCore core)
        {
            CoreTestStorage storage = storages[core.Id];
            Snapshot? snapshot =
                core.Log.GetNextUnstableSnapshot();
            Entry[] entries =
                [.. core.Log.GetNextUnstableEntries()];
            if (snapshot is not null || entries.Length > 0)
            {
                core.Log.AcceptUnstable();
            }

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

        private static bool ApplyCommitted(RaftCore core)
        {
            Entry[] entries =
            [
                .. core.Log.GetNextCommittedEntries(
                    allowUnstable: false),
            ];
            if (entries.Length == 0)
            {
                return false;
            }

            ulong encodedSize = EntrySizing.EncodedSize(entries);
            core.Log.AcceptApplying(
                entries[^1].Index,
                encodedSize,
                allowUnstable: false);
            foreach (Entry entry in entries)
            {
                switch (entry.Type)
                {
                    case EntryType.EntryConfChange:
                        core.ApplyConfigurationChange(
                            ProtocolConfChange.Parser.ParseFrom(
                                entry.Data));
                        break;
                    case EntryType.EntryConfChangeV2:
                        core.ApplyConfigurationChange(
                            ConfChangeV2.Parser.ParseFrom(
                                entry.Data));
                        break;
                }
            }

            core.AppliedTo(
                entries[^1].Index,
                encodedSize);
            return true;
        }
    }
}
