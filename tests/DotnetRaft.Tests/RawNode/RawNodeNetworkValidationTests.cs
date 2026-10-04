using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using static DotnetRaft.Tests.RawNode.RawNodeTestSupport;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.RawNode;

public sealed class RawNodeNetworkValidationTests
{
    public static TheoryData<Message> InvalidMessages =>
        new()
        {
            new Message(),
            new Message
            {
                From = 2,
                To = 1,
                Type = (MessageType)99,
            },
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgVote,
            },
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgProp,
            },
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgReadIndex,
            },
            Proposal(
                new Entry
                {
                    Data = ByteString.CopyFrom(
                        [1]),
                },
                term: 1),
            ReadIndex(term: 1),
            AppendWithGap(),
            AppendEndingAtMaximumIndex(),
            Heartbeat(commit: 1, term: 1),
            Proposal(new Entry
            {
                Type = (EntryType)99,
            }),
            Proposal(new Entry
            {
                Type = EntryType.EntryConfChangeV2,
                Data = ByteString.CopyFrom(
                    [0xff]),
            }),
            Proposal(new Entry
            {
                Type = EntryType.EntryConfChangeV2,
                Data = new ConfChangeV2
                {
                    Transition =
                        (ConfChangeTransition)99,
                }.ToByteString(),
            }),
            Proposal(new Entry
            {
                Type = EntryType.EntryConfChange,
                Data = new ProtocolConfChange
                {
                    NodeId =
                        RaftLocalMessageTargets
                            .ApplyThread,
                }.ToByteString(),
            }),
            SnapshotAt(ulong.MaxValue),
            SnapshotWithReservedMember(),
        };

    [Theory]
    [MemberData(nameof(InvalidMessages))]
    public void InvalidNetworkMessageDoesNotMutateNode(
        Message message)
    {
        var node = CreateNode(
            CreateStorage(voters: [1, 2]));
        BasicStatus before =
            node.GetBasicStatus();
        ulong lastIndex =
            node.Core.Log.LastIndex;
        ConfState configuration =
            node.Core.Tracker.ToConfState();

        Assert.Throws<ArgumentException>(
            () => node.Step(message));

        Assert.Equal(
            before,
            node.GetBasicStatus());
        Assert.Equal(
            lastIndex,
            node.Core.Log.LastIndex);
        Assert.True(
            configuration.IsEquivalentTo(
                node.Core.Tracker.ToConfState()));
        Assert.Empty(
            node.Core.TakeMessages());
        node.Tick();
    }

    [Fact]
    public void InvalidCurrentLeaderResponsesDoNotMutateNode()
    {
        MemoryStorage storage =
            CreateStorage(voters: [1, 2]);
        var node = CreateNode(storage);
        BecomeTwoVoterLeader(
            node,
            storage);
        BasicStatus before =
            node.GetBasicStatus();
        ulong lastIndex =
            node.Core.Log.LastIndex;

        Assert.Throws<ArgumentException>(
            () => node.Step(new Message
            {
                From = 2,
                To = 1,
                Term = node.Core.Term,
                Type = MessageType.MsgAppResp,
                Index = lastIndex + 1,
            }));
        Assert.Throws<ArgumentException>(
            () => node.Step(new Message
            {
                From = 2,
                To = 1,
                Term = node.Core.Term,
                Type =
                    MessageType.MsgHeartbeatResp,
                Context = ByteString.CopyFrom(
                    [1]),
            }));

        Assert.Equal(
            before,
            node.GetBasicStatus());
        Assert.Equal(
            lastIndex,
            node.Core.Log.LastIndex);
        Assert.Empty(
            node.Core.TakeMessages());
    }

    [Fact]
    public void LowerTermLeaderResponsesRemainIgnorable()
    {
        MemoryStorage storage =
            CreateStorage(voters: [1, 2]);
        storage.SetHardState(
            new HardState
            {
                Term = 1,
            });
        var node = CreateNode(storage);
        BecomeTwoVoterLeader(
            node,
            storage);
        Assert.Equal(2UL, node.Core.Term);

        node.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 1,
            Type = MessageType.MsgAppResp,
            Index = ulong.MaxValue,
        });
        node.Step(new Message
        {
            From = 2,
            To = 1,
            Term = 1,
            Type =
                MessageType.MsgHeartbeatResp,
            Context = ByteString.CopyFrom(
                [1]),
        });

        Assert.Equal(
            RaftRole.Leader,
            node.Core.Role);
        Assert.Equal(2UL, node.Core.Term);
    }

    [Fact]
    public void LowerTermHeartbeatAndSnapshotRemainIgnorable()
    {
        MemoryStorage storage =
            CreateStorage(voters: [1, 2]);
        storage.SetHardState(
            new HardState
            {
                Term = 2,
            });
        var node = CreateNode(storage);
        BasicStatus before =
            node.GetBasicStatus();

        node.Step(Heartbeat(
            commit: ulong.MaxValue,
            term: 1));
        node.Step(SnapshotAt(
            ulong.MaxValue));

        Assert.Equal(
            before,
            node.GetBasicStatus());
        Assert.Empty(
            node.Core.TakeMessages());
    }

    [Fact]
    public void CurrentLeaderIgnoresSnapshotRestoreFields()
    {
        MemoryStorage storage =
            CreateStorage(voters: [1, 2]);
        var node = CreateNode(storage);
        BecomeTwoVoterLeader(
            node,
            storage);
        BasicStatus before =
            node.GetBasicStatus();
        Message snapshot =
            SnapshotAt(ulong.MaxValue);
        snapshot.Term = node.Core.Term;

        node.Step(snapshot);

        Assert.Equal(
            before,
            node.GetBasicStatus());
        Assert.Empty(
            node.Core.TakeMessages());
    }

    [Fact]
    public void MatchingSnapshotPreservesCurrentConfiguration()
    {
        MemoryStorage storage =
            CreateStorage(voters: [1, 2]);
        storage.Append(
            [
                new Entry
                {
                    Index = 1,
                    Term = 1,
                },
            ]);
        storage.SetHardState(
            new HardState
            {
                Term = 1,
            });
        var node = CreateNode(storage);
        ConfState configuration =
            node.Core.Tracker.ToConfState();

        node.Step(
            SnapshotWithReservedMember());

        Assert.Equal(
            1UL,
            node.Core.Log.Committed);
        Assert.True(
            configuration.IsEquivalentTo(
                node.Core.Tracker.ToConfState()));
        Assert.True(
            node.HasReady());
    }

    private static Message AppendWithGap()
    {
        var message = new Message
        {
            From = 2,
            To = 1,
            Term = 2,
            LogTerm = 1,
            Index = 3,
            Type = MessageType.MsgApp,
        };
        message.Entries.Add(new Entry
        {
            Index = 5,
            Term = 2,
        });
        return message;
    }

    private static Message
        AppendEndingAtMaximumIndex()
    {
        var message = new Message
        {
            From = 2,
            To = 1,
            Term = 1,
            LogTerm = 1,
            Index = ulong.MaxValue - 1,
            Type = MessageType.MsgApp,
        };
        message.Entries.Add(new Entry
        {
            Index = ulong.MaxValue,
            Term = 1,
        });
        return message;
    }

    private static Message Heartbeat(
        ulong commit,
        ulong term)
    {
        return new Message
        {
            From = 2,
            To = 1,
            Commit = commit,
            Term = term,
            Type = MessageType.MsgHeartbeat,
        };
    }

    private static Message ReadIndex(
        ulong term)
    {
        var message = new Message
        {
            From = 2,
            To = 1,
            Term = term,
            Type = MessageType.MsgReadIndex,
        };
        message.Entries.Add(new Entry
        {
            Data = ByteString.CopyFrom(
                [1]),
        });
        return message;
    }

    private static Message Proposal(
        Entry entry,
        ulong term = 0)
    {
        var message = new Message
        {
            From = 2,
            To = 1,
            Term = term,
            Type = MessageType.MsgProp,
        };
        message.Entries.Add(entry);
        return message;
    }

    private static Message SnapshotAt(
        ulong index)
    {
        var state = new ConfState();
        state.Voters.Add([1UL, 2UL]);
        return new Message
        {
            From = 2,
            To = 1,
            Term = 1,
            Type = MessageType.MsgSnap,
            Snapshot = new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = index,
                    Term = 1,
                    ConfState = state,
                },
            },
        };
    }

    private static Message
        SnapshotWithReservedMember()
    {
        var message = SnapshotAt(1);
        message.Snapshot.Metadata.ConfState
            .Voters.Add(
                RaftLocalMessageTargets
                    .AppendThread);
        return message;
    }
}
