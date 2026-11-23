using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Tests.Core;
using DotnetRaft.Tests.Interaction;

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

    [Theory]
    [InlineData(1UL)]
    [InlineData(4UL)]
    public void InvalidAppendTermsDoNotAdvanceTheReceiverTerm(ulong entryTerm)
    {
        var node = CreateNode(CreateStorage(voters: [1, 2]));
        BasicStatus before = node.GetBasicStatus();
        var append = new Message
        {
            Type = MessageType.MsgApp,
            From = 2,
            To = 1,
            Term = 3,
            Index = 2,
            LogTerm = 2,
        };
        append.Entries.Add(new Entry { Index = 3, Term = entryTerm });

        Assert.Throws<ArgumentException>("message", () => node.Step(append));
        Assert.Equal(before, node.GetBasicStatus());
        Assert.False(node.HasReady());
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void PeerAppendOwnsEntriesAndDefersReplies(bool asyncStorageWrites, bool empty)
    {
        MemoryStorage storage = CreateStorage(voters: [1, 2]);
        var node = CreateNode(storage, asyncStorageWrites: asyncStorageWrites);
        var append = new Message
        {
            Type = MessageType.MsgApp,
            From = 2,
            To = 1,
            Term = 1,
        };
        Entry entry = Entry.Parser.ParseFrom(new byte[] { 0x10, 0x01, 0x18, 0x01, 0x78, 0x01 });
        byte[] encoded = entry.ToByteArray();
        if (!empty)
        {
            append.Entries.Add(entry);
        }

        node.Step(append);
        entry.Index = 99;
        append.Entries.Clear();
        Assert.Equal(0UL, storage.GetLastIndex());

        Ready ready = node.Ready();
        ulong expectedIndex = empty ? 0UL : 1UL;
        if (empty)
        {
            Assert.Empty(ready.Entries);
        }
        else
        {
            Entry output = Assert.Single(ready.Entries);
            Assert.Equal(encoded, output.ToByteArray());
            Assert.False(output.HasType);
            Assert.False(output.HasData);
        }

        Message response;
        if (asyncStorageWrites)
        {
            Message work = Assert.Single(ready.Messages, message => message.Type == MessageType.MsgStorageAppend);
            Assert.DoesNotContain(ready.Messages, message => message.Type == MessageType.MsgAppResp);
            response = Assert.Single(work.Responses, message => message.Type == MessageType.MsgAppResp);
            if (!empty)
            {
                Assert.Equal(encoded, Assert.Single(work.Entries).ToByteArray());
            }

            Message[] responses = AsyncStorage.AsyncStorageTestSupport.PersistAppend(storage, work);
            foreach (Message completion in responses.Where(message => message.To == 1))
            {
                node.Step(completion);
            }
        }
        else
        {
            response = Assert.Single(ready.Messages, message => message.Type == MessageType.MsgAppResp);
            PersistAndAdvance(node, storage, ready);
        }

        Assert.Equal(2UL, response.To);
        Assert.False(response.Reject);
        Assert.Equal(expectedIndex, response.Index);
        Assert.Equal(expectedIndex, storage.GetLastIndex());
        if (!empty)
        {
            ready.Entries[0].Index = 98;
            Assert.Equal(encoded, Assert.Single(storage.GetEntries(1, 2, ulong.MaxValue)).ToByteArray());
        }
    }

    [Fact]
    public void EmptyAppendAtMaximumIndexUsesNormalRejection()
    {
        var node = CreateNode(CreateStorage(voters: [1, 2]));
        node.Step(new Message
        {
            Type = MessageType.MsgApp,
            From = 2,
            To = 1,
            Term = 1,
            Index = ulong.MaxValue,
        });

        Ready ready = node.Ready();
        Assert.Empty(ready.Entries);
        Message response = Assert.Single(ready.Messages, message => message.Type == MessageType.MsgAppResp);
        Assert.True(response.Reject);
        Assert.Equal(ulong.MaxValue, response.Index);
    }

    [Theory]
    [InlineData(1UL)]
    [InlineData(2UL)]
    public void MalformedAppendIsRejectedEvenWhenRoleOrTermWouldIgnoreIt(ulong term)
    {
        MemoryStorage storage = CreateStorage(voters: [1, 2]);
        storage.SetHardState(new RaftHardState { Term = 1 });
        var node = CreateNode(storage);
        BecomeTwoVoterLeader(node, storage);
        BasicStatus before = node.GetBasicStatus();
        Assert.Equal(2UL, before.Term);
        Message append = AppendWithGap();
        append.Term = term;

        Assert.Throws<ArgumentException>("message", () => node.Step(append));
        Assert.Equal(before, node.GetBasicStatus());
        Assert.False(node.HasReady());
    }

    [Fact]
    public void RejectedPeerMessageDoesNotConsumeBootstrapEligibility()
    {
        var node = CreateNode(CreateEmptyStorage());
        Assert.Throws<ArgumentException>(() => node.Step(AppendWithGap()));

        node.Bootstrap([new Peer(1)]);

        Assert.Equal(1UL, node.GetBasicStatus().Commit);
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
        var progress = node.Core.Tracker.Progress[2];
        progress.RecentActive = false;
        progress.AppendFlowPaused = true;

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
        Assert.False(progress.RecentActive);
        Assert.True(progress.AppendFlowPaused);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public void InvalidReadPositionDoesNotConfirmOrFaultAPendingRead(int invalidPosition)
    {
        MemoryStorage storage = CreateStorage(voters: [1, 2]);
        var node = CreateNode(storage);
        BecomeTwoVoterLeader(node, storage);
        node.ReadIndex("read"u8);
        Ready request = node.Ready();
        Message heartbeat = Assert.Single(request.Messages, message => message.Type == MessageType.MsgHeartbeat);
        Assert.Empty(request.ReadStates);
        PersistAndAdvance(node, storage, request);
        var invalid = new byte[sizeof(ulong)];
        invalid[0] = (byte)invalidPosition;
        var response = new Message
        {
            Type = MessageType.MsgHeartbeatResp,
            From = 2,
            To = 1,
            Term = heartbeat.Term,
            Context = ByteString.CopyFrom(invalid),
        };

        BasicStatus before = node.GetBasicStatus();
        var progress = node.Core.Tracker.Progress[2];
        progress.RecentActive = false;
        progress.AppendFlowPaused = true;
        Assert.Throws<ArgumentException>("context", () => node.Step(response));
        Assert.Equal(before, node.GetBasicStatus());
        Assert.False(node.HasReady());
        Assert.False(progress.RecentActive);
        Assert.True(progress.AppendFlowPaused);

        response.Context = heartbeat.Context;
        node.Step(response);
        ReadState read = Assert.Single(node.Ready().ReadStates);
        Assert.True(read.RequestContext.Span.SequenceEqual("read"u8));
        Assert.Equal(before.Commit, read.Index);
    }

    [Fact]
    public void LowerTermLeaderResponsesRemainIgnorable()
    {
        MemoryStorage storage =
            CreateStorage(voters: [1, 2]);
        storage.SetHardState(
            new RaftHardState
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
    public void HigherTermHeartbeatResponseStepsLeaderDownWithoutParsingItsContext()
    {
        MemoryStorage storage = CreateStorage(voters: [1, 2]);
        var node = CreateNode(storage);
        BecomeTwoVoterLeader(node, storage);
        ulong nextTerm = node.GetBasicStatus().Term + 1;

        node.Step(new Message
        {
            Type = MessageType.MsgHeartbeatResp,
            From = 2,
            To = 1,
            Term = nextTerm,
            Context = ByteString.CopyFrom([1]),
        });

        BasicStatus status = node.GetBasicStatus();
        Assert.Equal(RaftRole.Follower, status.Role);
        Assert.Equal(nextTerm, status.Term);
        Assert.Empty(node.Ready().ReadStates);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TermlessHeartbeatResponseCarriesReadEvidenceInEitherStorageMode(bool asyncStorageWrites)
    {
        // Let the interaction host process the election and worker responses
        // for each protocol before exercising the same incoming reply.
        var cluster = new InteractionEnvironment();
        cluster.AddNodes(2, new InteractionNodeOptions
        {
            Voters = [1, 2],
            SnapshotIndex = 2,
            AsyncStorageWrites = asyncStorageWrites,
        });
        cluster.Campaign(0);
        cluster.Stabilize();
        DotnetRaft.RawNode node = cluster.Nodes[0].RawNode;
        Assert.Equal(RaftRole.Leader, node.GetBasicStatus().Role);

        node.ReadIndex("read"u8);
        Ready request = node.Ready();
        Message heartbeat = Assert.Single(request.Messages, message => message.Type == MessageType.MsgHeartbeat);
        Assert.Empty(request.ReadStates);
        if (!asyncStorageWrites)
        {
            PersistAndAdvance(node, cluster.Nodes[0].Storage, request);
        }

        node.Step(new Message
        {
            Type = MessageType.MsgHeartbeatResp,
            From = heartbeat.To,
            To = heartbeat.From,
            Context = heartbeat.Context,
        });

        ReadState read = Assert.Single(node.Ready().ReadStates);
        Assert.True(read.RequestContext.Span.SequenceEqual("read"u8));
        Assert.Equal(node.GetBasicStatus().Commit, read.Index);
    }

    [Fact]
    public void HeartbeatResponseQueuesCatchUpBeforeCompletingRemoteRead()
    {
        MemoryStorage storage = CreateStorage(voters: [1, 2]);
        var node = CreateNode(storage);
        BecomeTwoVoterLeader(node, storage);

        Message read = ReadIndex(term: 0);
        node.Step(read);
        Ready request = node.Ready();
        Message heartbeat = Assert.Single(request.Messages, message => message.Type == MessageType.MsgHeartbeat);
        PersistAndAdvance(node, storage, request);
        node.Core.Tracker.Progress[2].BecomeProbe();

        var response = new Message
        {
            Type = MessageType.MsgHeartbeatResp,
            From = heartbeat.To,
            To = heartbeat.From,
            Term = heartbeat.Term,
            Context = heartbeat.Context,
        };

        node.Step(response);

        Ready ready = node.Ready();
        Assert.Collection(ready.Messages,
            append => Assert.Equal(MessageType.MsgApp, append.Type),
            reply =>
            {
                Assert.Equal(MessageType.MsgReadIndexResp, reply.Type);
                Assert.Equal(heartbeat.Commit, reply.Index);
                Assert.Equal(read.Entries[0].Data, Assert.Single(reply.Entries).Data);
            });
    }

    [Fact]
    public void LowerTermHeartbeatAndSnapshotRemainIgnorable()
    {
        MemoryStorage storage =
            CreateStorage(voters: [1, 2]);
        storage.SetHardState(
            new RaftHardState
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
            new RaftHardState
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SnapshotPreparationReadsTheLocalTermOnlyOnce(bool matches)
    {
        var state = new ConfState { Voters = { 1, 2 } };
        var storage = new CoreTestStorage
        {
            InitialState = new StorageState(new RaftHardState { Term = 2 }, state),
        };
        storage.LogStorage.Append([EntryAt(1, matches ? 1UL : 2UL)]);
        var node = CreateNode(storage);
        int reads = 0;
        storage.GetTermOverride = index =>
        {
            Assert.Equal(1UL, index);
            reads++;
            return storage.LogStorage.GetTerm(index);
        };
        Message incoming = SnapshotAt(1);
        incoming.Term = 3;
        incoming.Snapshot.Metadata.ConfState.Voters.Add(3);
        if (matches)
        {
            // No configuration is consumed when the local prefix already matches.
            incoming.Snapshot.Metadata.ConfState.Voters.Add(RaftLocalMessageTargets.AppendThread);
        }

        node.Step(incoming);
        incoming.Snapshot.Metadata.ConfState.Voters.Clear();

        Assert.Equal(1, reads);
        Assert.Equal(3UL, node.GetBasicStatus().Term);
        Assert.Equal(1UL, node.GetBasicStatus().Commit);
        Assert.Equal(matches ? [1UL, 2UL] : [1UL, 2UL, 3UL], node.GetStatus().Configuration.Voters);
        Ready ready = node.Ready();
        if (matches)
        {
            Assert.Null(ready.Snapshot);
        }
        else
        {
            Assert.Equal([1UL, 2UL, 3UL], ready.Snapshot!.Metadata.ConfState.Voters);
        }
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
