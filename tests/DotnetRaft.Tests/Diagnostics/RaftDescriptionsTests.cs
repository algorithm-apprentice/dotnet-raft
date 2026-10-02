using System.Globalization;

using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Diagnostics;

public sealed class RaftDescriptionsTests
{
    [Fact]
    public void StateAndSnapshotDescriptionsAreExact()
    {
        var state = new ConfState
        {
            AutoLeave = true,
        };
        state.Voters.Add([2UL, 1UL]);
        state.VotersOutgoing.Add(3);
        state.Learners.Add(4);
        state.LearnersNext.Add(5);
        var snapshot = new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = 7,
                Term = 3,
                ConfState = state,
            },
        };

        Assert.Equal(
            "Term:2 Vote:1 Commit:3",
            RaftDescriptions.DescribeHardState(
                new HardState
                {
                    Term = 2,
                    Vote = 1,
                    Commit = 3,
                }));
        Assert.Equal(
            "Term:2 Commit:3",
            RaftDescriptions.DescribeHardState(
                new HardState
                {
                    Term = 2,
                    Commit = 3,
                }));
        Assert.Equal(
            "Lead:1 State:Leader",
            RaftDescriptions.DescribeSoftState(
                new SoftState(1, RaftRole.Leader)));
        Assert.Equal(
            "Voters:[2 1] VotersOutgoing:[3] Learners:[4] LearnersNext:[5] AutoLeave:true",
            RaftDescriptions.DescribeConfState(state));
        Assert.Equal(
            "Index:7 Term:3 ConfState:Voters:[2 1] VotersOutgoing:[3] Learners:[4] LearnersNext:[5] AutoLeave:true",
            RaftDescriptions.DescribeSnapshot(snapshot));
    }

    [Fact]
    public void NormalEntryUsesStableByteEscaping()
    {
        var entry = new Entry
        {
            Term = 1,
            Index = 2,
            Data = ByteString.CopyFrom(
            [
                (byte)'a',
                (byte)'"',
                (byte)'\\',
                (byte)'\n',
                0,
                0xff,
            ]),
        };

        Assert.Equal(
            "1/2 EntryNormal \"a\\\"\\\\\\n\\x00\\xff\"",
            RaftDescriptions.DescribeEntry(entry));
        Assert.Equal(
            "1/2 EntryNormal bytes:6",
            RaftDescriptions.DescribeEntry(
                entry,
                data => $"bytes:{data.Length}"));
    }

    [Fact]
    public void ConfigurationEntriesUseCompactNotation()
    {
        var v1 = new ProtocolConfChange
        {
            Type = ConfChangeType.ConfChangeAddNode,
            NodeId = 3,
        };
        var v2 = new ConfChangeV2();
        v2.Changes.Add(
            new ConfChangeSingle
            {
                Type =
                    ConfChangeType.ConfChangeAddNode,
                NodeId = 3,
            });
        v2.Changes.Add(
            new ConfChangeSingle
            {
                Type =
                    ConfChangeType.ConfChangeAddLearnerNode,
                NodeId = 4,
            });

        Assert.Equal(
            "1/2 EntryConfChange v3",
            RaftDescriptions.DescribeEntry(
                new Entry
                {
                    Term = 1,
                    Index = 2,
                    Type = EntryType.EntryConfChange,
                    Data = v1.ToByteString(),
                }));
        Assert.Equal(
            "1/2 EntryConfChangeV2 v3 l4",
            RaftDescriptions.DescribeEntry(
                new Entry
                {
                    Term = 1,
                    Index = 2,
                    Type = EntryType.EntryConfChangeV2,
                    Data = v2.ToByteString(),
                }));
    }

    [Fact]
    public void UnknownAndMalformedConfigurationEntriesAreStable()
    {
        var unknown = new ConfChangeV2();
        unknown.Changes.Add(
            new ConfChangeSingle
            {
                Type = (ConfChangeType)99,
                NodeId = 9,
            });

        Assert.Equal(
            "1/2 EntryConfChangeV2 unknown9",
            RaftDescriptions.DescribeEntry(
                new Entry
                {
                    Term = 1,
                    Index = 2,
                    Type = EntryType.EntryConfChangeV2,
                    Data = unknown.ToByteString(),
                }));
        Assert.Equal(
            "1/2 EntryConfChange invalid ConfChange payload:ff",
            RaftDescriptions.DescribeEntry(
                new Entry
                {
                    Term = 1,
                    Index = 2,
                    Type = EntryType.EntryConfChange,
                    Data = ByteString.CopyFrom([0xff]),
                }));
        Assert.Equal(
            "1/2 EntryConfChangeV2 invalid ConfChangeV2 payload:ff",
            RaftDescriptions.DescribeEntry(
                new Entry
                {
                    Term = 1,
                    Index = 2,
                    Type = EntryType.EntryConfChangeV2,
                    Data = ByteString.CopyFrom([0xff]),
                }));
    }

    [Fact]
    public void DirectConfigurationDescriptionsUseProtoSymbols()
    {
        var v1 = new ProtocolConfChange
        {
            Type = ConfChangeType.ConfChangeAddNode,
            NodeId = 2,
            Context = ByteString.CopyFromUtf8("ctx"),
        };
        var v2 = new ConfChangeV2
        {
            Transition =
                ConfChangeTransition.JointImplicit,
        };
        v2.Changes.Add(
            new ConfChangeSingle
            {
                Type = (ConfChangeType)99,
                NodeId = 9,
            });

        Assert.Equal(
            "transition:ConfChangeTransitionAuto changes:{type:ConfChangeAddNode node_id:2} context:\"ctx\"",
            RaftDescriptions.DescribeConfChange(v1));
        Assert.Equal(
            "transition:ConfChangeTransitionJointImplicit changes:{type:99 node_id:9}",
            RaftDescriptions.DescribeConfChange(v2));
    }

    [Fact]
    public void MessageDescriptionIncludesOrderedOptionalFields()
    {
        var message = new Message
        {
            From = 0x10,
            To = ulong.MaxValue - 1,
            Type = MessageType.MsgAppResp,
            Term = 5,
            LogTerm = 3,
            Index = 4,
            Reject = true,
            RejectHint = 2,
            Commit = 3,
            Vote = 7,
        };
        message.Entries.Add(
            new Entry
            {
                Term = 3,
                Index = 4,
                Data = ByteString.CopyFromUtf8("x"),
            });

        Assert.Equal(
            "10->ApplyThread MsgAppResp Term:5 Log:3/4 Rejected (Hint: 2) Commit:3 Vote:7 Entries:[3/4 EntryNormal \"x\"]",
            RaftDescriptions.DescribeMessage(message));
    }

    [Fact]
    public void MessageDescriptionIndentsEntriesSnapshotAndResponses()
    {
        var state = new ConfState();
        state.Voters.Add(1);
        var message = new Message
        {
            To = ulong.MaxValue,
            Type = MessageType.MsgSnap,
            Term = 4,
            LogTerm = 2,
            Index = 3,
            Snapshot = new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = 7,
                    Term = 3,
                    ConfState = state,
                },
            },
        };
        message.Entries.Add(
            new Entry
            {
                Term = 1,
                Index = 4,
                Data = ByteString.CopyFromUtf8("a"),
            });
        message.Entries.Add(
            new Entry
            {
                Term = 1,
                Index = 5,
                Data = ByteString.CopyFromUtf8("b"),
            });
        message.Responses.Add(
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgHeartbeat,
                Term = 4,
            });

        Assert.Equal(
            "None->AppendThread MsgSnap Term:4 Log:2/3 Entries:[\n" +
            "  1/4 EntryNormal \"a\"\n" +
            "  1/5 EntryNormal \"b\"\n" +
            "]\n" +
            "  Snapshot: Index:7 Term:3 ConfState:Voters:[1] VotersOutgoing:[] Learners:[] LearnersNext:[] AutoLeave:false Responses:[\n" +
            "  2->1 MsgHeartbeat Term:4 Log:0/0\n" +
            "]",
            RaftDescriptions.DescribeMessage(message));
    }

    [Fact]
    public void ReadyDescriptionHasExactOrderAndTrailingNewline()
    {
        var state = new ConfState();
        state.Voters.Add(1);
        var entry = new Entry
        {
            Term = 1,
            Index = 4,
            Data = ByteString.CopyFromUtf8("x"),
        };
        var ready = new Ready(
            new SoftState(1, RaftRole.Leader),
            new HardState
            {
                Term = 2,
                Vote = 1,
                Commit = 3,
            },
            [new ReadState(
                3,
                ByteString.CopyFromUtf8("ctx"))],
            [entry],
            new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = 5,
                    Term = 2,
                    ConfState = state,
                },
            },
            [entry],
            [new Message
            {
                From = 1,
                To = 2,
                Type = MessageType.MsgHeartbeat,
                Term = 2,
                Commit = 3,
            }],
            mustSync: true);

        Assert.Equal(
            "Ready MustSync=true:\n" +
            "Lead:1 State:Leader\n" +
            "HardState Term:2 Vote:1 Commit:3\n" +
            "ReadStates:[3/\"ctx\"]\n" +
            "Entries:\n" +
            "1/4 EntryNormal \"x\"\n" +
            "Snapshot Index:5 Term:2 ConfState:Voters:[1] VotersOutgoing:[] Learners:[] LearnersNext:[] AutoLeave:false\n" +
            "CommittedEntries:\n" +
            "1/4 EntryNormal \"x\"\n" +
            "Messages:\n" +
            "1->2 MsgHeartbeat Term:2 Log:0/0 Commit:3\n",
            RaftDescriptions.DescribeReady(ready));
    }

    [Fact]
    public void EmptyReadyAndEntryCollectionsAreExact()
    {
        var ready = new Ready(
            null,
            null,
            [],
            [],
            null,
            [],
            [],
            mustSync: false);

        Assert.Equal(
            "<empty Ready>",
            RaftDescriptions.DescribeReady(ready));
        Assert.Equal(
            string.Empty,
            RaftDescriptions.DescribeEntries([]));
    }

    [Fact]
    public void DescriptionFormattingIsCultureInvariant()
    {
        CultureInfo previous =
            CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture =
                CultureInfo.GetCultureInfo("ar-EG");

            Assert.Equal(
                "Term:1234 Vote:2 Commit:99",
                RaftDescriptions.DescribeHardState(
                    new HardState
                    {
                        Term = 1234,
                        Vote = 2,
                        Commit = 99,
                    }));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }
}
