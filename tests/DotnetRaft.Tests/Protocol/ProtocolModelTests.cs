using DotnetRaft.Protocol;

using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace DotnetRaft.Tests.Protocol;

public sealed class ProtocolModelTests
{
    [Fact]
    public void EntryRoundTripsWithOptionalFields()
    {
        var entry = new Entry
        {
            Type = EntryType.EntryNormal,
            Term = 4,
            Index = 7,
            Data = RaftBytes.CopyFromUtf8("set x=1"),
        };

        var decoded = ProtobufRaftCodec.DecodeEntry(entry.ToByteArray());

        Assert.Equal(entry, decoded);
        Assert.True(decoded.HasType);
        Assert.True(decoded.HasTerm);
        Assert.True(decoded.HasIndex);
        Assert.True(decoded.HasData);
    }

    [Fact]
    public void HardStateDistinguishesAnExplicitZeroFromAnAbsentField()
    {
        var absent = new DotnetRaft.Protobuf.Wire.HardState();
        var explicitZero = new DotnetRaft.Protobuf.Wire.HardState
        {
            Term = 0,
        };

        var decoded = DotnetRaft.Protobuf.Wire.HardState.Parser.ParseFrom(explicitZero.ToByteArray());

        Assert.False(absent.HasTerm);
        Assert.True(explicitZero.HasTerm);
        Assert.True(decoded.HasTerm);
        Assert.NotEqual(absent.ToByteArray(), explicitZero.ToByteArray());
    }

    [Fact]
    public void AdapterRetainsExtensionsAndExactEntryBudgetWhenThePositionChanges()
    {
        // Existing raftpb entry: term=2, explicit index=0, type absent,
        // followed by a newer writer's unknown varint field 15.
        byte[] original = [0x10, 0x02, 0x18, 0x00, 0x78, 0x01];
        Entry entry = ProtobufRaftCodec.DecodeEntry(original);
        Assert.True(entry.HasIndex);
        Assert.False(entry.HasType);
        Assert.Equal(original, ProtobufRaftCodec.EncodeEntry(entry));

        Entry moved = entry.Clone();
        moved.Index = 128;
        byte[] expected = [0x10, 0x02, 0x18, 0x80, 0x01, 0x78, 0x01];
        Assert.Equal(expected, ProtobufRaftCodec.EncodeEntry(moved));
        Assert.Equal((ulong)expected.Length, ProtobufRaftCodec.Instance.MeasureEntry(moved));
        Assert.Equal((128UL, 2UL), ProtobufRaftCodec.ReadEntryPosition(expected));

        var append = new Message { Type = MessageType.MsgApp, Term = 2, Entries = { moved } };
        Message roundTrip = ProtobufRaftCodec.DecodeMessage(ProtobufRaftCodec.EncodeMessage(append));
        Assert.Equal(expected, ProtobufRaftCodec.EncodeEntry(Assert.Single(roundTrip.Entries)));
    }

    [Fact]
    public void FastEntryMeasurementMatchesTheGeneratedEncoder()
    {
        Entry[] entries =
        [
            new(),
            new() { Term = 0, Type = EntryType.EntryNormal, Data = RaftBytes.Empty },
            new() { Term = ulong.MaxValue, Index = 128, Type = (EntryType)(-1), Data = RaftBytes.CopyFrom(new byte[128]) },
        ];
        foreach (Entry entry in entries)
        {
            Assert.Equal((ulong)ProtobufRaftCodec.EncodeEntry(entry).Length, ProtobufRaftCodec.Instance.MeasureEntry(entry));
        }
    }

    [Fact]
    public void AdapterRetainsNestedSnapshotAndMembershipExtensions()
    {
        byte[] snapshot = [0x0a, 0x00, 0x12, 0x08, 0x0a, 0x04, 0x08, 0x01, 0x78, 0x01, 0x10, 0x00];
        Snapshot decoded = ProtobufRaftCodec.DecodeSnapshot(snapshot);
        Assert.True(decoded.HasData);
        Assert.True(decoded.Metadata.HasIndex);
        Assert.False(decoded.Metadata.HasTerm);
        Assert.Equal([1UL], decoded.Metadata.ConfState.Voters);
        Assert.Equal(snapshot, ProtobufRaftCodec.EncodeSnapshot(decoded.Clone()));
    }

    [Fact]
    public void DescriptorsRetainThePinnedSchemaFieldNames()
    {
        Assert.Equal("Type", Field(DotnetRaft.Protobuf.Wire.Entry.Descriptor, 1).Name);
        Assert.Equal("Term", Field(DotnetRaft.Protobuf.Wire.Entry.Descriptor, 2).Name);
        Assert.Equal("Index", Field(DotnetRaft.Protobuf.Wire.Entry.Descriptor, 3).Name);
        Assert.Equal("Data", Field(DotnetRaft.Protobuf.Wire.Entry.Descriptor, 4).Name);
        Assert.Equal("logTerm", Field(DotnetRaft.Protobuf.Wire.Message.Descriptor, 5).Name);
        Assert.Equal("rejectHint", Field(DotnetRaft.Protobuf.Wire.Message.Descriptor, 11).Name);
    }

    [Fact]
    public void CloneDoesNotShareNestedMessages()
    {
        var snapshot = new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = 5,
                Term = 2,
                ConfState = new ConfState
                {
                    Voters = { 1UL, 2UL, 3UL },
                },
            },
        };

        var clone = snapshot.Clone();
        clone.Metadata.Index = 8;
        clone.Metadata.ConfState.Voters.Add(4);

        Assert.Equal(5UL, snapshot.Metadata.Index);
        Assert.Equal([1UL, 2UL, 3UL], snapshot.Metadata.ConfState.Voters);
    }

    [Fact]
    public void EnsureSnapshotPopulatesNestedMessagesAndDefaultPresence()
    {
        var snapshot = ProtocolDefaults.EnsureSnapshot(null);

        Assert.NotNull(snapshot.Metadata);
        Assert.NotNull(snapshot.Metadata.ConfState);
        Assert.True(snapshot.Metadata.HasIndex);
        Assert.True(snapshot.Metadata.HasTerm);
        Assert.True(snapshot.Metadata.ConfState.HasAutoLeave);
    }

    [Fact]
    public void ConfStateEquivalenceIgnoresSetOrderAndAbsentFalse()
    {
        var left = new ConfState
        {
            Voters = { 3UL, 1UL, 2UL },
            Learners = { 5UL, 4UL },
        };
        var right = new ConfState
        {
            Voters = { 1UL, 2UL, 3UL },
            Learners = { 4UL, 5UL },
            AutoLeave = false,
        };

        Assert.True(left.IsEquivalentTo(right));
    }

    [Fact]
    public void ConfStateEquivalenceDetectsDifferentMembership()
    {
        var left = new ConfState
        {
            Voters = { 1UL, 2UL, 3UL },
        };
        var right = new ConfState
        {
            Voters = { 1UL, 2UL, 4UL },
        };

        Assert.False(left.IsEquivalentTo(right));
    }

    private static FieldDescriptor Field(MessageDescriptor descriptor, int number)
    {
        return descriptor.Fields.InDeclarationOrder()
            .Single(field => field.FieldNumber == number);
    }
}
