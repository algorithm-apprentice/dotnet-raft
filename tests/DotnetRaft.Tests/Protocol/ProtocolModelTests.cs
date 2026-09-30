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
            Data = ByteString.CopyFromUtf8("set x=1"),
        };

        var decoded = Entry.Parser.ParseFrom(entry.ToByteArray());

        Assert.Equal(entry, decoded);
        Assert.True(decoded.HasType);
        Assert.True(decoded.HasTerm);
        Assert.True(decoded.HasIndex);
        Assert.True(decoded.HasData);
    }

    [Fact]
    public void ExplicitZeroRetainsProto2Presence()
    {
        var absent = new HardState();
        var explicitZero = new HardState
        {
            Term = 0,
        };

        var decoded = HardState.Parser.ParseFrom(explicitZero.ToByteArray());

        Assert.False(absent.HasTerm);
        Assert.True(explicitZero.HasTerm);
        Assert.True(decoded.HasTerm);
        Assert.NotEqual(absent.ToByteArray(), explicitZero.ToByteArray());
    }

    [Fact]
    public void DescriptorsRetainThePinnedSchemaFieldNames()
    {
        Assert.Equal("Type", Field(Entry.Descriptor, 1).Name);
        Assert.Equal("Term", Field(Entry.Descriptor, 2).Name);
        Assert.Equal("Index", Field(Entry.Descriptor, 3).Name);
        Assert.Equal("Data", Field(Entry.Descriptor, 4).Name);
        Assert.Equal("logTerm", Field(Message.Descriptor, 5).Name);
        Assert.Equal("rejectHint", Field(Message.Descriptor, 11).Name);
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
