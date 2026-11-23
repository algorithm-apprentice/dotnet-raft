using DotnetRaft.Protocol;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;
using Wire = DotnetRaft.Protobuf.Wire;

namespace DotnetRaft.Tests.Protocol;

public sealed class ProtobufWireCompatibilityTests
{
    // The unpacked fixtures are output from the proto2 C# codec at 56a4a4d.
    private const string LegacyMembership = "08800108011009100618051804200420052800";

    [Fact]
    public void SchemaUsesProto3() => Assert.Equal("proto3", Wire.RaftReflection.Descriptor.ToProto().Syntax);

    [Fact]
    public void SingularFieldsRetainPresenceWithoutARealOneof()
    {
        Assert.All(Wire.RaftReflection.Descriptor.MessageTypes, message =>
        {
            Assert.DoesNotContain(message.Oneofs, oneof => !oneof.IsSynthetic);
            Assert.All(message.Fields.InDeclarationOrder().Where(field => !field.IsRepeated), field =>
                Assert.True(field.HasPresence, $"{field.FullName} must distinguish absent from explicit default."));
        });
    }

    [Theory]
    [InlineData("081310FFFFFFFFFFFFFFFFFF011801", false)]
    [InlineData("081310FFFFFFFFFFFFFFFFFF011801200540096800", true)]
    public void LegacyStorageAppendDistinguishesZeroVoteFromNoUpdate(string hex, bool hasUpdate)
    {
        byte[] legacy = Convert.FromHexString(hex);
        Message request = ProtobufRaftCodec.DecodeMessage(legacy);

        Assert.Equal(hasUpdate, request.HasVote);
        Assert.Equal(hasUpdate ? new RaftHardState(5, 0, 9) : null, StorageAppendHardState.Read(request));
        Assert.Equal(legacy, ProtobufRaftCodec.EncodeMessage(request));
    }

    [Fact]
    public void LegacyMessageKeepsExplicitDefaultsAndEmptySubmessages()
    {
        byte[] legacy = Convert.FromHexString("08003A0808001000180022004A0412020A00500062007200");
        Message message = ProtobufRaftCodec.DecodeMessage(legacy);

        Assert.True(message.HasType);
        Assert.Equal(MessageType.MsgHup, message.Type);
        Assert.True(message.HasReject);
        Assert.False(message.Reject);
        Assert.True(message.HasContext);
        Assert.True(message.Context.IsEmpty);
        Assert.True(Assert.Single(message.Entries).HasData);
        Snapshot snapshot = Assert.IsType<Snapshot>(message.Snapshot);
        Assert.NotNull(snapshot.Metadata.ConfState);
        Message response = Assert.Single(message.Responses);
        Assert.False(response.HasType);
        Assert.Null(response.Snapshot);
        Assert.Equal(legacy, ProtobufRaftCodec.EncodeMessage(message));
    }

    [Theory]
    [InlineData(LegacyMembership)]
    [InlineData("0A03800101120209061A020504220204052800")] // Packed input accepted by both readers.
    public void MembershipAcceptsEitherEncodingAndWritesLegacyVarints(string hex)
    {
        ConfState membership = ProtobufRaftCodec.DecodeMembership(Convert.FromHexString(hex));

        Assert.Equal([128UL, 1UL], membership.Voters);
        Assert.Equal([9UL, 6UL], membership.Learners);
        Assert.Equal([5UL, 4UL], membership.VotersOutgoing);
        Assert.Equal([4UL, 5UL], membership.LearnersNext);
        Assert.True(membership.HasAutoLeave);
        Assert.False(membership.AutoLeave);
        Assert.Equal(Convert.FromHexString(LegacyMembership), ProtobufRaftCodec.EncodeMembership(membership));
    }

    [Fact]
    public void UnrecognizedPeerAndEntryTypesRemainPresentEnumValues()
    {
        byte[] legacy = Convert.FromHexString("08633A0B08FFFFFFFFFFFFFFFFFF017801");
        Message message = ProtobufRaftCodec.DecodeMessage(legacy);

        Assert.True(message.HasType);
        Assert.Equal((MessageType)99, message.Type);
        Assert.Equal(RaftBytes.CopyFrom([0x78, 0x01]), message.ExtensionData);
        Entry entry = Assert.Single(message.Entries);
        Assert.True(entry.HasType);
        Assert.Equal((EntryType)(-1), entry.Type);
        Assert.Equal(11UL, ProtobufRaftCodec.Instance.MeasureEntry(entry));
        Assert.Equal(legacy, ProtobufRaftCodec.EncodeMessage(message));
    }

    [Theory]
    [InlineData(0, "0800100018042200", "08001204080010041A00")]
    [InlineData(99, "0800106318042200", "08631204086310041A00")]
    public void LegacyConfigurationChangesKeepExplicitEnumValues(int number, string v1, string v2)
    {
        ProtobufRaftCodec codec = ProtobufRaftCodec.Instance;
        byte[] legacyV1 = Convert.FromHexString(v1);
        ProtocolConfChange change = codec.DecodeConfigurationChange(legacyV1);

        Assert.True(change.HasType);
        Assert.Equal((ConfChangeType)number, change.Type);
        Assert.Equal(legacyV1, codec.EncodeConfigurationChange(change).ToByteArray());

        byte[] legacyV2 = Convert.FromHexString(v2);
        ConfChangeV2 batch = codec.DecodeConfigurationChangeV2(legacyV2);

        Assert.True(batch.HasTransition);
        Assert.Equal((ConfChangeTransition)number, batch.Transition);
        ConfChangeSingle single = Assert.Single(batch.Changes);
        Assert.True(single.HasType);
        Assert.Equal((ConfChangeType)number, single.Type);
        Assert.Equal(legacyV2, codec.EncodeConfigurationChange(batch).ToByteArray());
    }
}
