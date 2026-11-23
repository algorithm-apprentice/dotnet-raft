using DotnetRaft.Diagnostics;
using DotnetRaft.Protobuf;
using DotnetRaft.Protocol;

using LegacyChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft;

// Test fixtures still describe the existing protobuf artifacts. These helpers
// keep the adapter explicit without giving domain types serialization methods.
internal static class ProtobufFixtureEncoding
{
    internal static LegacyChange DecodeLegacy(RaftBytes payload) => ProtobufRaftCodec.Instance.DecodeConfigurationChange(payload.Span);
    internal static ConfChangeV2 DecodeV2(RaftBytes payload) => ProtobufRaftCodec.Instance.DecodeConfigurationChangeV2(payload.Span);
    internal static string DescribeEntry(Entry entry, EntryFormatter? formatter = null) =>
        RaftDescriptions.DescribeEntry(entry, formatter, ProtobufRaftCodec.Instance);
    internal static string DescribeEntries(IEnumerable<Entry> entries, EntryFormatter? formatter = null) =>
        RaftDescriptions.DescribeEntries(entries, formatter, ProtobufRaftCodec.Instance);
    internal static string DescribeMessage(Message message, EntryFormatter? formatter = null) =>
        RaftDescriptions.DescribeMessage(message, formatter, ProtobufRaftCodec.Instance);
    internal static string DescribeReady(Ready ready, EntryFormatter? formatter = null) =>
        RaftDescriptions.DescribeReady(ready, formatter, ProtobufRaftCodec.Instance);
    internal static int CalculateSize(this Entry entry) => checked((int)ProtobufRaftCodec.Instance.MeasureEntry(entry));
    internal static byte[] ToByteArray(this Entry entry) => ProtobufRaftCodec.EncodeEntry(entry);
    internal static byte[] ToByteArray(this Snapshot snapshot) => ProtobufRaftCodec.EncodeSnapshot(snapshot);
    internal static byte[] ToByteArray(this ConfState state) => ProtobufRaftCodec.EncodeMembership(state);
    internal static byte[] ToByteArray(this Message message) => ProtobufRaftCodec.EncodeMessage(message);
    internal static RaftBytes ToByteString(this LegacyChange change) => ProtobufRaftCodec.Instance.EncodeConfigurationChange(change);
    internal static RaftBytes ToByteString(this ConfChangeV2 change) => ProtobufRaftCodec.Instance.EncodeConfigurationChange(change);
    internal static RaftBytes ToByteString(this Message message) => RaftBytes.CopyFrom(ProtobufRaftCodec.EncodeMessage(message));
    internal static byte[] ToByteArray(this LegacyChange change) => change.ToByteString().ToByteArray();
    internal static byte[] ToByteArray(this ConfChangeV2 change) => change.ToByteString().ToByteArray();
}
