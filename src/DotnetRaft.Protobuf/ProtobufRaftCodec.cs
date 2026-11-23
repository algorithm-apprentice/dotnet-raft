using DotnetRaft.Protocol;

using Google.Protobuf;

using DomainConfChange = DotnetRaft.Protocol.ConfChange;
using Wire = DotnetRaft.Protobuf.Wire;

namespace DotnetRaft.Protobuf;

/// <summary>Encodes and decodes the existing raftpb wire and durable formats.</summary>
public sealed partial class ProtobufRaftCodec : IRaftLogFormat
{
    public static ProtobufRaftCodec Instance { get; } = new();

    private ProtobufRaftCodec() { }

    public ulong MeasureEntry(Entry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        return entry.ExtensionData.IsEmpty ? MeasureKnownEntryFields(entry) : (ulong)ToWire(entry).CalculateSize();
    }
    public static byte[] EncodeEntry(Entry entry) => ToWire(entry).ToByteArray();
    public static Entry DecodeEntry(ReadOnlySpan<byte> payload) => FromWire(Parse(Wire.Entry.Parser, payload));

    public static byte[] EncodeSnapshot(Snapshot snapshot) => ToWire(snapshot).ToByteArray();
    public static Snapshot DecodeSnapshot(ReadOnlySpan<byte> payload) => FromWire(Parse(Wire.Snapshot.Parser, payload));

    public static byte[] EncodeMembership(ConfState state) => ToWire(state).ToByteArray();
    public static ConfState DecodeMembership(ReadOnlySpan<byte> payload) => FromWire(Parse(Wire.ConfState.Parser, payload));

    public static byte[] EncodeMessage(Message message) => ToWire(message).ToByteArray();
    public static Message DecodeMessage(ReadOnlySpan<byte> payload) => FromWire(Parse(Wire.Message.Parser, payload));

    public RaftBytes EncodeConfigurationChange(DomainConfChange change) => RaftBytes.CopyFrom(ToWire(change).ToByteArray());
    public RaftBytes EncodeConfigurationChange(ConfChangeV2 change) => RaftBytes.CopyFrom(ToWire(change).ToByteArray());

    public DomainConfChange DecodeConfigurationChange(ReadOnlySpan<byte> payload) => FromWire(Parse(Wire.ConfChange.Parser, payload));

    public ConfChangeV2 DecodeConfigurationChangeV2(ReadOnlySpan<byte> payload) =>
        FromWire(Parse(Wire.ConfChangeV2.Parser, payload));

    public static byte[] EncodeHardState(RaftHardState state) =>
        new Wire.HardState { Term = state.Term, Vote = state.Vote, Commit = state.Commit }.ToByteArray();

    public static RaftHardState DecodeHardState(ReadOnlySpan<byte> payload)
    {
        Wire.HardState state = Parse(Wire.HardState.Parser, payload);
        return new RaftHardState(state.Term, state.Vote, state.Commit);
    }

    private static T Parse<T>(MessageParser<T> parser, ReadOnlySpan<byte> payload) where T : IMessage<T>
    {
        try
        {
            return parser.ParseFrom(payload);
        }
        catch (InvalidProtocolBufferException exception)
        {
            throw new InvalidDataException(exception.Message, exception);
        }
    }

    private static RaftBytes CaptureExtensions(Wire.IWireExtensions source)
    {
        UnknownFieldSet? fields = source.ExtensionFields;
        if (fields is null)
        {
            return RaftBytes.Empty;
        }

        byte[] bytes = new byte[fields.CalculateSize()];
        using var output = new CodedOutputStream(bytes);
        fields.WriteTo(output);
        output.CheckNoSpaceLeft();
        return RaftBytes.CopyFrom(bytes);
    }

    private static T CreateWire<T>(MessageParser<T> parser, RaftBytes extensions)
        where T : IMessage<T>, Wire.IWireExtensions, new() => new()
        {
            // Never install known fields from opaque extension data.
            ExtensionFields = extensions.IsEmpty ? null : Parse(parser, extensions.Span).ExtensionFields,
        };

    private static RaftBytes FromWire(ByteString bytes) => RaftBytes.CopyFrom(bytes.Span);
    private static ByteString ToWire(RaftBytes bytes) => UnsafeByteOperations.UnsafeWrap(bytes.Memory);
}
