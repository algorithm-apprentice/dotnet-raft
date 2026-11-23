using DotnetRaft.Protocol;

using Google.Protobuf;

using Wire = DotnetRaft.Protobuf.Wire;

namespace DotnetRaft.Protobuf;

public sealed partial class ProtobufRaftCodec
{
    private static ulong MeasureKnownEntryFields(Entry entry)
    {
        // The adapter uses protobuf's size primitives so the common path does
        // not allocate a second entry or a payload wrapper just to size a page.
        ulong size = 0;
        if (entry.HasTerm) size += FieldSize(Wire.Entry.TermFieldNumber, CodedOutputStream.ComputeUInt64Size(entry.Term));
        if (entry.HasIndex) size += FieldSize(Wire.Entry.IndexFieldNumber, CodedOutputStream.ComputeUInt64Size(entry.Index));
        if (entry.HasType) size += FieldSize(Wire.Entry.TypeFieldNumber, CodedOutputStream.ComputeEnumSize((int)entry.Type));
        if (entry.HasData)
        {
            int length = entry.Data.Length;
            size += FieldSize(Wire.Entry.DataFieldNumber, CodedOutputStream.ComputeLengthSize(length)) + (ulong)length;
        }

        return size;
    }

    private static ulong FieldSize(int tag, int valueSize) => (ulong)(CodedOutputStream.ComputeTagSize(tag) + valueSize);

    /// <summary>Reads the log position without materializing the command payload.</summary>
    public static (ulong Index, ulong Term) ReadEntryPosition(byte[] payload)
    {
        using var input = new CodedInputStream(payload);
        const uint termTag = Wire.Entry.TermFieldNumber << 3;
        const uint indexTag = Wire.Entry.IndexFieldNumber << 3;
        const uint typeTag = Wire.Entry.TypeFieldNumber << 3;
        const uint dataTag = (Wire.Entry.DataFieldNumber << 3) | (uint)WireFormat.WireType.LengthDelimited;
        ulong index = 0;
        ulong term = 0;
        try
        {
            for (uint tag = input.ReadTag(); tag != 0; tag = input.ReadTag())
            {
                switch (tag)
                {
                    case termTag:
                        term = input.ReadUInt64();
                        break;
                    case indexTag:
                        index = input.ReadUInt64();
                        break;
                    case typeTag:
                        _ = input.ReadEnum();
                        break;
                    case dataTag:
                        input.SkipLastField();
                        break;
                    default:
                        // Delegate unusual field forms to the same parser as DecodeEntry.
                        Wire.Entry parsed = Parse(Wire.Entry.Parser, payload);
                        return (parsed.Index, parsed.Term);
                }
            }
        }
        catch (InvalidProtocolBufferException exception)
        {
            throw new InvalidDataException(exception.Message, exception);
        }

        return (index, term);
    }

    private static Entry FromWire(Wire.Entry source)
    {
        var entry = new Entry { ExtensionData = CaptureExtensions(source) };
        if (source.HasType) entry.Type = (EntryType)source.Type;
        if (source.HasIndex) entry.Index = source.Index;
        if (source.HasTerm) entry.Term = source.Term;
        if (source.HasData) entry.Data = FromWire(source.Data);
        return entry;
    }

    private static Wire.Entry ToWire(Entry entry)
    {
        Wire.Entry wire = CreateWire(Wire.Entry.Parser, entry.ExtensionData);
        if (entry.HasType) wire.Type = (Wire.EntryType)entry.Type;
        if (entry.HasIndex) wire.Index = entry.Index;
        if (entry.HasTerm) wire.Term = entry.Term;
        if (entry.HasData) wire.Data = ToWire(entry.Data);
        return wire;
    }
}
