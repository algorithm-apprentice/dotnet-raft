namespace DotnetRaft.Protocol;

/// <summary>The storage/transport adapter's entry budget and membership payload encoding.</summary>
public interface IRaftLogFormat
{
    ulong MeasureEntry(Entry entry);

    RaftBytes EncodeConfigurationChange(ConfChange change);
    RaftBytes EncodeConfigurationChange(ConfChangeV2 change);

    /// <exception cref="InvalidDataException">The payload is malformed.</exception>
    ConfChange DecodeConfigurationChange(ReadOnlySpan<byte> payload);

    /// <exception cref="InvalidDataException">The payload is malformed.</exception>
    ConfChangeV2 DecodeConfigurationChangeV2(ReadOnlySpan<byte> payload);
}
