using System.Buffers.Binary;

using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

namespace DotnetRaft.Storage.Sqlite;

internal static class SqliteStorageCodec
{
    internal static byte[] EncodeUInt64(ulong value)
    {
        var result = new byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64BigEndian(
            result,
            value);
        return result;
    }

    internal static ulong DecodeUInt64(
        byte[] value,
        string description)
    {
        if (value.Length != sizeof(ulong))
        {
            throw Unknown(
                $"{description} is not an eight-byte unsigned value.");
        }

        return BinaryPrimitives.ReadUInt64BigEndian(
            value);
    }

    internal static Snapshot NormalizeSnapshot(
        Snapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        return ProtocolDefaults.EnsureSnapshot(snapshot.Clone());
    }

    internal static Snapshot ParseSnapshot(
        byte[] payload,
        string description)
    {
        try
        {
            return NormalizeSnapshot(
                Snapshot.Parser.ParseFrom(payload));
        }
        catch (InvalidProtocolBufferException exception)
        {
            throw Unknown(
                $"{description} is not a valid snapshot.",
                exception);
        }
    }

    internal static HardState ParseHardState(
        byte[] payload,
        string description)
    {
        try
        {
            return HardState.Parser.ParseFrom(
                payload);
        }
        catch (InvalidProtocolBufferException exception)
        {
            throw Unknown(
                $"{description} is not a valid hard state.",
                exception);
        }
    }

    internal static Entry ParseEntry(
        byte[] payload,
        ulong expectedIndex,
        ulong expectedTerm,
        string description)
    {
        var entry = new Entry();
        try
        {
            entry = Entry.Parser.ParseFrom(payload);
        }
        catch (InvalidProtocolBufferException exception)
        {
            throw Unknown(
                $"{description} is not a valid entry.",
                exception);
        }

        if (entry.Index != expectedIndex
            || entry.Term != expectedTerm)
        {
            throw Unknown(
                $"{description} payload identifies ({entry.Index}, {entry.Term}) instead of ({expectedIndex}, {expectedTerm}).");
        }

        return entry;
    }

    internal static List<Entry> CloneAndValidateEntries(
        IEnumerable<Entry> entries)
    {
        return EntrySequence.CloneContiguous(
            entries,
            static message => new InvalidOperationException(message));
    }

    internal static void EnsureHasSuccessor(
        ulong index,
        string description)
    {
        if (index == ulong.MaxValue)
        {
            throw new InvalidOperationException(
                $"{description} index {index} has no representable successor.");
        }
    }

    internal static StorageException Unknown(
        string message,
        Exception? innerException = null)
    {
        return new StorageException(
            StorageError.Unknown,
            message,
            innerException);
    }
}
