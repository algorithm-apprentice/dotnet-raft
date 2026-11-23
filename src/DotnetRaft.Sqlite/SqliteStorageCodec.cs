using System.Buffers.Binary;

using DotnetRaft.Core;
using DotnetRaft.Protobuf;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

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
                ProtobufRaftCodec.DecodeSnapshot(payload));
        }
        catch (InvalidDataException exception)
        {
            throw Unknown(
                $"{description} is not a valid snapshot.",
                exception);
        }
    }

    internal static RaftHardState ParseHardState(
        byte[] payload,
        string description)
    {
        try
        {
            return ProtobufRaftCodec.DecodeHardState(payload);
        }
        catch (InvalidDataException exception)
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
        Entry entry;
        try
        {
            entry = ProtobufRaftCodec.DecodeEntry(payload);
        }
        catch (InvalidDataException exception)
        {
            throw Unknown(
                $"{description} is not a valid entry.",
                exception);
        }

        RequireEntryIdentity(entry.Index, entry.Term, expectedIndex, expectedTerm, description);
        return entry;
    }

    internal static ulong ReadEntryTerm(byte[] payload, ulong expectedIndex, ulong expectedTerm, string description)
    {
        ulong index;
        ulong term;
        try
        {
            (index, term) = ProtobufRaftCodec.ReadEntryPosition(payload);
        }
        catch (InvalidDataException exception)
        {
            throw Unknown($"{description} is not a valid entry.", exception);
        }

        RequireEntryIdentity(index, term, expectedIndex, expectedTerm, description);
        return term;
    }

    private static void RequireEntryIdentity(ulong index, ulong term, ulong expectedIndex, ulong expectedTerm, string description)
    {
        if (index != expectedIndex || term != expectedTerm)
        {
            throw Unknown(
                $"{description} payload identifies ({index}, {term}) instead of ({expectedIndex}, {expectedTerm}).");
        }
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
