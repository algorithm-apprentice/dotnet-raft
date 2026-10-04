using System.Buffers.Binary;

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
        Snapshot result = snapshot.Clone();
        result.Metadata ??= new SnapshotMetadata();
        if (!result.Metadata.HasIndex)
        {
            result.Metadata.Index = 0;
        }

        if (!result.Metadata.HasTerm)
        {
            result.Metadata.Term = 0;
        }

        result.Metadata.ConfState ??=
            new ConfState();
        if (!result.Metadata.ConfState.HasAutoLeave)
        {
            result.Metadata.ConfState.AutoLeave =
                false;
        }

        return result;
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
        ArgumentNullException.ThrowIfNull(entries);

        var result = new List<Entry>();
        ulong? previousIndex = null;
        foreach (Entry entry in entries)
        {
            if (entry is null)
            {
                throw new ArgumentException(
                    "Entries cannot contain null values.",
                    nameof(entries));
            }

            if (previousIndex.HasValue
                && (previousIndex.Value
                        == ulong.MaxValue
                    || entry.Index
                        != previousIndex.Value + 1))
            {
                throw new InvalidOperationException(
                    $"Entries are not contiguous at indexes {previousIndex.Value} and {entry.Index}.");
            }

            result.Add(entry.Clone());
            previousIndex = entry.Index;
        }

        return result;
    }

    internal static IReadOnlyList<Entry> LimitSize(
        List<Entry> entries,
        ulong maxSize)
    {
        if (entries.Count == 0)
        {
            return [];
        }

        ulong size =
            (ulong)entries[0].CalculateSize();
        var count = 1;
        while (count < entries.Count)
        {
            ulong next =
                (ulong)entries[count].CalculateSize();
            if (size > maxSize
                || next > maxSize - size)
            {
                break;
            }

            size += next;
            count++;
        }

        return count == entries.Count
            ? entries
            : entries.GetRange(0, count);
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
