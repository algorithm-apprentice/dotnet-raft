using System.Buffers.Binary;

using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using ProtocolHardState = DotnetRaft.Protocol.HardState;

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

    internal static RaftHardState ParseHardState(
        byte[] payload,
        string description)
    {
        try
        {
            ProtocolHardState state = ProtocolHardState.Parser.ParseFrom(payload);
            return new RaftHardState(state.Term, state.Vote, state.Commit);
        }
        catch (InvalidProtocolBufferException exception)
        {
            throw Unknown(
                $"{description} is not a valid hard state.",
                exception);
        }
    }

    internal static byte[] EncodeHardState(RaftHardState state)
    {
        return new ProtocolHardState
        {
            Term = state.Term,
            Vote = state.Vote,
            Commit = state.Commit,
        }.ToByteArray();
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
            entry = Entry.Parser.ParseFrom(payload);
        }
        catch (InvalidProtocolBufferException exception)
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
        // Use the protobuf reader for framing, including unknown fields. Do not
        // construct the command ByteString merely to check the entry identity.
        using var input = new CodedInputStream(payload);
        const uint termTag = Entry.TermFieldNumber << 3;
        const uint indexTag = Entry.IndexFieldNumber << 3;
        const uint typeTag = Entry.TypeFieldNumber << 3;
        const uint dataTag = (Entry.DataFieldNumber << 3) | (uint)WireFormat.WireType.LengthDelimited;
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
                        // Keep the generated parser's behavior for unusual wire
                        // forms (including groups and invalid wire types).
                        return ParseEntry(payload, expectedIndex, expectedTerm, description).Term;
                }
            }
        }
        catch (InvalidProtocolBufferException exception)
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
