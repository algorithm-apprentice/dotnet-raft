using System.Buffers.Binary;

using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Storage.Sqlite;

using Google.Protobuf;

using Microsoft.Data.Sqlite;

namespace DotnetRaft.Sqlite.Tests;

internal static class SqliteStorageTestSupport
{
    internal static TemporaryStorageDirectory
        CreateDirectory()
    {
        return new TemporaryStorageDirectory();
    }

    internal static SqliteStorage CreateStorage(
        TemporaryStorageDirectory directory)
    {
        return new SqliteStorage(
            directory.DatabasePath);
    }

    internal static Entry[] Entries(
        params (ulong Index, ulong Term)[] values)
    {
        return
        [
            .. values.Select(value =>
                new Entry
                {
                    Index = value.Index,
                    Term = value.Term,
                }),
        ];
    }

    internal static Snapshot SnapshotAt(
        ulong index,
        ulong term,
        ConfState? confState = null,
        string? data = null)
    {
        return new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = index,
                Term = term,
                ConfState =
                    confState?.Clone()
                    ?? new ConfState(),
            },
            Data = data is null
                ? ByteString.Empty
                : ByteString.CopyFromUtf8(data),
        };
    }

    internal static void AssertStorageError(
        StorageError error,
        Action action)
    {
        StorageException exception =
            Assert.Throws<StorageException>(action);
        Assert.Equal(error, exception.Error);
    }

    internal static void AssertEntries(
        IEnumerable<Entry> expected,
        IReadOnlyList<Entry> actual)
    {
        Entry[] expectedArray = expected.ToArray();
        Assert.Equal(expectedArray.Length, actual.Count);
        for (var index = 0;
             index < expectedArray.Length;
             index++)
        {
            Assert.Equal(
                expectedArray[index],
                actual[index]);
        }
    }

    internal static SqliteConnection OpenRaw(
        string databasePath,
        int timeoutSeconds = 5)
    {
        var builder =
            new SqliteConnectionStringBuilder
            {
                DataSource = databasePath,
                Mode =
                    SqliteOpenMode.ReadWriteCreate,
                Cache = SqliteCacheMode.Private,
                Pooling = false,
                DefaultTimeout = timeoutSeconds,
            };
        var connection =
            new SqliteConnection(
                builder.ToString());
        connection.Open();
        return connection;
    }

    internal static byte[] EncodeIndex(ulong index)
    {
        var result = new byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64BigEndian(
            result,
            index);
        return result;
    }
}

internal sealed class TemporaryStorageDirectory
    : IDisposable
{
    internal TemporaryStorageDirectory()
    {
        DirectoryPath = Path.Combine(
            Path.GetTempPath(),
            "dotnet-raft-sqlite-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(DirectoryPath);
        DatabasePath = Path.Combine(
            DirectoryPath,
            "raft.db");
    }

    internal string DirectoryPath { get; }

    internal string DatabasePath { get; }

    public void Dispose()
    {
        if (Directory.Exists(DirectoryPath))
        {
            Directory.Delete(
                DirectoryPath,
                recursive: true);
        }
    }
}
