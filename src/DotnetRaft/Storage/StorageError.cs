namespace DotnetRaft.Storage;

public enum StorageError
{
    Unknown = 0,
    Compacted,
    Unavailable,
    SnapshotOutOfDate,
    SnapshotTemporarilyUnavailable,
}
