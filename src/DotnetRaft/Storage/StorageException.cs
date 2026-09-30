namespace DotnetRaft.Storage;

public sealed class StorageException : Exception
{
    public StorageException(
        StorageError error,
        string message,
        Exception? innerException = null)
        : base(message, innerException)
    {
        Error = error;
    }

    public StorageError Error { get; }
}
