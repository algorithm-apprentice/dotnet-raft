namespace DotnetRaft.Core;

internal sealed class StorageResponseValidationException(
    string message)
    : InvalidOperationException(message);
