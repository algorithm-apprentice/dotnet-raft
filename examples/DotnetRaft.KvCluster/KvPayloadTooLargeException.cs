namespace DotnetRaft.Examples.KvCluster;

public sealed class KvPayloadTooLargeException(
    int payloadBytes,
    int maximumBytes)
    : InvalidOperationException(
        $"Replicated command payload {payloadBytes} bytes exceeds maximum {maximumBytes} bytes.");
