using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;

using Google.Protobuf;

namespace DotnetRaft.Examples.KvCluster;

public sealed record KeyValueReadResult(
    bool Found,
    string? Value,
    ulong PhysicalApplied);

public enum KvCommandType
{
    Set = 1,
    Delete = 2,
}

public sealed record KvCommand(
    Guid RequestId,
    KvCommandType Type,
    string Key,
    string? Value);

public sealed record KvApplyResult(
    Guid RequestId,
    ByteString Fingerprint,
    ulong ResultIndex,
    ulong PhysicalApplied,
    bool Duplicate,
    bool Conflict);

public sealed record KvRequestResolution(
    ulong ResultIndex,
    bool Conflict);

public sealed class KvRequestConflictException(
    Guid requestId)
    : InvalidOperationException(
        $"Request ID {requestId} is already associated with another command.");

public sealed class KvPayloadTooLargeException(
    int payloadBytes,
    int maximumBytes)
    : InvalidOperationException(
        $"Replicated command payload {payloadBytes} bytes exceeds maximum {maximumBytes} bytes.");

public static class DurableKvCommandCodec
{
    private static readonly JsonSerializerOptions
        Options = new()
        {
            PropertyNamingPolicy =
                JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = false,
            UnmappedMemberHandling =
                JsonUnmappedMemberHandling.Disallow,
        };

    public static byte[] Encode(KvCommand command)
    {
        Validate(command);
        return JsonSerializer.SerializeToUtf8Bytes(
            command,
            Options);
    }

    public static KvCommand Decode(
        ReadOnlySpan<byte> data)
    {
        KvCommand command = null!;
        try
        {
            command =
                JsonSerializer.Deserialize<KvCommand>(
                    data,
                    Options)
                ?? throw new InvalidDataException(
                    "Replicated command is null.");
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException(
                $"Replicated command is malformed: {exception.Message}",
                exception);
        }

        Validate(command);
        return command;
    }

    public static ByteString Fingerprint(
        KvCommand command)
    {
        return ByteString.CopyFrom(
            SHA256.HashData(
                Encode(command)));
    }

    private static void Validate(KvCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        if (command.RequestId == Guid.Empty)
        {
            throw new InvalidDataException(
                "Replicated command request ID must be nonempty.");
        }

        if (string.IsNullOrWhiteSpace(command.Key))
        {
            throw new InvalidDataException(
                "Replicated command key must be nonempty.");
        }

        switch (command.Type)
        {
            case KvCommandType.Set
                when command.Value is not null:
            case KvCommandType.Delete
                when command.Value is null:
                return;
            case KvCommandType.Set:
                throw new InvalidDataException(
                    "Set command value must be present.");
            case KvCommandType.Delete:
                throw new InvalidDataException(
                    "Delete command value must be absent.");
            default:
                throw new InvalidDataException(
                    $"Unknown KV command type {command.Type}.");
        }
    }
}
