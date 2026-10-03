using System.Text.Json;
using System.Text.Json.Serialization;

namespace DotnetRaft.Examples.KvCluster;

public sealed record KvSetCommand(
    Guid RequestId,
    string Key,
    string Value);

public static class KvCommandCodec
{
    private static readonly JsonSerializerOptions Options =
        new()
        {
            PropertyNamingPolicy =
                JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = false,
            UnmappedMemberHandling =
                JsonUnmappedMemberHandling.Disallow,
        };

    public static byte[] Encode(
        KvSetCommand command)
    {
        Validate(command);
        return JsonSerializer.SerializeToUtf8Bytes(
            command,
            Options);
    }

    public static KvSetCommand Decode(
        ReadOnlySpan<byte> data)
    {
        KvSetCommand command;
        try
        {
            command =
                JsonSerializer.Deserialize<KvSetCommand>(
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

    private static void Validate(
        KvSetCommand command)
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

        if (command.Value is null)
        {
            throw new InvalidDataException(
                "Replicated command value must be present.");
        }
    }
}
