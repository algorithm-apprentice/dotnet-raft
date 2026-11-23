using System.Text.Json.Serialization;

namespace DotnetRaft.Diagnostics;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(StatusSummary))]
internal sealed partial class StatusJsonContext : JsonSerializerContext
{
}
