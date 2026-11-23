using System.Globalization;
using System.Text.Json.Serialization;

namespace DotnetRaft.Diagnostics;

internal sealed record StatusSummary(
    string Id,
    ulong Term,
    string Vote,
    ulong Commit,
    string Lead,
    string RaftState,
    ulong Applied,
    Dictionary<string, StatusSummary.PeerProgress> Progress,
    [property: JsonPropertyName("leadtransferee")] string LeadTransferee)
{
    internal static StatusSummary FromStatus(Status status)
    {
        BasicStatus basic = status.Basic;
        return new StatusSummary(
            FormatNodeId(basic.Id),
            basic.Term,
            FormatNodeId(basic.Vote),
            basic.Commit,
            FormatNodeId(basic.LeaderId),
            FormatEnum(basic.Role),
            basic.Applied,
            status.Progress.ToDictionary(
                static peer => FormatNodeId(peer.Key),
                static peer => new PeerProgress(peer.Value.Match, peer.Value.Next, FormatEnum(peer.Value.State)),
                StringComparer.Ordinal),
            FormatNodeId(basic.LeaderTransferee));
    }

    private static string FormatNodeId(ulong id) => id.ToString("x", CultureInfo.InvariantCulture);

    private static string FormatEnum<T>(T value) where T : struct, Enum =>
        Enum.GetName(value) ?? Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);

    internal readonly record struct PeerProgress(ulong Match, ulong Next, string State);
}
