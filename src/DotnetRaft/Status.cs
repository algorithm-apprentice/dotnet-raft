using System.Collections.ObjectModel;
using System.Globalization;
using System.Text;

namespace DotnetRaft;

public sealed class Status
{
    private readonly ReadOnlyDictionary<ulong, ProgressStatus>
        _progress;

    internal Status(
        BasicStatus basic,
        ConfigurationStatus configuration,
        IEnumerable<KeyValuePair<ulong, ProgressStatus>>
            progress)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(progress);

        Basic = basic;
        Configuration = configuration;
        var sorted =
            new SortedDictionary<ulong, ProgressStatus>();
        foreach ((ulong id, ProgressStatus status) in progress)
        {
            sorted.Add(id, status);
        }

        _progress =
            new ReadOnlyDictionary<ulong, ProgressStatus>(
                sorted);
    }

    public BasicStatus Basic { get; }

    public ConfigurationStatus Configuration { get; }

    public IReadOnlyDictionary<ulong, ProgressStatus>
        Progress => _progress;

    public override string ToString()
    {
        var builder = new StringBuilder();
        builder.Append("{\"id\":\"");
        AppendHex(builder, Basic.Id);
        builder.Append("\",\"term\":");
        AppendDecimal(builder, Basic.Term);
        builder.Append(",\"vote\":\"");
        AppendHex(builder, Basic.Vote);
        builder.Append("\",\"commit\":");
        AppendDecimal(builder, Basic.Commit);
        builder.Append(",\"lead\":\"");
        AppendHex(builder, Basic.LeaderId);
        builder.Append("\",\"raftState\":\"");
        builder.Append(FormatEnum(Basic.Role));
        builder.Append("\",\"applied\":");
        AppendDecimal(builder, Basic.Applied);
        builder.Append(",\"progress\":{");

        var first = true;
        foreach ((ulong id, ProgressStatus progress) in
                 _progress)
        {
            if (!first)
            {
                builder.Append(',');
            }

            first = false;
            builder.Append('"');
            AppendHex(builder, id);
            builder.Append("\":{\"match\":");
            AppendDecimal(builder, progress.Match);
            builder.Append(",\"next\":");
            AppendDecimal(builder, progress.Next);
            builder.Append(",\"state\":\"");
            builder.Append(FormatEnum(progress.State));
            builder.Append("\"}");
        }

        builder.Append("},\"leadtransferee\":\"");
        AppendHex(builder, Basic.LeaderTransferee);
        builder.Append("\"}");
        return builder.ToString();
    }

    private static void AppendDecimal(
        StringBuilder builder,
        ulong value)
    {
        builder.Append(
            value.ToString(
                CultureInfo.InvariantCulture));
    }

    private static void AppendHex(
        StringBuilder builder,
        ulong value)
    {
        builder.Append(
            value.ToString(
                "x",
                CultureInfo.InvariantCulture));
    }

    private static string FormatEnum<T>(T value)
        where T : struct, Enum
    {
        return Enum.IsDefined(value)
            ? value.ToString()
            : Convert.ToInt64(
                    value,
                    CultureInfo.InvariantCulture)
                .ToString(CultureInfo.InvariantCulture);
    }
}
