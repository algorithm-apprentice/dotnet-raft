using System.Collections.ObjectModel;
using System.Text.Json;

using DotnetRaft.Diagnostics;

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

    public override string ToString() =>
        JsonSerializer.Serialize(StatusSummary.FromStatus(this), StatusJsonContext.Default.StatusSummary);
}
