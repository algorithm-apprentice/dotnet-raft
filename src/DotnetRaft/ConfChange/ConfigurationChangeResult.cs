using DotnetRaft.Tracker;

namespace DotnetRaft.ConfChange;

internal sealed record ConfigurationChangeResult(
    TrackerConfig Config,
    ProgressMap Progress);
