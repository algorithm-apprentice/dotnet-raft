namespace DotnetRaft.Tracker;

internal enum ProgressState
{
    Probe,
    Replicate,
    Snapshot,
}
