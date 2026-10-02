namespace DotnetRaft;

public enum ReplicationState
{
    Probe,
    Replicate,
    Snapshot,
}
