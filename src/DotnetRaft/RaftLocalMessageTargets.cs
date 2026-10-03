namespace DotnetRaft;

public static class RaftLocalMessageTargets
{
    public const ulong AppendThread = ulong.MaxValue;

    public const ulong ApplyThread = ulong.MaxValue - 1;

    public static bool IsLocal(ulong id)
    {
        return id is AppendThread or ApplyThread;
    }
}
