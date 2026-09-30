namespace DotnetRaft.Core;

internal static class RaftMessageTargets
{
    public const ulong None = 0;
    public const ulong LocalAppendThread = ulong.MaxValue;
    public const ulong LocalApplyThread = ulong.MaxValue - 1;

    public static bool IsLocal(ulong id)
    {
        return id is LocalAppendThread or LocalApplyThread;
    }
}
