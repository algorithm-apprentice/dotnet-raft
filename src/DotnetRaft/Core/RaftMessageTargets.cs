namespace DotnetRaft.Core;

internal static class RaftMessageTargets
{
    public const ulong None = 0;
    public const ulong LocalAppendThread =
        RaftLocalMessageTargets.AppendThread;
    public const ulong LocalApplyThread =
        RaftLocalMessageTargets.ApplyThread;

    public static bool IsLocal(ulong id)
    {
        return RaftLocalMessageTargets.IsLocal(id);
    }
}
