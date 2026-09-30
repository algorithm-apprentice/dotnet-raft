namespace DotnetRaft.Diagnostics;

public enum RaftLogLevel
{
    Debug,
    Information,
    Warning,
    Error,
}

public interface IRaftLogger
{
    bool IsEnabled(RaftLogLevel level);

    void Log(RaftLogLevel level, string message);
}

public sealed class NullRaftLogger : IRaftLogger
{
    private NullRaftLogger()
    {
    }

    public static NullRaftLogger Instance { get; } = new();

    public bool IsEnabled(RaftLogLevel level)
    {
        return false;
    }

    public void Log(RaftLogLevel level, string message)
    {
    }
}
