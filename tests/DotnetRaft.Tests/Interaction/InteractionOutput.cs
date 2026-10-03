using System.Text;

using DotnetRaft.Diagnostics;

namespace DotnetRaft.Tests.Interaction;

internal enum InteractionLogLevel
{
    Debug,
    Information,
    Warning,
    Error,
    None,
}

internal sealed class InteractionOutput : IRaftLogger
{
    private StringBuilder _builder = new();

    internal InteractionLogLevel Level { get; set; } =
        InteractionLogLevel.Debug;

    internal bool IsQuiet =>
        Level == InteractionLogLevel.None;

    internal int Length => _builder.Length;

    internal void Reset()
    {
        _builder.Clear();
    }

    internal void Write(string value)
    {
        if (!IsQuiet)
        {
            _builder.Append(value);
        }
    }

    internal void WriteLine(string value)
    {
        if (!IsQuiet)
        {
            _builder.Append(value);
            _builder.Append('\n');
        }
    }

    internal void WithIndent(Action action)
    {
        ArgumentNullException.ThrowIfNull(action);
        StringBuilder parent = _builder;
        var nested = new StringBuilder();
        _builder = nested;
        try
        {
            action();
        }
        finally
        {
            _builder = parent;
            string text = nested.ToString();
            if (text.Length > 0)
            {
                string[] lines = text.Split('\n');
                for (var index = 0;
                     index < lines.Length;
                     index++)
                {
                    if (index == lines.Length - 1
                        && lines[index].Length == 0)
                    {
                        break;
                    }

                    parent.Append("  ");
                    parent.Append(lines[index]);
                    parent.Append('\n');
                }
            }
        }
    }

    public bool IsEnabled(RaftLogLevel level)
    {
        return Level != InteractionLogLevel.None
            && Map(level) >= Level;
    }

    public void Log(
        RaftLogLevel level,
        string message)
    {
        if (!IsEnabled(level))
        {
            return;
        }

        _builder.Append(GetName(level));
        _builder.Append(' ');
        _builder.Append(message);
        _builder.Append('\n');
    }

    public override string ToString()
    {
        return _builder.ToString();
    }

    private static InteractionLogLevel Map(
        RaftLogLevel level)
    {
        return level switch
        {
            RaftLogLevel.Debug =>
                InteractionLogLevel.Debug,
            RaftLogLevel.Information =>
                InteractionLogLevel.Information,
            RaftLogLevel.Warning =>
                InteractionLogLevel.Warning,
            RaftLogLevel.Error =>
                InteractionLogLevel.Error,
            _ => throw new ArgumentOutOfRangeException(
                nameof(level),
                level,
                "Unknown Raft log level."),
        };
    }

    private static string GetName(RaftLogLevel level)
    {
        return level switch
        {
            RaftLogLevel.Debug => "DEBUG",
            RaftLogLevel.Information => "INFO",
            RaftLogLevel.Warning => "WARN",
            RaftLogLevel.Error => "ERROR",
            _ => throw new ArgumentOutOfRangeException(
                nameof(level),
                level,
                "Unknown Raft log level."),
        };
    }
}
