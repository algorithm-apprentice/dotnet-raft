namespace DotnetRaft.Diagnostics;

public sealed class RaftTraceEvent
{
    internal RaftTraceEvent(
        RaftTraceEventType type,
        BasicStatus status,
        ulong lastLogIndex,
        ConfigurationStatus configuration,
        RaftTraceMessage? message,
        string? detail)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        Type = type;
        Status = status;
        LastLogIndex = lastLogIndex;
        Configuration = configuration;
        Message = message;
        Detail = detail;
    }

    public RaftTraceEventType Type { get; }

    public BasicStatus Status { get; }

    public ulong LastLogIndex { get; }

    public ConfigurationStatus Configuration { get; }

    public RaftTraceMessage? Message { get; }

    public string? Detail { get; }
}
