namespace DotnetRaft.Diagnostics;

public interface IRaftTraceSink
{
    void Trace(RaftTraceEvent traceEvent);
}
