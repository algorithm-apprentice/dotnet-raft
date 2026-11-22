using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;

namespace DotnetRaft.Examples.Counter;

// All observations run on the demo's single host loop. Never call into
// the node from Trace; use the detached event instead.
internal sealed class CounterWriteTrace : IRaftTraceSink
{
    private ulong _index;

    internal void Follow(ulong index)
    {
        _index = index;
        Console.WriteLine($"Tracing entry {index} (storage is in memory).");
    }

    public void Trace(RaftTraceEvent traceEvent)
    {
        if (_index == 0)
        {
            return;
        }

        BasicStatus status = traceEvent.Status;
        switch (traceEvent.Type)
        {
            case RaftTraceEventType.EntriesAppended when traceEvent.LastLogIndex == _index:
                Core(status.Id, $"appended entry {_index} to the unstable log (term {status.Term})");
                break;
            case RaftTraceEventType.MessageReceived when traceEvent.Message is { Type: MessageType.MsgApp } append
                && append.Index < _index && _index - append.Index <= (ulong)append.EntryCount:
                Core(status.Id, $"received MsgApp from node {append.From} carrying entry {_index}");
                break;
            case RaftTraceEventType.MessageSent or RaftTraceEventType.MessageReceived
                when traceEvent.Message is { Type: MessageType.MsgAppResp, Reject: false } response
                && response.Index == _index:
                string action = traceEvent.Type == RaftTraceEventType.MessageSent ? "queued" : "received";
                Core(status.Id, $"{action} MsgAppResp {response.From}->{response.To} for index {_index}");
                break;
            case RaftTraceEventType.MessageReceived
                when traceEvent.Message is { Type: MessageType.MsgStorageAppendResp } persisted
                && persisted.Index == _index:
                Core(status.Id, $"received local MsgStorageAppendResp for index {_index}");
                break;
            case RaftTraceEventType.CommitAdvanced when status.Commit == _index:
                Core(status.Id, $"{status.Role} advanced Commit to {_index}");
                break;
        }
    }

    internal void Stored(ulong id, IReadOnlyList<Entry> entries, HardState? hardState, string source)
    {
        if (IncludesWrite(entries))
        {
            Host(id, $"stored entry {_index} ({source})");
        }

        if (_index != 0 && hardState?.Commit == _index)
        {
            Host(id, $"stored HardState.Commit={_index} ({source})");
        }
    }

    internal void Applied(CounterReplica replica, IReadOnlyList<Entry> entries)
    {
        if (IncludesWrite(entries))
        {
            Host(replica.Id, $"applied entry {_index}: counter={replica.Count}");
        }
    }

    internal void ReceivedApplicationWork(ulong id, IReadOnlyList<Entry> entries, string source)
    {
        if (IncludesWrite(entries))
        {
            Host(id, $"received application work for index {_index} ({source})");
        }
    }

    internal void AcknowledgedApplication(CounterReplica replica, IReadOnlyList<Entry> entries, string source)
    {
        if (IncludesWrite(entries))
        {
            Host(replica.Id, $"acknowledged application ({source}): Raft Applied={replica.Node.GetBasicStatus().Applied}");
        }
    }

    private bool IncludesWrite(IReadOnlyList<Entry> entries) =>
        _index != 0 && entries.Any(entry => entry.Index == _index);

    private static void Core(ulong id, string message) => Console.WriteLine($"  core node {id}: {message}");

    private static void Host(ulong id, string message) => Console.WriteLine($"  host node {id}: {message}");
}
