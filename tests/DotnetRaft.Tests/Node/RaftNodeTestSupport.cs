using System.Collections.Concurrent;

using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Node;

internal static class RaftNodeTestSupport
{
    internal static TimeSpan DeadlockGuardTimeout { get; } =
        TimeSpan.FromSeconds(10);

    internal static (
        RaftNode Node,
        MemoryStorage Storage) RestartNode(
        IEnumerable<ulong>? voters = null,
        IEnumerable<ulong>? learners = null,
        int electionTick = 10,
        ulong maxCommittedSizePerReady = 0,
        ulong maxUncommittedEntriesSize = 0,
        IRaftTraceSink? traceSink = null,
        IRaftLogger? logger = null)
    {
        MemoryStorage storage = CreateStorage(
            voters ?? [1UL],
            learners);
        var config = new RaftConfig
        {
            Id = 1,
            ElectionTick = electionTick,
            HeartbeatTick = 1,
            Storage = storage,
            Applied = 2,
            MaxSizePerMessage = ulong.MaxValue,
            MaxCommittedSizePerReady =
                maxCommittedSizePerReady,
            MaxUncommittedEntriesSize =
                maxUncommittedEntriesSize,
            MaxInflightMessages = 256,
            TraceSink = traceSink,
            Logger = logger,
        };
        return (
            RaftNode.Restart(config),
            storage);
    }

    internal static MemoryStorage CreateStorage(
        IEnumerable<ulong> voters,
        IEnumerable<ulong>? learners = null)
    {
        var state = new ConfState();
        state.Voters.Add(voters);
        if (learners is not null)
        {
            state.Learners.Add(learners);
        }

        var storage = new MemoryStorage();
        storage.ApplySnapshot(
            new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = 2,
                    Term = 1,
                    ConfState = state,
                },
            });
        return storage;
    }

    internal static async Task<Ready> WaitReadyAsync(
        RaftNode node)
    {
        return await node.WaitForReadyAsync()
            .AsTask()
            .WaitAsync(DeadlockGuardTimeout);
    }

    internal static async Task PersistAndAdvanceAsync(
        RaftNode node,
        MemoryStorage storage,
        Ready ready)
    {
        if (ready.Snapshot is not null)
        {
            storage.ApplySnapshot(ready.Snapshot);
        }

        storage.Append(ready.Entries);
        if (ready.HardState is not null)
        {
            storage.SetHardState(ready.HardState);
        }

        foreach (Entry entry in ready.CommittedEntries)
        {
            switch (entry.Type)
            {
                case EntryType.EntryConfChange:
                    await node.ApplyConfChangeAsync(
                        ProtocolConfChange.Parser.ParseFrom(
                            entry.Data));
                    break;
                case EntryType.EntryConfChangeV2:
                    await node.ApplyConfChangeAsync(
                        ConfChangeV2.Parser.ParseFrom(
                            entry.Data));
                    break;
            }
        }

        await node.AdvanceAsync();
    }

    internal static async Task BecomeSingletonLeaderAsync(
        RaftNode node,
        MemoryStorage storage)
    {
        await node.CampaignAsync();

        Ready election = await WaitReadyAsync(node);
        Assert.Equal(
            RaftRole.Candidate,
            election.SoftState?.Role);
        await PersistAndAdvanceAsync(
            node,
            storage,
            election);

        Ready leadership = await WaitReadyAsync(node);
        Assert.Equal(
            RaftRole.Leader,
            leadership.SoftState?.Role);
        await PersistAndAdvanceAsync(
            node,
            storage,
            leadership);

        Ready commit = await WaitReadyAsync(node);
        Assert.Single(commit.CommittedEntries);
        await PersistAndAdvanceAsync(
            node,
            storage,
            commit);
    }
}

internal sealed class CallbackTraceSink : IRaftTraceSink
{
    internal Action<RaftTraceEvent>? OnTrace { get; set; }

    public void Trace(RaftTraceEvent traceEvent)
    {
        OnTrace?.Invoke(traceEvent);
    }
}

internal sealed class BlockingTraceSink : IRaftTraceSink
{
    private readonly Func<RaftTraceEvent, bool>
        _shouldBlock;
    private readonly Action<RaftTraceEvent>?
        _afterRelease;
    private readonly Action<RaftTraceEvent>?
        _onTrace;
    private int _blocked;

    internal BlockingTraceSink(
        Func<RaftTraceEvent, bool>? shouldBlock = null,
        Action<RaftTraceEvent>? afterRelease = null,
        Action<RaftTraceEvent>? onTrace = null)
    {
        _shouldBlock =
            shouldBlock
            ?? (traceEvent =>
                traceEvent.Type
                == RaftTraceEventType.MessageReceived);
        _afterRelease = afterRelease;
        _onTrace = onTrace;
    }

    internal ConcurrentQueue<RaftTraceEvent> Events
    {
        get;
    } = new();

    internal ManualResetEventSlim Entered { get; } =
        new(false);

    internal ManualResetEventSlim Release { get; } =
        new(false);

    internal ManualResetEventSlim Exited { get; } =
        new(false);

    public void Trace(RaftTraceEvent traceEvent)
    {
        Events.Enqueue(traceEvent);
        _onTrace?.Invoke(traceEvent);
        if (!_shouldBlock(traceEvent)
            || Interlocked.Exchange(ref _blocked, 1) != 0)
        {
            return;
        }

        Entered.Set();
        try
        {
            Release.Wait();
            _afterRelease?.Invoke(traceEvent);
        }
        finally
        {
            Exited.Set();
        }
    }
}

internal sealed class RecordingLogger : IRaftLogger
{
    internal ConcurrentQueue<(
        RaftLogLevel Level,
        string Message,
        int ThreadId)> Events
    {
        get;
    } = new();

    internal ManualResetEventSlim WarningWritten { get; } =
        new(false);

    internal bool ThrowOnWarning { get; set; }

    internal bool ThrowOnInformation { get; set; }

    internal Exception? InformationFailure { get; set; }

    internal bool WarningEnabled { get; set; } = true;

    public bool IsEnabled(RaftLogLevel level)
    {
        return level != RaftLogLevel.Warning
            || WarningEnabled;
    }

    public void Log(
        RaftLogLevel level,
        string message)
    {
        Events.Enqueue(
            (level, message, Environment.CurrentManagedThreadId));
        if (level == RaftLogLevel.Warning)
        {
            WarningWritten.Set();
            if (ThrowOnWarning)
            {
                throw new InvalidOperationException(
                    "Injected logger failure.");
            }
        }

        if (level == RaftLogLevel.Information
            && InformationFailure is not null)
        {
            throw InformationFailure;
        }

        if (level == RaftLogLevel.Information
            && ThrowOnInformation)
        {
            throw new InvalidOperationException(
                "Injected information logger failure.");
        }
    }
}
