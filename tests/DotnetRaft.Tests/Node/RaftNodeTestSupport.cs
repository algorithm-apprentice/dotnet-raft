using System.Collections.Concurrent;

using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Node;

internal static class RaftNodeTestSupport
{
    internal static (
        RaftNode Node,
        MemoryStorage Storage) RestartNode(
        IEnumerable<ulong>? voters = null,
        IEnumerable<ulong>? learners = null,
        int electionTick = 10,
        ulong maxCommittedSizePerReady = 0,
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
            .WaitAsync(TimeSpan.FromSeconds(2));
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
    private int _blocked;

    internal ManualResetEventSlim Entered { get; } =
        new(false);

    internal ManualResetEventSlim Release { get; } =
        new(false);

    public void Trace(RaftTraceEvent traceEvent)
    {
        if (traceEvent.Type
                != RaftTraceEventType.MessageReceived
            || Interlocked.Exchange(ref _blocked, 1) != 0)
        {
            return;
        }

        Entered.Set();
        if (!Release.Wait(TimeSpan.FromSeconds(2)))
        {
            throw new TimeoutException(
                "Blocking trace sink was not released.");
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

    public bool IsEnabled(RaftLogLevel level)
    {
        return true;
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
    }
}
