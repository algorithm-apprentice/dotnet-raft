using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Read;
using DotnetRaft.Storage;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.RawNode;

internal static class RawNodeTestSupport
{
    internal static MemoryStorage CreateStorage(
        IEnumerable<ulong>? voters = null,
        IEnumerable<ulong>? learners = null)
    {
        var confState = new ConfState();
        confState.Voters.Add(voters ?? [1UL]);
        if (learners is not null)
        {
            confState.Learners.Add(learners);
        }

        var storage = new MemoryStorage();
        storage.ApplySnapshot(new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                ConfState = confState,
            },
        });
        return storage;
    }

    internal static MemoryStorage CreateEmptyStorage()
    {
        return new MemoryStorage();
    }

    internal static RaftConfig CreateConfig(
        IStorage storage,
        ulong id = 1,
        ulong applied = 0,
        int electionTick = 10,
        int heartbeatTick = 1,
        ulong maxCommittedSizePerReady = 0,
        ulong maxUncommittedEntriesSize = 0,
        bool asyncStorageWrites = false,
        bool checkQuorum = false,
        bool preVote = false,
        ReadOnlyOption readOnlyOption = ReadOnlyOption.Safe,
        IRaftTraceSink? traceSink = null,
        bool disableConfChangeValidation = false)
    {
        return new RaftConfig
        {
            Id = id,
            Storage = storage,
            Applied = applied,
            ElectionTick = electionTick,
            HeartbeatTick = heartbeatTick,
            MaxCommittedSizePerReady =
                maxCommittedSizePerReady,
            MaxUncommittedEntriesSize =
                maxUncommittedEntriesSize,
            AsyncStorageWrites = asyncStorageWrites,
            CheckQuorum = checkQuorum,
            PreVote = preVote,
            ReadOnlyOption = readOnlyOption,
            TraceSink = traceSink,
            DisableConfChangeValidation =
                disableConfChangeValidation,
        };
    }

    internal static DotnetRaft.RawNode CreateNode(
        IStorage storage,
        ulong id = 1,
        ulong applied = 0,
        int electionTick = 10,
        int heartbeatTick = 1,
        ulong maxCommittedSizePerReady = 0,
        ulong maxUncommittedEntriesSize = 0,
        bool asyncStorageWrites = false,
        bool checkQuorum = false,
        bool preVote = false,
        ReadOnlyOption readOnlyOption = ReadOnlyOption.Safe,
        IRaftTraceSink? traceSink = null,
        bool disableConfChangeValidation = false)
    {
        return new DotnetRaft.RawNode(CreateConfig(
            storage,
            id,
            applied,
            electionTick,
            heartbeatTick,
            maxCommittedSizePerReady,
            maxUncommittedEntriesSize,
            asyncStorageWrites,
            checkQuorum,
            preVote,
            readOnlyOption,
            traceSink,
            disableConfChangeValidation));
    }

    internal static void Persist(
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
    }

    internal static void PersistAndAdvance(
        DotnetRaft.RawNode node,
        MemoryStorage storage,
        Ready ready)
    {
        Persist(storage, ready);
        node.Advance(ready);
    }

    internal static void BecomeSingletonLeader(
        DotnetRaft.RawNode node,
        MemoryStorage storage)
    {
        node.Campaign();

        Ready election = node.Ready();
        Assert.Equal(
            RaftRole.Candidate,
            election.SoftState?.Role);
        PersistAndAdvance(node, storage, election);

        Ready leadership = node.Ready();
        Assert.Equal(
            RaftRole.Leader,
            leadership.SoftState?.Role);
        Assert.Single(leadership.Entries);
        PersistAndAdvance(node, storage, leadership);

        Ready commit = node.Ready();
        Assert.Single(commit.CommittedEntries);
        PersistAndAdvance(node, storage, commit);

        Assert.False(node.HasReady());
    }

    internal static void BecomeTwoVoterLeader(
        DotnetRaft.RawNode node,
        MemoryStorage storage,
        ulong peerId = 2)
    {
        node.Campaign();

        Ready election = node.Ready();
        Message voteRequest = Assert.Single(
            election.Messages,
            message =>
                message.Type == MessageType.MsgVote
                && message.To == peerId);
        PersistAndAdvance(node, storage, election);

        node.Step(new Message
        {
            From = peerId,
            To = 1,
            Term = voteRequest.Term,
            Type = MessageType.MsgVoteResp,
        });

        Ready leadership = node.Ready();
        Message append = Assert.Single(
            leadership.Messages,
            message =>
                message.Type == MessageType.MsgApp
                && message.To == peerId);
        PersistAndAdvance(node, storage, leadership);

        ulong acknowledgedIndex = append.Entries.Count == 0
            ? append.Index
            : append.Entries[^1].Index;
        node.Step(new Message
        {
            From = peerId,
            To = 1,
            Term = append.Term,
            Type = MessageType.MsgAppResp,
            Index = acknowledgedIndex,
        });

        Drain(node, storage);
        Assert.Equal(RaftRole.Leader, node.Core.Role);
        Assert.False(node.HasReady());
    }

    internal static void Drain(
        DotnetRaft.RawNode node,
        MemoryStorage storage)
    {
        int remaining = 20;
        while (node.HasReady())
        {
            Assert.True(
                remaining-- > 0,
                "RawNode did not become idle.");
            Ready ready = node.Ready();
            PersistAndAdvance(node, storage, ready);
        }
    }

    internal static Entry EntryAt(
        ulong index,
        ulong term,
        string? data = null,
        EntryType type = EntryType.EntryNormal)
    {
        return new Entry
        {
            Index = index,
            Term = term,
            Type = type,
            Data = data is null
                ? ByteString.Empty
                : ByteString.CopyFromUtf8(data),
        };
    }
}

internal sealed class FaultingStorage(
    MemoryStorage inner) : IStorage
{
    internal bool ThrowOnGetTerm { get; set; }

    public StorageState GetInitialState()
    {
        return inner.GetInitialState();
    }

    public IReadOnlyList<Entry> GetEntries(
        ulong lowInclusive,
        ulong highExclusive,
        ulong maxSize)
    {
        return inner.GetEntries(
            lowInclusive,
            highExclusive,
            maxSize);
    }

    public ulong GetTerm(ulong index)
    {
        if (ThrowOnGetTerm)
        {
            throw new StorageException(
                StorageError.Unavailable,
                "Injected term failure.");
        }

        return inner.GetTerm(index);
    }

    public ulong GetLastIndex()
    {
        return inner.GetLastIndex();
    }

    public ulong GetFirstIndex()
    {
        return inner.GetFirstIndex();
    }

    public Snapshot GetSnapshot()
    {
        return inner.GetSnapshot();
    }
}

internal sealed class OrderedApplicationHarness
{
    private readonly HashSet<ulong> _decidedConfigurations = [];

    internal ulong PhysicalApplied { get; private set; }

    internal ConfState? LatestConfState { get; private set; }

    internal void AcceptConfiguration(
        DotnetRaft.RawNode node,
        Entry entry)
    {
        LatestConfState = entry.Type switch
        {
            EntryType.EntryConfChange =>
                node.ApplyConfChange(
                    ProtocolConfChange.Parser.ParseFrom(
                        entry.Data)),
            EntryType.EntryConfChangeV2 =>
                node.ApplyConfChange(
                    ConfChangeV2.Parser.ParseFrom(entry.Data)),
            _ => throw new ArgumentException(
                "Expected a configuration entry.",
                nameof(entry)),
        };
        _decidedConfigurations.Add(entry.Index);
    }

    internal void RejectConfiguration(Entry entry)
    {
        if (entry.Type is not (
                EntryType.EntryConfChange
                or EntryType.EntryConfChangeV2))
        {
            throw new ArgumentException(
                "Expected a configuration entry.",
                nameof(entry));
        }

        _decidedConfigurations.Add(entry.Index);
    }

    internal void Advance(
        DotnetRaft.RawNode node,
        Ready ready)
    {
        foreach (Entry entry in ready.CommittedEntries)
        {
            if (entry.Type is (
                    EntryType.EntryConfChange
                    or EntryType.EntryConfChangeV2)
                && !_decidedConfigurations.Contains(entry.Index))
            {
                throw new InvalidOperationException(
                    $"Configuration entry {entry.Index} has no ordered decision.");
            }
        }

        node.Advance(ready);
        if (ready.Snapshot is not null)
        {
            PhysicalApplied = ready.Snapshot.Metadata.Index;
        }

        if (ready.CommittedEntries.Count > 0)
        {
            PhysicalApplied = ready.CommittedEntries[^1].Index;
        }
    }

    internal Snapshot CreateSnapshot(
        MemoryStorage storage,
        ulong index,
        ByteString data)
    {
        if (index > PhysicalApplied)
        {
            throw new InvalidOperationException(
                "Application snapshots cannot pass physical application.");
        }

        return storage.CreateSnapshot(
            index,
            LatestConfState,
            data);
    }

    internal void Compact(
        MemoryStorage storage,
        ulong index)
    {
        if (index > PhysicalApplied)
        {
            throw new InvalidOperationException(
                "Compaction cannot pass physical application.");
        }

        ulong snapshotIndex =
            storage.GetSnapshot().Metadata.Index;
        if (index > snapshotIndex)
        {
            throw new InvalidOperationException(
                "Compaction cannot pass the durable application snapshot.");
        }

        storage.Compact(index);
    }
}

internal sealed class RecordingTraceSink : IRaftTraceSink
{
    internal List<RaftTraceEvent> Events { get; } = [];

    internal Action<RaftTraceEvent>? OnTrace { get; set; }

    public void Trace(RaftTraceEvent traceEvent)
    {
        Events.Add(traceEvent);
        OnTrace?.Invoke(traceEvent);
    }
}
