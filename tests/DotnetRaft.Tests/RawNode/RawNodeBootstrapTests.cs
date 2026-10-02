using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using static DotnetRaft.Tests.RawNode.RawNodeTestSupport;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.RawNode;

public sealed class RawNodeBootstrapTests
{
    [Fact]
    public void BootstrapProducesPinnedTermOneEntries()
    {
        MemoryStorage storage = CreateEmptyStorage();
        var node = CreateNode(storage, id: 2);
        Peer[] peers =
        [
            new Peer(
                2,
                ByteString.CopyFromUtf8("local")),
            new Peer(
                1,
                ByteString.CopyFromUtf8("remote")),
        ];

        node.Bootstrap(peers);

        Ready ready = node.Ready();
        Assert.Equal(
            new HardState
            {
                Term = 1,
                Commit = 2,
            },
            ready.HardState);
        Assert.True(ready.MustSync);
        Assert.Empty(ready.Messages);
        Assert.Equal(2, ready.Entries.Count);
        Assert.Equal(2, ready.CommittedEntries.Count);
        for (var offset = 0; offset < peers.Length; offset++)
        {
            Entry entry = ready.Entries[offset];
            Assert.Equal(1UL, entry.Term);
            Assert.Equal((ulong)offset + 1, entry.Index);
            Assert.Equal(
                EntryType.EntryConfChange,
                entry.Type);

            ProtocolConfChange change =
                ProtocolConfChange.Parser.ParseFrom(
                    entry.Data);
            Assert.Equal(
                ConfChangeType.ConfChangeAddNode,
                change.Type);
            Assert.Equal(peers[offset].Id, change.NodeId);
            Assert.Equal(
                peers[offset].Context,
                change.Context);
            Assert.Equal(
                ready.CommittedEntries[offset],
                entry);
        }

        Status status = node.GetStatus();
        Assert.Equal([1UL, 2UL], status.Configuration.Voters);
        node.VisitProgress((_, progress) =>
            Assert.Equal(2UL, progress.Next));
    }

    [Fact]
    public void BootstrapConfigurationBytesMatchPinnedPresence()
    {
        var node = CreateNode(CreateEmptyStorage());
        node.Bootstrap(
        [
            new Peer(1),
            new Peer(
                2,
                ByteString.CopyFromUtf8("ctx")),
        ]);

        Ready ready = node.Ready();

        Assert.Equal(
            ByteString.CopyFrom(
            [
                0x10,
                0x00,
                0x18,
                0x01,
            ]),
            ready.Entries[0].Data);
        Assert.Equal(
            ByteString.CopyFrom(
            [
                0x10,
                0x00,
                0x18,
                0x02,
                0x22,
                0x03,
                (byte)'c',
                (byte)'t',
                (byte)'x',
            ]),
            ready.Entries[1].Data);
    }

    [Fact]
    public void BootstrapCommittedEntriesRespectPagination()
    {
        MemoryStorage storage = CreateEmptyStorage();
        var node = CreateNode(
            storage,
            maxCommittedSizePerReady: 1);
        node.Bootstrap(
        [
            new Peer(1),
            new Peer(2),
            new Peer(3),
        ]);

        var appliedIndexes = new List<ulong>();
        var page = 0;
        while (node.HasReady())
        {
            Ready ready = node.Ready();
            page++;
            Assert.Single(ready.CommittedEntries);
            if (page == 1)
            {
                Assert.Equal(3, ready.Entries.Count);
                Assert.NotNull(ready.HardState);
                Assert.True(ready.MustSync);
            }
            else
            {
                Assert.Empty(ready.Entries);
                Assert.Null(ready.HardState);
                Assert.False(ready.MustSync);
            }

            Persist(storage, ready);
            foreach (Entry entry in ready.CommittedEntries)
            {
                ProtocolConfChange change =
                    ProtocolConfChange.Parser.ParseFrom(
                        entry.Data);
                node.ApplyConfChange(change);
                appliedIndexes.Add(entry.Index);
            }

            node.Advance(ready);
        }

        Assert.Equal([1UL, 2UL, 3UL], appliedIndexes);
        Assert.Equal(3, page);

        node.Campaign();
        Assert.Equal(
            RaftRole.Candidate,
            node.Ready().SoftState?.Role);
    }

    [Fact]
    public void InvalidPeersLeaveFreshNodeRetryable()
    {
        var node = CreateNode(CreateEmptyStorage());

        Assert.ThrowsAny<ArgumentException>(
            () => node.Bootstrap([]));
        Assert.ThrowsAny<ArgumentException>(
            () => node.Bootstrap([null!]));
        Assert.ThrowsAny<ArgumentException>(
            () => node.Bootstrap([new Peer(1), new Peer(1)]));

        node.Bootstrap([new Peer(1)]);

        Assert.True(node.HasReady());
        Assert.Equal(1UL, node.GetBasicStatus().Commit);
    }

    [Theory]
    [InlineData(0UL)]
    [InlineData(ulong.MaxValue)]
    [InlineData(ulong.MaxValue - 1)]
    public void InvalidPeerIdsLeaveFreshNodeRetryable(
        ulong id)
    {
        var node = CreateNode(CreateEmptyStorage());

        Assert.ThrowsAny<ArgumentException>(
            () => node.Bootstrap([new Peer(id)]));

        node.Bootstrap([new Peer(1)]);
        Assert.True(node.HasReady());
    }

    [Fact]
    public void StatusInspectionPreservesBootstrapFreshness()
    {
        var node = CreateNode(CreateEmptyStorage());

        Assert.False(node.HasReady());
        Assert.Equal(0UL, node.GetBasicStatus().Term);
        Assert.Empty(node.GetStatus().Configuration.Voters);
        node.VisitProgress((_, _) =>
            throw new InvalidOperationException(
                "No progress should exist."));

        node.Bootstrap([new Peer(1)]);

        Assert.True(node.HasReady());
    }

    [Fact]
    public void PriorStateMachineUsePermanentlyRejectsBootstrap()
    {
        var node = CreateNode(CreateEmptyStorage());
        node.Tick();

        Assert.Throws<InvalidOperationException>(
            () => node.Bootstrap([new Peer(1)]));
        Assert.Throws<InvalidOperationException>(
            () => node.HasReady());
        Assert.Equal(0UL, node.GetBasicStatus().Term);
    }

    [Fact]
    public void ExistingLogFaultsBootstrapFacade()
    {
        MemoryStorage storage = CreateEmptyStorage();
        storage.Append([EntryAt(1, 1)]);
        var node = CreateNode(storage);

        Assert.Throws<InvalidOperationException>(
            () => node.Bootstrap([new Peer(1)]));

        AssertFaultedButInspectable(node);
    }

    [Fact]
    public void ExistingHardStateFaultsBootstrapFacade()
    {
        MemoryStorage storage = CreateEmptyStorage();
        storage.SetHardState(new HardState
        {
            Term = 1,
        });
        var node = CreateNode(storage);

        Assert.Throws<InvalidOperationException>(
            () => node.Bootstrap([new Peer(1)]));

        AssertFaultedButInspectable(node);
    }

    [Fact]
    public void ExistingMembershipFaultsBootstrapFacade()
    {
        var node = CreateNode(
            CreateStorage(voters: [1]));

        Assert.Throws<InvalidOperationException>(
            () => node.Bootstrap([new Peer(1)]));

        AssertFaultedButInspectable(node);
    }

    [Fact]
    public void StartValidatesPeersBeforeConstructionTrace()
    {
        var sink = new RecordingTraceSink();
        RaftConfig config = CreateConfig(
            CreateEmptyStorage(),
            traceSink: sink);

        Assert.ThrowsAny<ArgumentException>(
            () => DotnetRaft.RawNode.Start(
                config,
                []));

        Assert.Empty(sink.Events);
    }

    [Fact]
    public void StartReturnsBootstrappedNode()
    {
        var sink = new RecordingTraceSink();
        MemoryStorage storage = CreateEmptyStorage();

        DotnetRaft.RawNode node =
            DotnetRaft.RawNode.Start(
                CreateConfig(
                    storage,
                    traceSink: sink),
                [new Peer(1)]);

        Assert.Equal(
            RaftTraceEventType.Initialized,
            sink.Events[0].Type);
        Ready ready = node.Ready();
        Assert.Single(ready.Entries);
        Assert.Single(ready.CommittedEntries);
        Assert.Equal(1UL, ready.HardState?.Term);
    }

    [Fact]
    public void RestartPaginatesExistingCommittedWork()
    {
        MemoryStorage storage =
            CreateStorage(voters: [1]);
        storage.Append(
        [
            EntryAt(1, 1, "one"),
            EntryAt(2, 1, "two"),
            EntryAt(3, 1, "three"),
        ]);
        storage.SetHardState(new HardState
        {
            Term = 1,
            Vote = 1,
            Commit = 3,
        });
        DotnetRaft.RawNode node =
            DotnetRaft.RawNode.Restart(
                CreateConfig(
                    storage,
                    maxCommittedSizePerReady: 1));

        var indexes = new List<ulong>();
        while (node.HasReady())
        {
            Ready ready = node.Ready();
            Entry entry =
                Assert.Single(ready.CommittedEntries);
            indexes.Add(entry.Index);
            Assert.Empty(ready.Entries);
            Assert.Null(ready.HardState);
            Assert.False(ready.MustSync);
            node.Advance(ready);
        }

        Assert.Equal([1UL, 2UL, 3UL], indexes);
    }

    private static void AssertFaultedButInspectable(
        DotnetRaft.RawNode node)
    {
        Assert.Throws<InvalidOperationException>(
            () => node.HasReady());
        _ = node.GetBasicStatus();
        _ = node.GetStatus();
        node.VisitProgress((_, _) => { });
    }
}
