using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using static DotnetRaft.Tests.AsyncStorage.AsyncStorageTestSupport;

using DotnetRawNode = DotnetRaft.RawNode;
using RaftTraceEventType =
    DotnetRaft.Diagnostics.RaftTraceEventType;
using RaftTracingException =
    DotnetRaft.Diagnostics.RaftTracingException;
using RecordingTraceSink =
    DotnetRaft.Tests.RawNode.RecordingTraceSink;

namespace DotnetRaft.Tests.AsyncStorage;

public sealed class RawNodeAsyncStorageTests
{
    [Fact]
    public void ElectionPersistenceUsesAppendWork()
    {
        (DotnetRawNode node, _) = CreateNode();

        node.Campaign();
        Ready ready = node.Ready();

        Assert.Equal(
            RaftRole.Candidate,
            ready.SoftState?.Role);
        Assert.Empty(ready.Entries);
        Assert.Empty(ready.CommittedEntries);
        Assert.Empty(ready.Messages);
        StorageAppendWork append = Assert.IsType<StorageAppendWork>(ready.AppendWork);
        Assert.Equal(new RaftHardState(1, 1, 2), append.HardState);
        Assert.Empty(append.Entries);
        Assert.Null(append.Completion);
        Message vote = Assert.Single(append.MessagesAfterPersist);
        Assert.Equal(MessageType.MsgVoteResp, vote.Type);
        Assert.Equal(1UL, vote.To);
        Assert.Throws<NotSupportedException>(
            () => node.Advance(ready));
    }

    [Fact]
    public async Task AppendReleasesPeerResponsesSeparatelyFromItsReceipt()
    {
        (DotnetRawNode node, MemoryStorage storage) = CreateNode();
        node.Campaign();
        await ProcessReadyAsync(
            node,
            storage,
            node.Ready());

        Ready ready = node.Ready();
        Entry entry = Assert.Single(ready.Entries);
        StorageAppendWork append = Assert.IsType<StorageAppendWork>(ready.AppendWork);
        Message response = Assert.Single(append.MessagesAfterPersist);
        Assert.Equal(MessageType.MsgAppResp, response.Type);
        Assert.Equal(1UL, response.From);
        Assert.Equal(1UL, response.To);
        Assert.Equal(1UL, response.Term);
        Assert.Equal(entry.Index, response.Index);
        Assert.NotNull(append.Completion);
    }

    [Fact]
    public async Task CommitOnlyAppendCanBeResponseFree()
    {
        (DotnetRawNode node, MemoryStorage storage) = CreateNode();
        node.Campaign();
        await ProcessReadyAsync(
            node,
            storage,
            node.Ready());
        Ready leadership = node.Ready();
        await ProcessReadyAsync(
            node,
            storage,
            leadership);

        Ready commit = node.Ready();

        StorageAppendWork append = Assert.IsType<StorageAppendWork>(commit.AppendWork);
        Assert.Equal(new RaftHardState(1, 1, 3), append.HardState);
        Assert.Empty(append.Entries);
        Assert.Empty(append.MessagesAfterPersist);
        Assert.Null(append.Completion);
        StorageApplyWork apply = Assert.IsType<StorageApplyWork>(commit.ApplyWork);
        Assert.Equal(3UL, Assert.Single(apply.Entries).Index);
    }

    [Fact]
    public async Task DiagnosticEntriesAndWorkerCompletionsDoNotAlias()
    {
        (DotnetRawNode node, MemoryStorage storage) = CreateNode();
        await BecomeLeaderAsync(node, storage);
        node.Propose("command"u8);
        Ready ready = node.Ready();
        StorageAppendWork append = Assert.IsType<StorageAppendWork>(ready.AppendWork);
        ulong index = Assert.Single(append.Entries).Index;

        ready.Entries[0].Index = index + 10;
        Assert.Equal(index, append.Entries[0].Index);
        await ProcessReadyAsync(node, storage, ready);
        Ready committed = node.Ready();
        StorageApplyWork apply = Assert.IsType<StorageApplyWork>(committed.ApplyWork);
        committed.CommittedEntries[0].Index = index + 11;
        Assert.Equal(index, apply.Entries[0].Index);
        apply.Entries[0].Index = index + 12;
        node.CompleteStorageWork(apply.Completion);
        Assert.Equal(index, node.GetBasicStatus().Applied);
    }

    [Fact]
    public async Task ReadyPipelinesNewUnstableEntries()
    {
        (DotnetRawNode node, MemoryStorage storage) = CreateNode();
        await BecomeLeaderAsync(node, storage);

        node.Propose("one"u8);
        Ready first = node.Ready();
        Entry firstEntry = Assert.Single(first.Entries);

        node.Propose("two"u8);
        Ready second = node.Ready();
        Entry secondEntry = Assert.Single(second.Entries);

        Assert.Equal(
            firstEntry.Index + 1,
            secondEntry.Index);
        Assert.Equal(
            RaftBytes.CopyFromUtf8("one"),
            firstEntry.Data);
        Assert.Equal(
            RaftBytes.CopyFromUtf8("two"),
            secondEntry.Data);
        Assert.Throws<NotSupportedException>(
            () => node.Advance(first));
    }

    [Fact]
    public async Task ApplyWaitsForStableAcknowledgement()
    {
        (DotnetRawNode node, MemoryStorage storage) = CreateNode();
        await BecomeLeaderAsync(node, storage);
        node.Propose("value"u8);
        Ready appendReady = node.Ready();
        StorageAppendWork append = Assert.IsType<StorageAppendWork>(appendReady.AppendWork);
        Message selfAppend = Assert.Single(append.MessagesAfterPersist);

        PersistAppend(storage, append);
        node.Step(selfAppend);
        Ready committedButUnstable = node.Ready();

        Assert.NotNull(committedButUnstable.AppendWork);
        Assert.Null(committedButUnstable.ApplyWork);

        node.CompleteStorageWork(Assert.IsAssignableFrom<StorageCompletion>(append.Completion));
        Ready applicable = node.Ready();

        StorageApplyWork apply = Assert.IsType<StorageApplyWork>(applicable.ApplyWork);
        Assert.Equal(
            RaftBytes.CopyFromUtf8("value"),
            Assert.Single(apply.Entries).Data);
    }

    [Fact]
    public async Task StorageMessagesAndDiagnosticsAreDetached()
    {
        (DotnetRawNode node, MemoryStorage storage) = CreateNode();
        await BecomeLeaderAsync(node, storage);
        node.Propose("value"u8);

        Ready ready = node.Ready();
        StorageAppendWork append = Assert.IsType<StorageAppendWork>(ready.AppendWork);
        ready.Entries[0].Data =
            RaftBytes.CopyFromUtf8("mutated");
        append.Entries[0].Data =
            RaftBytes.CopyFromUtf8("request");

        Assert.Equal(
            RaftBytes.CopyFromUtf8("request"),
            append.Entries[0].Data);
        Assert.NotEqual(
            ready.Entries[0].Data,
            append.Entries[0].Data);
    }

    [Fact]
    public async Task ApplyPaginationWaitsForEachReceipt()
    {
        (DotnetRawNode node, MemoryStorage storage) =
            CreateNode(
                maxCommittedSizePerReady: 1);
        await BecomeLeaderAsync(node, storage);
        node.Propose("a"u8);
        node.Propose("b"u8);
        node.Propose("c"u8);
        Ready appendReady = node.Ready();
        await ProcessReadyAsync(node, storage, appendReady);

        var indexes = new List<ulong>();
        for (var page = 0; page < 3; page++)
        {
            Ready ready = node.Ready();
            StorageApplyWork apply = Assert.IsType<StorageApplyWork>(ready.ApplyWork);
            Entry entry = Assert.Single(apply.Entries);
            indexes.Add(entry.Index);
            if (page < 2)
            {
                Assert.False(node.HasReady());
            }

            node.CompleteStorageWork(apply.Completion);
        }

        Assert.Equal(
            [4UL, 5UL, 6UL],
            indexes);
    }

    [Fact]
    public void SyntheticTracePrecedesReadyAcceptance()
    {
        var sink = new RecordingTraceSink();
        (DotnetRawNode node, _) =
            CreateNode(traceSink: sink);
        node.Campaign();
        sink.Events.Clear();

        _ = node.Ready();

        Assert.Equal(
        [
            RaftTraceEventType.MessageSent,
            RaftTraceEventType.ReadyAccepted,
        ],
            sink.Events.Select(
                traceEvent => traceEvent.Type));
        Assert.Equal(
            MessageType.MsgStorageAppend,
            sink.Events[0].Message?.Type);
    }

    [Fact]
    public void SyntheticTraceFailureDoesNotAcceptReady()
    {
        var sink = new RecordingTraceSink();
        (DotnetRawNode node, _) =
            CreateNode(traceSink: sink);
        node.Core.BecomeFollower(1, 0);
        node.Core.Log.Append(
        [
            new Entry
            {
                Index = 3,
                Term = 1,
            },
        ]);
        sink.OnTrace = traceEvent =>
        {
            if (traceEvent.Type
                    == RaftTraceEventType.MessageSent
                && traceEvent.Message?.Type
                    == MessageType.MsgStorageAppend)
            {
                throw new InvalidOperationException(
                    "trace failed");
            }
        };

        Assert.Throws<RaftTracingException>(
            () => node.Ready());

        Assert.Equal(
            3UL,
            node.Core.Log.Unstable.OffsetInProgress);
        Assert.True(node.IsFaultedForTesting);
    }
}
