using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

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
    public void ElectionPersistenceUsesAppendRequest()
    {
        (DotnetRawNode node, _) = CreateNode();

        node.Campaign();
        Ready ready = node.Ready();

        Assert.Equal(
            RaftRole.Candidate,
            ready.SoftState?.Role);
        Assert.Empty(ready.Entries);
        Assert.Empty(ready.CommittedEntries);
        Message append = StorageMessage(
            ready,
            MessageType.MsgStorageAppend);
        Assert.Equal(
            RaftLocalMessageTargets.AppendThread,
            append.To);
        Assert.Equal(1UL, append.From);
        Assert.True(append.HasTerm);
        Assert.True(append.HasVote);
        Assert.True(append.HasCommit);
        Assert.Equal(1UL, append.Term);
        Assert.Equal(1UL, append.Vote);
        Assert.Equal(2UL, append.Commit);
        Message vote = Assert.Single(append.Responses);
        Assert.Equal(MessageType.MsgVoteResp, vote.Type);
        Assert.False(append.HasIndex);
        Assert.False(append.HasLogTerm);
        Assert.Throws<NotSupportedException>(
            () => node.Advance(ready));
    }

    [Fact]
    public async Task AppendResponsesPrecedeStorageAcknowledgement()
    {
        (DotnetRawNode node, MemoryStorage storage) = CreateNode();
        node.Campaign();
        await ProcessReadyAsync(
            node,
            storage,
            node.Ready());

        Ready ready = node.Ready();
        Entry entry = Assert.Single(ready.Entries);
        Message append = StorageMessage(
            ready,
            MessageType.MsgStorageAppend);

        Assert.Equal(
            [
                MessageType.MsgAppResp,
                MessageType.MsgStorageAppendResp,
            ],
            append.Responses.Select(
                response => response.Type));
        Message response = append.Responses[^1];
        Assert.Equal(
            RaftLocalMessageTargets.AppendThread,
            response.From);
        Assert.Equal(1UL, response.To);
        Assert.True(response.HasTerm);
        Assert.Equal(1UL, response.Term);
        Assert.True(response.HasIndex);
        Assert.True(response.HasLogTerm);
        Assert.Equal(entry.Index, response.Index);
        Assert.Equal(entry.Term, response.LogTerm);
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

        Message append = StorageMessage(
            commit,
            MessageType.MsgStorageAppend);
        Assert.True(append.HasTerm);
        Assert.Equal(3UL, append.Commit);
        Assert.Empty(append.Entries);
        Assert.Empty(append.Responses);
        Message apply = StorageMessage(
            commit,
            MessageType.MsgStorageApply);
        Assert.True(apply.HasTerm);
        Assert.Equal(0UL, apply.Term);
        Message applyResponse =
            Assert.Single(apply.Responses);
        Assert.True(applyResponse.HasTerm);
        Assert.Equal(0UL, applyResponse.Term);
        Assert.Single(applyResponse.Entries);
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
            ByteString.CopyFromUtf8("one"),
            firstEntry.Data);
        Assert.Equal(
            ByteString.CopyFromUtf8("two"),
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
        Message append = StorageMessage(
            appendReady,
            MessageType.MsgStorageAppend);
        Message selfAppend = append.Responses[0];
        Message stable = append.Responses[1];

        PersistAppend(storage, append);
        node.Step(selfAppend);
        Ready committedButUnstable = node.Ready();

        Assert.Single(
            committedButUnstable.Messages,
            message =>
                message.Type
                == MessageType.MsgStorageAppend);
        Assert.DoesNotContain(
            committedButUnstable.Messages,
            message =>
                message.Type
                == MessageType.MsgStorageApply);

        node.Step(stable);
        Ready applicable = node.Ready();

        Message apply = StorageMessage(
            applicable,
            MessageType.MsgStorageApply);
        Assert.Equal(
            ByteString.CopyFromUtf8("value"),
            Assert.Single(apply.Entries).Data);
    }

    [Fact]
    public async Task StorageMessagesAndDiagnosticsAreDetached()
    {
        (DotnetRawNode node, MemoryStorage storage) = CreateNode();
        await BecomeLeaderAsync(node, storage);
        node.Propose("value"u8);

        Ready ready = node.Ready();
        Message append = StorageMessage(
            ready,
            MessageType.MsgStorageAppend);
        ready.Entries[0].Data =
            ByteString.CopyFromUtf8("mutated");
        append.Entries[0].Data =
            ByteString.CopyFromUtf8("request");

        Assert.Equal(
            ByteString.CopyFromUtf8("request"),
            append.Entries[0].Data);
        Assert.NotEqual(
            ready.Entries[0].Data,
            append.Entries[0].Data);
    }

    [Fact]
    public async Task ApplyPaginationWaitsForEachResponse()
    {
        (DotnetRawNode node, MemoryStorage storage) =
            CreateNode(
                maxCommittedSizePerReady: 1);
        await BecomeLeaderAsync(node, storage);
        node.Propose("a"u8);
        node.Propose("b"u8);
        node.Propose("c"u8);
        Ready appendReady = node.Ready();
        Message append = StorageMessage(
            appendReady,
            MessageType.MsgStorageAppend);
        Message[] responses =
            PersistAppend(storage, append);
        foreach (Message response in responses)
        {
            node.Step(response);
        }

        var indexes = new List<ulong>();
        for (var page = 0; page < 3; page++)
        {
            Ready ready = node.Ready();
            Message apply = StorageMessage(
                ready,
                MessageType.MsgStorageApply);
            Entry entry = Assert.Single(apply.Entries);
            indexes.Add(entry.Index);
            if (page < 2)
            {
                Assert.False(node.HasReady());
            }

            node.Step(
                Assert.Single(
                    apply.Responses));
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
