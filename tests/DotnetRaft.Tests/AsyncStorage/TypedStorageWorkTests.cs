using DotnetRaft.Protocol;

namespace DotnetRaft.Tests.AsyncStorage;

public sealed class TypedStorageWorkTests
{
    [Fact]
    public async Task AppendAndApplyReceiptsAreOrderedAndBoundToTheIssuer()
    {
        var (node, storage) = AsyncStorageTestSupport.CreateNode();
        await AsyncStorageTestSupport.BecomeLeaderAsync(node, storage);
        var (replacement, _) = AsyncStorageTestSupport.CreateNode();
        node.Propose("typed"u8);

        Ready ready = node.Ready();
        var append = Assert.IsType<StorageAppendWork>(ready.AppendWork);
        StorageCompletion written = Assert.IsAssignableFrom<StorageCompletion>(append.Completion);
        Assert.Empty(ready.Messages);
        Assert.Null(ready.ApplyWork);
        Assert.Throws<InvalidOperationException>(() => replacement.CompleteStorageWork(written));

        // The Ready mirror is detached from the actual work and its receipt.
        Assert.Single(ready.Entries).Index = 99;
        Entry durable = Assert.Single(append.Entries);
        storage.Append(append.Entries);
        foreach (Message response in append.MessagesAfterPersist)
        {
            node.Step(response);
        }

        node.CompleteStorageWork(written);
        Assert.Throws<InvalidOperationException>(() => node.CompleteStorageWork(written));

        Ready committed = node.Ready();
        var apply = Assert.IsType<StorageApplyWork>(committed.ApplyWork);
        Assert.Equal(durable.Index, Assert.Single(apply.Entries).Index);
        node.CompleteStorageWork(apply.Completion);
        Assert.Equal(durable.Index, node.GetBasicStatus().Applied);
        Assert.Throws<InvalidOperationException>(() => node.CompleteStorageWork(apply.Completion));
    }

    [Fact]
    public void PeerStepCannotAcknowledgeAStorageWorker()
    {
        var (node, _) = AsyncStorageTestSupport.CreateNode();
        Assert.Throws<InvalidOperationException>(() => node.Step(new Message
        {
            Type = MessageType.MsgStorageAppendResp,
            From = RaftLocalMessageTargets.AppendThread,
            To = 1,
            Term = 1,
        }));
        Assert.False(node.HasReady());
    }
}
