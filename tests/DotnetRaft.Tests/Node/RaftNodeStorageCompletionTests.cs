using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using static DotnetRaft.Tests.AsyncStorage.AsyncStorageTestSupport;
using static DotnetRaft.Tests.Node.RaftNodeTestSupport;

namespace DotnetRaft.Tests.Node;

public sealed class RaftNodeStorageCompletionTests
{
    [Theory]
    [InlineData(false, MessageType.MsgStorageAppendResp, RaftLocalMessageTargets.AppendThread)]
    [InlineData(true, MessageType.MsgStorageAppendResp, RaftLocalMessageTargets.AppendThread)]
    [InlineData(false, MessageType.MsgStorageApplyResp, 2UL)]
    [InlineData(true, MessageType.MsgStorageApplyResp, 2UL)]
    [InlineData(false, MessageType.MsgProp, RaftLocalMessageTargets.ApplyThread)]
    [InlineData(true, MessageType.MsgProp, RaftLocalMessageTargets.ApplyThread)]
    public async Task PeerStepRejectsStorageInputWithoutFaulting(
        bool asyncStorageWrites, MessageType type, ulong sender)
    {
        (RaftNode node, _) = RestartNode(asyncStorageWrites: asyncStorageWrites);
        using var guard = new CancellationTokenSource(DeadlockGuardTimeout);
        try
        {
            BasicStatus before = (await node.GetStatusAsync(guard.Token)).Basic;
            var input = new Message { Type = type, From = sender, To = before.Id, Term = type == MessageType.MsgProp ? 0UL : 99UL };

            await Assert.ThrowsAnyAsync<InvalidOperationException>(
                () => node.StepAsync(input, guard.Token).AsTask().WaitAsync(guard.Token));

            Assert.Equal(before, (await node.GetStatusAsync(guard.Token)).Basic);
            await node.CampaignAsync(guard.Token);
            Assert.Equal(RaftRole.Candidate, (await WaitReadyAsync(node)).SoftState?.Role);
        }
        finally
        {
            await StopAsync(node);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ForeignAndConsumedReceiptsDoNotFaultTheNode(bool useApplyReceipt)
    {
        // The two instances have the same ID and receive identical log work.
        (RaftNode issuer, MemoryStorage issuerStorage) = RestartNode(voters: [1, 2], asyncStorageWrites: true);
        (RaftNode receiver, MemoryStorage receiverStorage) = RestartNode(voters: [1, 2], asyncStorageWrites: true);
        using var guard = new CancellationTokenSource(DeadlockGuardTimeout);
        try
        {
            const ulong first = 3;
            StorageCompletion issued = await PrepareAsync(issuer, issuerStorage, first);
            StorageCompletion receiving = await PrepareAsync(receiver, receiverStorage, first);
            BasicStatus before = (await receiver.GetStatusAsync(guard.Token)).Basic;

            await RejectAsync(receiver, issued);
            Assert.Equal(before, (await receiver.GetStatusAsync(guard.Token)).Basic);
            await FinishAsync(receiver, receiving, first);
            await FinishAsync(issuer, issued, first);

            BasicStatus after = (await issuer.GetStatusAsync(guard.Token)).Basic;
            await RejectAsync(issuer, issued);
            Assert.Equal(after, (await issuer.GetStatusAsync(guard.Token)).Basic);
            StorageCompletion next = await PrepareAsync(issuer, issuerStorage, first + 1);
            await FinishAsync(issuer, next, first + 1);
        }
        finally
        {
            await Task.WhenAll(StopAsync(issuer), StopAsync(receiver));
        }

        Task RejectAsync(RaftNode node, StorageCompletion receipt) => Assert.ThrowsAnyAsync<InvalidOperationException>(
            () => node.CompleteStorageWorkAsync(receipt, guard.Token).AsTask().WaitAsync(guard.Token));

        async Task<StorageCompletion> PrepareAsync(RaftNode node, MemoryStorage storage, ulong index)
        {
            StorageCompletion append = await PublishEntryAsync(node, storage, index, guard.Token);
            if (!useApplyReceipt)
            {
                return append;
            }

            await node.CompleteStorageWorkAsync(append, guard.Token);
            return (await ObserveApplicationAsync(node, index)).Completion;
        }

        async Task FinishAsync(RaftNode node, StorageCompletion receipt, ulong index)
        {
            await node.CompleteStorageWorkAsync(receipt, guard.Token);
            if (!useApplyReceipt)
            {
                StorageApplyWork apply = await ObserveApplicationAsync(node, index);
                await node.CompleteStorageWorkAsync(apply.Completion, guard.Token);
            }

            Assert.Equal(index, (await node.GetStatusAsync(guard.Token)).Basic.Applied);
        }
    }

    [Fact]
    public async Task FailureAfterAValidReceiptIsClaimedStillFaultsTheNode()
    {
        var trace = new CallbackTraceSink
        {
            OnTrace = observed =>
            {
                if (observed.Type == RaftTraceEventType.MessageReceived
                    && observed.Message?.Type == MessageType.MsgStorageAppendResp)
                {
                    throw new InvalidOperationException("completion trace failed");
                }
            },
        };
        (RaftNode node, MemoryStorage storage) = RestartNode(voters: [1, 2], traceSink: trace, asyncStorageWrites: true);
        using var guard = new CancellationTokenSource(DeadlockGuardTimeout);
        try
        {
            StorageCompletion completion = await PublishEntryAsync(node, storage, 3, guard.Token);
            RaftTracingException failure = await Assert.ThrowsAsync<RaftTracingException>(
                () => node.CompleteStorageWorkAsync(completion, guard.Token).AsTask().WaitAsync(guard.Token));
            RaftNodeFaultedException terminal = await Assert.ThrowsAsync<RaftNodeFaultedException>(
                () => node.Completion.WaitAsync(guard.Token));
            Assert.Same(failure, terminal.InnerException);
        }
        finally
        {
            await StopAsync(node);
        }
    }

    private static async Task<StorageCompletion> PublishEntryAsync(
        RaftNode node, MemoryStorage storage, ulong index, CancellationToken cancellationToken)
    {
        var append = new Message
        {
            Type = MessageType.MsgApp,
            From = 2,
            To = 1,
            Term = 2,
            Index = index - 1,
            LogTerm = index == 3 ? 1UL : 2UL,
            Commit = index,
            Entries = { new Entry { Index = index, Term = 2, Data = RaftBytes.CopyFromUtf8("command") } },
        };
        await node.StepAsync(append, cancellationToken);
        StorageAppendWork work = Assert.IsType<StorageAppendWork>((await WaitReadyAsync(node)).AppendWork);
        PersistAppend(storage, work);
        Assert.Equal(index, storage.GetInitialState().HardState?.Commit);
        // The test's transport observes the reply without running the other peer.
        Assert.Equal(2UL, Assert.Single(work.MessagesAfterPersist).To);
        return Assert.IsAssignableFrom<StorageCompletion>(work.Completion);
    }

    private static async Task<StorageApplyWork> ObserveApplicationAsync(RaftNode node, ulong index)
    {
        StorageApplyWork work = Assert.IsType<StorageApplyWork>((await WaitReadyAsync(node)).ApplyWork);
        Entry entry = Assert.Single(work.Entries);
        Assert.Equal(index, entry.Index);
        Assert.Equal("command", entry.Data.ToStringUtf8());
        return work;
    }

    private static async Task StopAsync(RaftNode node)
    {
        try
        {
            await node.StopAsync().AsTask().WaitAsync(DeadlockGuardTimeout);
        }
        catch (RaftNodeFaultedException)
        {
            // Preserve the test's assertion when the owner is already faulted.
        }
    }
}
