using DotnetRaft.Protocol;
using DotnetRaft.Read;
using DotnetRaft.Storage;
using DotnetRaft.Tests.AsyncStorage;

using Google.Protobuf;

using static DotnetRaft.Tests.RawNode.RawNodeTestSupport;

namespace DotnetRaft.Tests.RawNode;

public sealed class RawNodeReadIndexTests
{
    [Theory]
    [InlineData(false, ReadOnlyOption.Safe)]
    [InlineData(true, ReadOnlyOption.Safe)]
    [InlineData(false, ReadOnlyOption.LeaseBased)]
    [InlineData(true, ReadOnlyOption.LeaseBased)]
    public async Task NewSingleVoterLeaderWaitsForCurrentTermCommitBeforeReadIndex(
        bool asyncStorageWrites,
        ReadOnlyOption readOnlyOption)
    {
        const ulong removalIndex = 2;
        const ulong writeIndex = 3;
        const ulong oldTerm = 1;
        var storage = new MemoryStorage();
        storage.ApplySnapshot(new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = removalIndex,
                Term = oldTerm,
                ConfState = new ConfState { Voters = { 1 } },
            },
        });
        // The old leader committed through the write before applying its own
        // removal. This replica applied the removal and saved the write, but
        // only learned the commit index through the removal (etcd-io/raft#535).
        storage.Append([EntryAt(writeIndex, oldTerm, "x")]);
        storage.SetHardState(new HardState { Term = oldTerm, Commit = removalIndex });
        var node = DotnetRaft.RawNode.Restart(CreateConfig(
            storage,
            applied: removalIndex,
            asyncStorageWrites: asyncStorageWrites,
            readOnlyOption: readOnlyOption,
            checkQuorum: readOnlyOption == ReadOnlyOption.LeaseBased));

        node.Campaign();
        await CompleteReadyAsync(node.Ready());
        BasicStatus leader = node.GetBasicStatus();
        Assert.Equal(RaftRole.Leader, leader.Role);
        Assert.Equal(oldTerm + 1, leader.Term);
        Assert.Equal(removalIndex, leader.Commit);

        node.ReadIndex("read-x"u8);
        Ready noOp = node.Ready();
        Entry currentTerm = Assert.Single(noOp.Entries);
        Assert.Equal(leader.Term, currentTerm.Term);
        Assert.True(currentTerm.Data.IsEmpty);
        Assert.Empty(noOp.ReadStates);

        await CompleteReadyAsync(noOp);

        Ready committed = node.Ready();
        ReadState read = Assert.Single(committed.ReadStates);
        Assert.Equal(currentTerm.Index, read.Index);
        Assert.Equal(ByteString.CopyFromUtf8("read-x"), read.RequestContext);
        Assert.Contains(committed.CommittedEntries, entry =>
            entry.Index == writeIndex && entry.Data.ToStringUtf8() == "x");
        Assert.DoesNotContain(committed.Messages, message => message.Type == MessageType.MsgHeartbeat);

        Task CompleteReadyAsync(Ready ready)
        {
            if (asyncStorageWrites)
            {
                return AsyncStorageTestSupport.ProcessReadyAsync(node, storage, ready);
            }

            PersistAndAdvance(node, storage, ready);
            return Task.CompletedTask;
        }
    }
}
