using DotnetRaft.Protocol;

namespace DotnetRaft.Tests.Interaction;

public sealed class InteractionAsyncStorageTests
{
    [Fact]
    public void StabilizeDrainsAsyncWorkers()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                Voters = [1],
                SnapshotIndex = 2,
                AsyncStorageWrites = true,
            });
        environment.Campaign(0);

        environment.Stabilize();

        InteractionNode node =
            environment.Nodes[0];
        Assert.Equal(
            RaftRole.Leader,
            node.RawNode.GetBasicStatus().Role);
        Assert.Equal(
            3UL,
            node.GetApplicationSnapshot()
                .Metadata.Index);
        Assert.Equal(0, node.AppendWorkCount);
        Assert.Equal(0, node.ApplyWorkCount);
    }

    [Fact]
    public void ProcessReadyRoutesLocalWorkerMessages()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                Voters = [1],
                SnapshotIndex = 2,
                AsyncStorageWrites = true,
            });
        environment.Campaign(0);

        environment.ProcessReady(0);

        Assert.Equal(
            1,
            environment.Nodes[0].AppendWorkCount);
        Assert.Equal(
            0,
            environment.Nodes[0].ApplyWorkCount);
        environment.ProcessAppendThread(0);
        Assert.Equal(
            0,
            environment.Nodes[0].AppendWorkCount);
        Assert.Contains(
            environment.QueuedMessages,
            message =>
                message.Type
                == MessageType.MsgVoteResp);
    }

    [Fact]
    public void EmptyWorkerCommandsAreExact()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                AsyncStorageWrites = true,
            });

        Assert.Equal(
            "no append work to perform",
            environment.Handle(
                "process-append-thread 1",
                string.Empty));
        Assert.Equal(
            "no apply work to perform",
            environment.Handle(
                "process-apply-thread 1",
                string.Empty));
    }

    [Fact]
    public void SnapshotAppendUpdatesPhysicalApplicationState()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                Voters = [1, 2],
                SnapshotIndex = 2,
                AsyncStorageWrites = true,
            });
        var state = new ConfState();
        state.Voters.Add([1UL, 2UL]);
        environment.Nodes[0].RawNode.Step(
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgSnap,
                Term = 1,
                Snapshot = new Snapshot
                {
                    Metadata = new SnapshotMetadata
                    {
                        Index = 5,
                        Term = 1,
                        ConfState = state,
                    },
                },
            });

        environment.ProcessReady(0);
        environment.ProcessAppendThread(0);

        Assert.Equal(
            5UL,
            environment.Nodes[0]
                .GetApplicationSnapshot()
                .Metadata.Index);
    }

    [Fact]
    public void AppendWorkerAcceptsSnapshotAndFollowingEntries()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            1,
            new InteractionNodeOptions
            {
                Voters = [1, 2],
                SnapshotIndex = 2,
                AsyncStorageWrites = true,
            });
        var state = new ConfState();
        state.Voters.Add([1UL, 2UL]);
        InteractionNode node = environment.Nodes[0];
        node.RawNode.Step(
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgSnap,
                Term = 1,
                Snapshot = new Snapshot
                {
                    Metadata = new SnapshotMetadata
                    {
                        Index = 5,
                        Term = 1,
                        ConfState = state,
                    },
                },
            });
        var append = new Message
        {
            From = 2,
            To = 1,
            Type = MessageType.MsgApp,
            Term = 1,
            Index = 5,
            LogTerm = 1,
            Commit = 6,
        };
        append.Entries.Add(
            new Entry
            {
                Index = 6,
                Term = 1,
                Data =
                    Google.Protobuf.ByteString
                        .CopyFromUtf8("after"),
            });
        node.RawNode.Step(append);

        environment.ProcessReady(0);
        environment.ProcessAppendThread(0);

        Assert.Equal(6UL, node.Storage.GetLastIndex());
        Assert.Equal(
            "after",
            node.Storage.GetEntries(
                    6,
                    7,
                    ulong.MaxValue)[0]
                .Data.ToStringUtf8());
        Assert.Equal(
            5UL,
            node.GetApplicationSnapshot()
                .Metadata.Index);
    }
}
