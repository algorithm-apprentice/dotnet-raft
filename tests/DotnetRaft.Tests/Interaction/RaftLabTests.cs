using DotnetRaft.Protocol;
using DotnetRaft.Tests.RawNode;

using Google.Protobuf;

namespace DotnetRaft.Tests.Interaction;

public sealed class RaftLabTests
{
    [Theory]
    [InlineData(false, 3)]
    [InlineData(true, 3)]
    [InlineData(false, 5)]
    [InlineData(true, 5)]
    public void MinorityCannotCommitAndItsOldLeaderRejoins(bool asyncStorageWrites, int count)
    {
        (InteractionEnvironment cluster, AppliedLogOracle oracle) = CreateCluster(asyncStorageWrites, count);
        cluster.Campaign(0);
        cluster.Stabilize();
        Assert.Equal(RaftRole.Leader, cluster.Nodes[0].RawNode.GetBasicStatus().Role);
        cluster.Propose(0, "prefix/"u8);
        cluster.Stabilize();
        ulong prefix = oracle.AppliedThrough(1);
        AssertApplication(cluster, "prefix/");

        // In the five-node case the old leader can still replicate to one
        // peer. That is not enough for this configuration's quorum of three.
        int minorityCount = count / 2;
        cluster.NetworkLinkIsOpen = message =>
            (message.From <= (ulong)minorityCount) == (message.To <= (ulong)minorityCount);
        cluster.Propose(0, "minority/"u8);
        cluster.Stabilize();
        Assert.Equal(prefix, cluster.Nodes[0].RawNode.GetBasicStatus().Commit);
        Assert.Equal(prefix, oracle.AppliedThrough(1));
        Assert.Equal(ByteString.CopyFromUtf8("minority/"),
            Assert.Single(cluster.Nodes[0].Storage.GetEntries(prefix + 1, prefix + 2, ulong.MaxValue)).Data);

        int newLeader = minorityCount;
        cluster.Campaign(newLeader);
        cluster.Stabilize();
        Assert.Equal(RaftRole.Leader, cluster.Nodes[newLeader].RawNode.GetBasicStatus().Role);
        cluster.Propose(newLeader, "majority/"u8);
        cluster.Stabilize();
        Assert.True(oracle.AppliedThrough((ulong)newLeader + 1) > prefix);
        Assert.Equal(prefix, oracle.AppliedThrough(1));

        cluster.NetworkLinkIsOpen = null;
        cluster.TickHeartbeat(newLeader);
        cluster.Stabilize();

        Assert.Equal(RaftRole.Follower, cluster.Nodes[0].RawNode.GetBasicStatus().Role);
        AssertApplication(cluster, "prefix/majority/");
        Assert.All(cluster.Nodes, node => Assert.Equal(
            oracle.AppliedThrough((ulong)newLeader + 1), oracle.AppliedThrough(node.Config.Id)));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void CrashAtStorageBoundaryReplaysOnlyUnappliedWorkAndRejoins(
        bool asyncStorageWrites, bool applyBeforeCommitWrite)
    {
        (InteractionEnvironment cluster, AppliedLogOracle oracle) = CreateCluster(asyncStorageWrites, 3);
        cluster.Campaign(0);
        cluster.Stabilize();
        InteractionNode previous = cluster.Nodes[0];
        ulong before = oracle.AppliedThrough(1);

        cluster.Propose(0, "durable/"u8);
        cluster.ProcessReady(0);
        if (asyncStorageWrites)
        {
            cluster.ProcessAppendThread(0);
        }

        // Progress the followers and return their replies without letting node
        // 1 consume its next Ready or run its application worker.
        cluster.Stabilize(1, 2);
        cluster.DeliverMessages(type: null, new InteractionRecipient(1));
        Assert.Equal(before + 1, previous.RawNode.GetBasicStatus().Commit);
        if (asyncStorageWrites)
        {
            cluster.ProcessReady(0);
            Assert.True(previous.ApplyWorkCount > 0);
            if (applyBeforeCommitWrite)
            {
                cluster.ProcessApplyThread(0);
            }
            else
            {
                cluster.ProcessAppendThread(0);
            }
        }
        else
        {
            Ready ready = previous.RawNode.Ready();
            Assert.Equal(before + 1, Assert.Single(ready.CommittedEntries).Index);
            RawNodeTestSupport.Persist(previous.Storage, ready);
        }

        ulong durableBeforeCrash = applyBeforeCommitWrite ? before : before + 1;
        ulong physicalBeforeCrash = applyBeforeCommitWrite ? before + 1 : before;
        Assert.Equal(durableBeforeCrash, previous.Storage.GetInitialState().HardState?.Commit);
        Assert.Equal(physicalBeforeCrash, oracle.AppliedThrough(1));
        cluster.CrashNode(0);

        cluster.Campaign(1);
        cluster.Stabilize();
        Assert.Equal(RaftRole.Leader, cluster.Nodes[1].RawNode.GetBasicStatus().Role);
        cluster.Propose(1, "while-down/"u8);
        cluster.Stabilize();
        Assert.Equal(physicalBeforeCrash, oracle.AppliedThrough(1));

        cluster.RestartNode(0);
        Assert.NotSame(previous.RawNode, cluster.Nodes[0].RawNode);
        cluster.Stabilize(0);
        Assert.Equal(before + 1, oracle.AppliedThrough(1));
        Assert.Equal("durable/", cluster.Nodes[0].GetApplicationSnapshot().Data.ToStringUtf8());

        cluster.TickHeartbeat(1);
        cluster.Stabilize();
        AssertApplication(cluster, "durable/while-down/");
    }

    private static (InteractionEnvironment, AppliedLogOracle) CreateCluster(bool asyncStorageWrites, int count)
    {
        const ulong initialIndex = 2;
        var oracle = new AppliedLogOracle(initialIndex);
        var cluster = new InteractionEnvironment { EntryApplied = oracle.Record };
        cluster.Handle("log-level none", string.Empty);
        cluster.AddNodes(count, new InteractionNodeOptions
        {
            Voters = [.. Enumerable.Range(1, count).Select(id => (ulong)id)],
            SnapshotIndex = initialIndex,
            AsyncStorageWrites = asyncStorageWrites,
        });
        return (cluster, oracle);
    }

    private static void AssertApplication(InteractionEnvironment cluster, string expected)
    {
        Assert.All(cluster.Nodes, node => Assert.Equal(expected, node.GetApplicationSnapshot().Data.ToStringUtf8()));
    }
}
