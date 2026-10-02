using System.Collections;
using System.Globalization;

using DotnetRaft.Protocol;
using DotnetRaft.Storage;
using DotnetRaft.Tracker;

using static DotnetRaft.Tests.RawNode.RawNodeTestSupport;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.RawNode;

public sealed class RawNodeStatusTests
{
    [Fact]
    public void FullStatusAlwaysIncludesDetachedConfiguration()
    {
        var node = CreateNode(
            CreateStorage(
                voters: [3, 1],
                learners: [2]));

        Status captured = node.GetStatus();

        Assert.Equal([1UL, 3UL], captured.Configuration.Voters);
        Assert.Equal([2UL], captured.Configuration.Learners);
        Assert.Empty(captured.Progress);

        node.ApplyConfChange(new ProtocolConfChange
        {
            Type =
                ConfChangeType.ConfChangeAddLearnerNode,
            NodeId = 4,
        });

        Assert.Equal([1UL, 3UL], captured.Configuration.Voters);
        Assert.Equal([2UL], captured.Configuration.Learners);
        Assert.DoesNotContain(
            4UL,
            captured.Configuration.Learners);
    }

    [Fact]
    public void LeaderProgressIsSortedAndDetached()
    {
        var node = CreateNode(
            CreateStorage(voters: [3, 1, 2]));
        node.Core.BecomeCandidate();
        node.Core.BecomeLeader();

        Status captured = node.GetStatus();

        Assert.Equal(
            [1UL, 2UL, 3UL],
            captured.Progress.Keys);
        ProgressStatus peer = captured.Progress[2];
        node.Core.Tracker.Progress[2]
            .MaybeUpdate(5);
        Assert.Equal(peer, captured.Progress[2]);
        Assert.NotEqual(
            peer.Match,
            node.GetStatus().Progress[2].Match);
    }

    [Fact]
    public void StatusCollectionsAreReadOnly()
    {
        var node = CreateNode(
            CreateStorage(voters: [1, 2]));
        node.Core.BecomeCandidate();
        node.Core.BecomeLeader();
        Status status = node.GetStatus();

        IList voters = Assert.IsAssignableFrom<IList>(
            status.Configuration.Voters);
        IDictionary progress =
            Assert.IsAssignableFrom<IDictionary>(
                status.Progress);

        Assert.Throws<NotSupportedException>(
            () => voters.Add(3UL));
        Assert.Throws<NotSupportedException>(
            () => progress.Add(
                3UL,
                status.Progress[1]));
    }

    [Fact]
    public void VisitProgressWorksForFollowersInSortedOrder()
    {
        var node = CreateNode(
            CreateStorage(voters: [3, 1, 2]));
        var visited = new List<ulong>();

        node.VisitProgress(
            (id, _) => visited.Add(id));

        Assert.Equal([1UL, 2UL, 3UL], visited);
    }

    [Fact]
    public void VisitProgressMaterializesBeforeCallbacks()
    {
        var node = CreateNode(
            CreateStorage(voters: [1, 2, 3]));
        var snapshots =
            new Dictionary<ulong, ProgressStatus>();

        node.VisitProgress((id, progress) =>
        {
            snapshots.Add(id, progress);
            if (id == 1)
            {
                node.Core.Tracker.Progress[3]
                    .MaybeUpdate(7);
            }
        });

        Assert.Equal(0UL, snapshots[3].Match);
        Assert.Equal(
            7UL,
            node.Core.Tracker.Progress[3].Match);
    }

    [Fact]
    public void VisitProgressRejectsSameNodeReentry()
    {
        var node = CreateNode(
            CreateStorage(voters: [1]));
        InvalidOperationException? rejection = null;

        node.VisitProgress((_, _) =>
        {
            rejection = Assert.Throws<InvalidOperationException>(
                () => node.GetBasicStatus());
        });

        Assert.NotNull(rejection);
        Assert.False(node.HasReady());
    }

    [Fact]
    public void ProgressStatusIncludesInflightAccounting()
    {
        var node = CreateNode(
            CreateStorage(voters: [1, 2]));
        Progress peer = node.Core.Tracker.Progress[2];
        peer.BecomeReplicate();
        peer.SentEntries(1, 7);
        node.Core.BecomeCandidate();
        node.Core.BecomeLeader();
        peer = node.Core.Tracker.Progress[2];
        peer.BecomeReplicate();
        peer.SentEntries(1, 7);

        ProgressStatus status =
            node.GetStatus().Progress[2];

        Assert.Equal(1, status.InflightCount);
        Assert.Equal(7UL, status.InflightBytes);
        Assert.Equal(256, status.InflightCapacity);
        Assert.Equal(
            ulong.MaxValue,
            status.MaxInflightBytes);
        Assert.True(status.AppendFlowPaused is false);
    }

    [Fact]
    public void SnapshotProgressIsEffectivelyPaused()
    {
        var node = CreateNode(
            CreateStorage(voters: [1, 2]));
        node.Core.BecomeCandidate();
        node.Core.BecomeLeader();
        Progress peer = node.Core.Tracker.Progress[2];
        peer.BecomeSnapshot(5);
        peer.AppendFlowPaused = false;

        ProgressStatus status =
            node.GetStatus().Progress[2];

        Assert.Equal(
            ReplicationState.Snapshot,
            status.State);
        Assert.False(status.AppendFlowPaused);
        Assert.True(status.IsPaused);
        Assert.Equal(5UL, status.PendingSnapshot);
    }

    [Fact]
    public void AppliedStatusIsLogicalAdvanceCursor()
    {
        MemoryStorage storage =
            CreateStorage(voters: [1]);
        storage.Append([EntryAt(1, 1, "value")]);
        storage.SetHardState(new HardState
        {
            Term = 1,
            Commit = 1,
        });
        var node = CreateNode(storage);

        Ready ready = node.Ready();
        node.Advance(ready);

        Assert.Equal(
            1UL,
            node.GetBasicStatus().Applied);
    }

    [Fact]
    public void BasicStatusTracksLeadershipTransfer()
    {
        MemoryStorage storage =
            CreateStorage(voters: [1, 2]);
        var node = CreateNode(storage);
        BecomeTwoVoterLeader(node, storage);

        node.TransferLeader(2);

        BasicStatus status = node.GetBasicStatus();
        Assert.Equal(1UL, status.Id);
        Assert.Equal(RaftRole.Leader, status.Role);
        Assert.Equal(1UL, status.LeaderId);
        Assert.Equal(2UL, status.LeaderTransferee);
    }

    [Fact]
    public void StatusJsonIsExactAndCultureInvariant()
    {
        MemoryStorage storage =
            CreateStorage(voters: [10]);
        storage.SetHardState(new HardState
        {
            Term = 2,
            Vote = 10,
        });
        var node = CreateNode(storage, id: 10);
        CultureInfo previous =
            CultureInfo.CurrentCulture;

        try
        {
            CultureInfo.CurrentCulture =
                CultureInfo.GetCultureInfo("ar-EG");

            Assert.Equal(
                "{\"id\":\"a\",\"term\":2,\"vote\":\"a\",\"commit\":0,\"lead\":\"0\",\"raftState\":\"Follower\",\"applied\":0,\"progress\":{},\"leadtransferee\":\"0\"}",
                node.GetStatus().ToString());
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Fact]
    public void LeaderStatusJsonSortsProgressNumerically()
    {
        var node = CreateNode(
            CreateStorage(voters: [0x10, 2, 1]));
        node.Core.BecomeCandidate();
        node.Core.BecomeLeader();

        string json = node.GetStatus().ToString();
        int one = json.IndexOf(
            "\"1\":{",
            StringComparison.Ordinal);
        int two = json.IndexOf(
            "\"2\":{",
            StringComparison.Ordinal);
        int sixteen = json.IndexOf(
            "\"10\":{",
            StringComparison.Ordinal);

        Assert.True(one >= 0);
        Assert.True(one < two);
        Assert.True(two < sixteen);
    }

    [Fact]
    public void StatusInspectionDoesNotConsumeOutstandingReady()
    {
        MemoryStorage storage = CreateStorage();
        var node = CreateNode(storage);
        node.Campaign();
        Ready ready = node.Ready();

        _ = node.GetBasicStatus();
        _ = node.GetStatus();
        node.VisitProgress((_, _) => { });

        node.Advance(ready);
        Assert.True(node.HasReady());
    }
}
