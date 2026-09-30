using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Tracker;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreFlowControlTests
{
    [Fact]
    public void ProbeAcknowledgementFillsAndSlidesBoundedReplicateWindow()
    {
        RaftCore core = NewLeader(
            maxSizePerMessage: 0,
            maxInflightMessages: 2);
        Progress progress = core.Tracker.Progress[2];

        core.Step(Proposal("a"));
        Message firstProbe = Assert.Single(MessagesTo(core, 2));
        Assert.Equal([1UL], EntryIndexes(firstProbe));
        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.Equal(1UL, progress.Next);
        Assert.True(progress.AppendFlowPaused);

        core.Step(Proposal("b"));
        core.Step(Proposal("c"));

        Assert.Empty(MessagesTo(core, 2));
        Assert.Equal(1UL, progress.Next);
        Assert.Equal(0, progress.Inflights.Count);
        core.TakeMessagesAfterAppend();

        core.Step(AppResponse(core, 2, index: 1));

        Message[] pipeline = MessagesTo(core, 2);
        Assert.Equal(2, pipeline.Length);
        Assert.Equal([2UL], EntryIndexes(pipeline[0]));
        Assert.Equal([3UL], EntryIndexes(pipeline[1]));
        Assert.Equal(ProgressState.Replicate, progress.State);
        Assert.Equal(1UL, progress.Match);
        Assert.Equal(4UL, progress.Next);
        Assert.Equal(2, progress.Inflights.Count);
        Assert.True(progress.AppendFlowPaused);

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgHeartbeatResp,
        });

        Message recoveryProbe = Assert.Single(MessagesTo(core, 2));
        Assert.Empty(recoveryProbe.Entries);
        Assert.Equal(3UL, recoveryProbe.Index);
        Assert.Equal(2, progress.Inflights.Count);
        Assert.True(progress.AppendFlowPaused);

        core.Step(AppResponse(core, 2, index: 3));

        Message refill = Assert.Single(MessagesTo(core, 2));
        Assert.Equal([4UL], EntryIndexes(refill));
        Assert.Equal(3UL, progress.Match);
        Assert.Equal(5UL, progress.Next);
        Assert.Equal(1, progress.Inflights.Count);
        Assert.False(progress.AppendFlowPaused);

        core.Step(AppResponse(core, 2, index: 2));

        Assert.Equal(3UL, progress.Match);
        Assert.Equal(1, progress.Inflights.Count);
        Assert.Equal(1UL, progress.Inflights.Bytes);
        Assert.Empty(MessagesTo(core, 2));
    }

    [Fact]
    public void OversizedEntryIsSentAloneEvenWithZeroMessageLimit()
    {
        RaftCore core = NewLeader(maxSizePerMessage: 0);
        Progress progress = core.Tracker.Progress[2];
        Assert.True(progress.MaybeUpdate(1));
        progress.BecomeReplicate();
        string data = new('x', 2048);

        core.Step(Proposal(data));

        Message append = Assert.Single(MessagesTo(core, 2));
        Entry entry = Assert.Single(append.Entries);
        Assert.Equal(2UL, entry.Index);
        Assert.Equal(data, entry.Data.ToStringUtf8());
        Assert.Equal(3UL, progress.Next);
        Assert.Equal(1, progress.Inflights.Count);
    }

    [Fact]
    public void SoftInflightByteLimitAllowsOneOvershootingBatchThenPauses()
    {
        RaftCore core = NewLeader(
            maxSizePerMessage: 700,
            maxInflightMessages: 10,
            maxInflightBytes: 1000);
        Progress progress = core.Tracker.Progress[2];
        Assert.True(progress.MaybeUpdate(1));
        progress.BecomeReplicate();
        string payload = new('x', 600);

        core.Step(Proposal(payload));
        Assert.Single(MessagesTo(core, 2));
        Assert.Equal(600UL, progress.Inflights.Bytes);
        Assert.False(progress.IsPaused);

        core.Step(Proposal(payload));
        Assert.Single(MessagesTo(core, 2));
        Assert.Equal(1200UL, progress.Inflights.Bytes);
        Assert.Equal(2, progress.Inflights.Count);
        Assert.True(progress.IsPaused);

        core.Step(Proposal(payload));
        Assert.Empty(MessagesTo(core, 2));
        Assert.Equal(1200UL, progress.Inflights.Bytes);
    }

    [Fact]
    public void ProbeCanRecoverOnAcknowledgementEqualToKnownMatch()
    {
        RaftCore core = NewLeader();
        Progress progress = core.Tracker.Progress[2];
        Assert.True(progress.MaybeUpdate(1));
        progress.BecomeProbe();
        progress.AppendFlowPaused = true;

        core.Step(AppResponse(core, 2, index: 1));

        Assert.Equal(ProgressState.Replicate, progress.State);
        Assert.Equal(1UL, progress.Match);
        Assert.Equal(2UL, progress.Next);
        Assert.False(progress.AppendFlowPaused);
        Assert.Empty(MessagesTo(core, 2));
    }

    [Fact]
    public void HeartbeatRecordsBoundedCommitAndSuppressesRedundantAppend()
    {
        RaftCore core = NewLeader();
        Progress progress = core.Tracker.Progress[2];
        Assert.True(progress.MaybeUpdate(1));
        progress.BecomeProbe();
        core.Log.CommitTo(1);
        progress.RecordSentCommit(9);

        core.Step(new Message
        {
            From = 1,
            To = 1,
            Type = MessageType.MsgBeat,
        });

        Message heartbeat = Assert.Single(
            MessagesTo(core, 2));
        Assert.Equal(MessageType.MsgHeartbeat, heartbeat.Type);
        Assert.Equal(1UL, heartbeat.Commit);
        Assert.Equal(1UL, progress.LastSentCommit);

        core.Step(AppResponse(core, 2, index: 1));

        Assert.Equal(ProgressState.Replicate, progress.State);
        Assert.Empty(MessagesTo(core, 2));
    }

    [Fact]
    public void UnreachableReplicateDiscardsOptimisticPipeline()
    {
        RaftCore core = NewLeader(
            maxSizePerMessage: 0,
            maxInflightMessages: 2);
        Progress progress = core.Tracker.Progress[2];
        Assert.True(progress.MaybeUpdate(1));
        progress.BecomeReplicate();

        core.Step(Proposal("a"));
        core.TakeMessages();
        core.Step(Proposal("b"));
        core.TakeMessages();
        Assert.Equal(4UL, progress.Next);
        Assert.Equal(2, progress.Inflights.Count);

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Type = MessageType.MsgUnreachable,
        });

        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.Equal(2UL, progress.Next);
        Assert.Equal(0, progress.Inflights.Count);
        Assert.False(progress.AppendFlowPaused);
        Assert.Empty(core.TakeMessages());
    }

    [Fact]
    public void HeartbeatRecoversCaughtUpUnreachablePeerWithEmptyProbe()
    {
        RaftCore core = NewLeader();
        Progress progress = core.Tracker.Progress[2];
        Assert.True(progress.MaybeUpdate(core.Log.LastIndex));
        progress.BecomeReplicate();
        core.Step(new Message
        {
            From = 2,
            To = 1,
            Type = MessageType.MsgUnreachable,
        });
        Assert.Equal(ProgressState.Probe, progress.State);

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgHeartbeatResp,
        });

        Message probe = Assert.Single(MessagesTo(core, 2));
        Assert.Empty(probe.Entries);
        Assert.Equal(core.Log.LastIndex, probe.Index);

        core.Step(AppResponse(
            core,
            2,
            core.Log.LastIndex));

        Assert.Equal(ProgressState.Replicate, progress.State);
        Assert.Empty(MessagesTo(core, 2));
    }

    [Fact]
    public void UnknownAndNonLeaderUnreachableReportsAreIgnored()
    {
        RaftCore leader = NewLeader();
        Progress known = leader.Tracker.Progress[2];

        leader.Step(new Message
        {
            From = 99,
            To = 1,
            Type = MessageType.MsgUnreachable,
        });

        Assert.Equal(ProgressState.Probe, known.State);
        Assert.Empty(leader.TakeMessages());

        RaftCore follower = Create(
            voters: [1, 2, 3],
            term: 1).Core;
        follower.Step(new Message
        {
            From = 2,
            To = 1,
            Type = MessageType.MsgUnreachable,
        });

        Assert.Equal(RaftRole.Follower, follower.Role);
        Assert.Empty(follower.TakeMessages());
        Assert.Empty(follower.TakeMessagesAfterAppend());
    }

    [Fact]
    public void EarlierSelfAckWithCommitNeverUsesCommitBumpOrFillPath()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 1,
            commit: 1,
            applied: 1).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        core.Step(Proposal("later"));
        core.TakeMessages();
        Message[] acknowledgements =
            core.TakeMessagesAfterAppend();
        Assert.Equal([2UL, 3UL], acknowledgements.Select(
            message => message.Index));

        core.Step(acknowledgements[0]);

        Assert.Equal(2UL, core.Tracker.Progress[1].Match);
        Assert.Equal(3UL, core.Log.LastIndex);
        Assert.Equal(1UL, core.Log.Committed);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    private static RaftCore NewLeader(
        ulong maxSizePerMessage = ulong.MaxValue,
        int maxInflightMessages = 256,
        ulong maxInflightBytes = 0)
    {
        RaftCore core = Create(
            voters: [1, 3],
            learners: [2],
            maxSizePerMessage: maxSizePerMessage,
            maxInflightMessages: maxInflightMessages,
            maxInflightBytes: maxInflightBytes).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        Message noOpAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(noOpAck);
        core.TakeMessages();
        return core;
    }

    private static Message Proposal(string data)
    {
        var message = new Message
        {
            From = 1,
            To = 1,
            Type = MessageType.MsgProp,
        };
        message.Entries.Add(new Entry
        {
            Data = ByteString.CopyFromUtf8(data),
        });
        return message;
    }

    private static Message AppResponse(
        RaftCore core,
        ulong from,
        ulong index)
    {
        return new Message
        {
            From = from,
            To = core.Id,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = index,
        };
    }

    private static Message[] MessagesTo(
        RaftCore core,
        ulong destination)
    {
        return
        [
            .. core.TakeMessages().Where(
                message => message.To == destination),
        ];
    }

    private static ulong[] EntryIndexes(Message message)
    {
        return [.. message.Entries.Select(entry => entry.Index)];
    }
}
