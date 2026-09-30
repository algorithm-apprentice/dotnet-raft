using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Tracker;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreRejectionHintTests
{
    [Fact]
    public void FollowerRejectionSkipsHigherTermDivergentSuffix()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 3),
                EntryAt(3, 3),
                EntryAt(4, 4),
                EntryAt(5, 4),
                EntryAt(6, 5),
                EntryAt(7, 5),
                EntryAt(8, 5),
                EntryAt(9, 6),
            ],
            term: 7,
            commit: 1,
            applied: 1).Core;
        core.BecomeFollower(7, 1);

        core.Step(new Message
        {
            From = 1,
            To = 2,
            Term = 7,
            Type = MessageType.MsgApp,
            Index = 8,
            LogTerm = 3,
        });

        Assert.Empty(core.TakeMessages());
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.True(response.Reject);
        Assert.Equal(8UL, response.Index);
        Assert.Equal(3UL, response.RejectHint);
        Assert.Equal(3UL, response.LogTerm);
    }

    [Fact]
    public void LeaderUsesFollowerTermToSkipItsOwnIncompatibleRun()
    {
        RaftCore core = NewLeader(
        [
            EntryAt(1, 1),
            EntryAt(2, 3),
            EntryAt(3, 3),
            EntryAt(4, 3),
            EntryAt(5, 3),
            EntryAt(6, 3),
            EntryAt(7, 3),
            EntryAt(8, 3),
            EntryAt(9, 5),
        ],
        term: 5);
        Progress progress = core.Tracker.Progress[2];
        Assert.Equal(10UL, progress.Next);

        core.Step(Rejected(
            core,
            index: 9,
            rejectHint: 6,
            logTerm: 2));

        Assert.Equal(2UL, progress.Next);
        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.True(progress.AppendFlowPaused);
        Message retry = Assert.Single(core.TakeMessages());
        Assert.Equal(1UL, retry.Index);
        Assert.Equal(1UL, retry.LogTerm);
        Assert.Equal(2UL, retry.Entries[0].Index);
    }

    [Fact]
    public void ReplicateRejectionBecomesProbeAndClearsInflights()
    {
        RaftCore core = NewLeader(
            [EntryAt(1, 1)],
            term: 1,
            maxSizePerMessage: 0);
        Progress progress = core.Tracker.Progress[2];
        Assert.True(progress.MaybeUpdate(1));
        progress.BecomeReplicate();

        core.Step(Proposal("a"));
        core.TakeMessages();
        core.Step(Proposal("b"));
        core.TakeMessages();
        Assert.Equal(ProgressState.Replicate, progress.State);
        Assert.Equal(4UL, progress.Next);
        Assert.Equal(2, progress.Inflights.Count);

        core.Step(Rejected(
            core,
            index: 3,
            rejectHint: 1,
            logTerm: 1));

        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.Equal(2UL, progress.Next);
        Assert.Equal(0, progress.Inflights.Count);
        Assert.True(progress.AppendFlowPaused);
        Message retry = Assert.Single(core.TakeMessages());
        Assert.Equal(1UL, retry.Index);
        Assert.Equal([2UL], retry.Entries.Select(entry => entry.Index));
    }

    [Fact]
    public void StaleAndZeroIndexRejectionsDoNotChangeProgress()
    {
        RaftCore core = NewLeader(
            [EntryAt(1, 1)],
            term: 1);
        Progress progress = core.Tracker.Progress[2];
        ulong next = progress.Next;

        core.Step(Rejected(
            core,
            index: next,
            rejectHint: 0,
            logTerm: 0));

        Assert.Equal(next, progress.Next);
        Assert.Empty(core.TakeMessages());

        core.Step(Rejected(
            core,
            index: 0,
            rejectHint: 0,
            logTerm: 0));

        Assert.Equal(next, progress.Next);
        Assert.Empty(core.TakeMessages());
    }

    private static RaftCore NewLeader(
        IEnumerable<Entry> entries,
        ulong term,
        ulong maxSizePerMessage = ulong.MaxValue)
    {
        RaftCore core = Create(
            voters: [1, 2],
            entries: entries,
            term: term,
            maxSizePerMessage: maxSizePerMessage).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        Message noOpAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(noOpAck);
        core.TakeMessages();
        return core;
    }

    private static Message Rejected(
        RaftCore core,
        ulong index,
        ulong rejectHint,
        ulong logTerm)
    {
        return new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = index,
            Reject = true,
            RejectHint = rejectHint,
            LogTerm = logTerm,
        };
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
}
