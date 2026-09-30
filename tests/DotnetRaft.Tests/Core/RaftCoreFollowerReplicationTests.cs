using DotnetRaft.Core;
using DotnetRaft.Protocol;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreFollowerReplicationTests
{
    [Fact]
    public void MatchingAppendWaitsForDurabilityBeforeAcknowledging()
    {
        RaftCore core = Create(
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
            ],
            term: 3,
            commit: 1,
            applied: 1).Core;
        core.BecomeFollower(3, 2);
        var append = Append(
            core,
            previousIndex: 2,
            previousTerm: 2,
            leaderCommit: 3,
            EntryAt(3, 3),
            EntryAt(4, 3));
        Message original = append.Clone();

        core.Step(append);

        Assert.Equal(original, append);
        Assert.Equal(4UL, core.Log.LastIndex);
        Assert.Equal(3UL, core.Log.Committed);
        Assert.Equal(
            [1UL, 2UL, 3UL, 4UL],
            core.Log.GetAllEntries().Select(entry => entry.Index));
        Assert.Empty(core.TakeMessages());

        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgAppResp, response.Type);
        Assert.Equal(core.Id, response.From);
        Assert.Equal(2UL, response.To);
        Assert.Equal(core.Term, response.Term);
        Assert.Equal(4UL, response.Index);
        Assert.False(response.Reject);
        Assert.Equal(0UL, response.RejectHint);
        Assert.Equal(0UL, response.LogTerm);
    }

    [Fact]
    public void MatchingHeartbeatAppendWithoutEntriesStillAcknowledgesDurably()
    {
        RaftCore core = Create(
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
            ],
            term: 3,
            commit: 1,
            applied: 1).Core;
        core.BecomeFollower(3, 2);

        core.Step(Append(
            core,
            previousIndex: 2,
            previousTerm: 2,
            leaderCommit: 2));

        Assert.Equal(2UL, core.Log.Committed);
        Assert.Empty(core.TakeMessages());
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(2UL, response.Index);
        Assert.False(response.Reject);
    }

    [Fact]
    public void ConflictingSuffixIsReplacedAndCommitIsBoundedByLastNewEntry()
    {
        RaftCore core = Create(
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
                EntryAt(3, 2),
                EntryAt(4, 2),
            ],
            term: 4,
            commit: 1,
            applied: 1).Core;
        core.BecomeFollower(4, 2);

        core.Step(Append(
            core,
            previousIndex: 1,
            previousTerm: 1,
            leaderCommit: 9,
            EntryAt(2, 3),
            EntryAt(3, 3)));

        Entry[] entries = [.. core.Log.GetAllEntries()];
        Assert.Equal([1UL, 2UL, 3UL], entries.Select(entry => entry.Index));
        Assert.Equal([1UL, 3UL, 3UL], entries.Select(entry => entry.Term));
        Assert.Equal(3UL, core.Log.Committed);
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(3UL, response.Index);
        Assert.False(response.Reject);
    }

    [Fact]
    public void MismatchedPreviousEntryRejectsWithoutChangingLogOrCommit()
    {
        RaftCore core = Create(
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
            ],
            term: 4,
            commit: 1,
            applied: 1).Core;
        core.BecomeFollower(4, 2);
        Entry[] before = [.. core.Log.GetAllEntries()];

        core.Step(Append(
            core,
            previousIndex: 2,
            previousTerm: 3,
            leaderCommit: 2,
            EntryAt(3, 4)));

        Assert.Equal(before, core.Log.GetAllEntries());
        Assert.Equal(1UL, core.Log.Committed);
        Assert.Empty(core.TakeMessages());
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.True(response.Reject);
        Assert.Equal(2UL, response.Index);
        Assert.Equal(2UL, response.RejectHint);
        Assert.Equal(2UL, response.LogTerm);
    }

    [Fact]
    public void AppendBelowCommittedPrefixReturnsCommittedIndex()
    {
        RaftCore core = Create(
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
                EntryAt(3, 2),
            ],
            term: 4,
            commit: 3,
            applied: 3).Core;
        core.BecomeFollower(4, 2);

        core.Step(Append(
            core,
            previousIndex: 1,
            previousTerm: 1,
            leaderCommit: 1,
            EntryAt(2, 2)));

        Assert.Equal(3UL, core.Log.LastIndex);
        Assert.Equal(3UL, core.Log.Committed);
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.False(response.Reject);
        Assert.Equal(3UL, response.Index);
    }

    [Fact]
    public void CurrentTermAppendMakesCandidateFollowerThenProcessesPayload()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 4).Core;
        core.BecomeCandidate();
        core.ElectionElapsed = 4;

        core.Step(Append(
            core,
            from: 2,
            previousIndex: 1,
            previousTerm: 1,
            leaderCommit: 0,
            EntryAt(2, 5)));

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(5UL, core.Term);
        Assert.Equal(2UL, core.LeaderId);
        Assert.Equal(0, core.ElectionElapsed);
        Assert.Equal(2UL, core.Log.LastIndex);
        Message response = Assert.Single(
            core.TakeMessagesAfterAppend());
        Assert.Equal(2UL, response.Index);
    }

    [Fact]
    public void EqualTermLeaderStillIgnoresCompetingAppendPayload()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 4).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        core.TakeMessagesAfterAppend();
        ulong lastIndex = core.Log.LastIndex;

        core.Step(Append(
            core,
            from: 2,
            previousIndex: 1,
            previousTerm: 1,
            leaderCommit: 0,
            EntryAt(2, core.Term)));

        Assert.Equal(RaftRole.Leader, core.Role);
        Assert.Equal(core.Id, core.LeaderId);
        Assert.Equal(lastIndex, core.Log.LastIndex);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void HeartbeatAdvancesCommitAndRespondsImmediatelyWithContext()
    {
        RaftCore core = Create(
            voters: [1, 2],
            entries:
            [
                EntryAt(1, 1),
                EntryAt(2, 2),
                EntryAt(3, 3),
            ],
            term: 4,
            commit: 1,
            applied: 1).Core;
        core.BecomeFollower(4, 2);
        ByteString context = ByteString.CopyFromUtf8("read-context");
        var heartbeat = new Message
        {
            From = 2,
            To = 1,
            Term = 4,
            Type = MessageType.MsgHeartbeat,
            Commit = 3,
            Context = context,
        };
        Message original = heartbeat.Clone();

        core.Step(heartbeat);

        Assert.Equal(original, heartbeat);
        Assert.Equal(3UL, core.Log.Committed);
        Assert.Equal(0, core.ElectionElapsed);
        Message response = Assert.Single(core.TakeMessages());
        Assert.Equal(MessageType.MsgHeartbeatResp, response.Type);
        Assert.Equal(1UL, response.From);
        Assert.Equal(2UL, response.To);
        Assert.Equal(4UL, response.Term);
        Assert.Equal(context, response.Context);
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void CurrentTermHeartbeatMakesCandidateFollowerAndProcessesPayload()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 4).Core;
        core.BecomeCandidate();
        core.ElectionElapsed = 4;

        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgHeartbeat,
            Commit = 1,
        });

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(5UL, core.Term);
        Assert.Equal(2UL, core.LeaderId);
        Assert.Equal(0, core.ElectionElapsed);
        Assert.Equal(1UL, core.Log.Committed);
        Assert.Single(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void SnapshotStillOnlyRecordsLeaderIdentityInD14()
    {
        RaftCore core = Create(
            voters: [1, 2, 3],
            entries: [EntryAt(1, 1)],
            term: 4).Core;
        core.BecomeCandidate();
        var snapshot = new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgSnap,
            Snapshot = new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = 10,
                    Term = 5,
                    ConfState = new ConfState
                    {
                        Voters = { 1, 2, 3 },
                    },
                },
            },
        };

        core.Step(snapshot);

        Assert.Equal(RaftRole.Follower, core.Role);
        Assert.Equal(2UL, core.LeaderId);
        Assert.Equal(1UL, core.Log.LastIndex);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    private static Message Append(
        RaftCore core,
        ulong previousIndex,
        ulong previousTerm,
        ulong leaderCommit,
        params Entry[] entries)
    {
        return Append(
            core,
            from: 2,
            previousIndex,
            previousTerm,
            leaderCommit,
            entries);
    }

    private static Message Append(
        RaftCore core,
        ulong from,
        ulong previousIndex,
        ulong previousTerm,
        ulong leaderCommit,
        params Entry[] entries)
    {
        var message = new Message
        {
            From = from,
            To = core.Id,
            Term = core.Term,
            Type = MessageType.MsgApp,
            Index = previousIndex,
            LogTerm = previousTerm,
            Commit = leaderCommit,
        };
        message.Entries.Add(entries);
        return message;
    }
}
