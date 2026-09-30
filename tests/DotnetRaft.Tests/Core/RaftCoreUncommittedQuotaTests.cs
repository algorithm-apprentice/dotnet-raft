using DotnetRaft.Core;
using DotnetRaft.Protocol;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreUncommittedQuotaTests
{
    [Fact]
    public void AccumulatedPayloadAtLimitIsAcceptedAndNextProposalDrops()
    {
        RaftCore core = NewLeader(maxUncommittedSize: 10);

        core.Step(Proposal("12345"));
        core.TakeMessages();
        core.TakeMessagesAfterAppend();
        Assert.Equal(5UL, core.UncommittedSize);

        core.Step(Proposal("67890"));
        core.TakeMessages();
        core.TakeMessagesAfterAppend();
        Assert.Equal(10UL, core.UncommittedSize);
        ulong lastIndex = core.Log.LastIndex;
        HardState hardState = core.HardState;

        Assert.Throws<ProposalDroppedException>(
            () => core.Step(Proposal("x")));

        Assert.Equal(10UL, core.UncommittedSize);
        Assert.Equal(lastIndex, core.Log.LastIndex);
        Assert.Equal(hardState, core.HardState);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void FirstOversizedProposalAndLaterEmptyEntryAreAccepted()
    {
        RaftCore core = NewLeader(maxUncommittedSize: 5);

        core.Step(Proposal("1234567890"));

        Assert.Equal(10UL, core.UncommittedSize);
        Assert.Equal(2UL, core.Log.LastIndex);
        core.TakeMessages();
        core.TakeMessagesAfterAppend();

        Assert.Throws<ProposalDroppedException>(
            () => core.Step(Proposal("x")));
        Assert.Equal(2UL, core.Log.LastIndex);

        core.Step(EmptyProposal());

        Assert.Equal(10UL, core.UncommittedSize);
        Assert.Equal(3UL, core.Log.LastIndex);
        Entry empty = core.Log.GetEntries(3)[0];
        Assert.True(empty.Data.IsEmpty);
    }

    [Fact]
    public void ExplicitReductionSubtractsAndSaturatesAtZero()
    {
        RaftCore core = NewLeader(maxUncommittedSize: 20);
        core.Step(Proposal("1234567890"));
        Assert.Equal(10UL, core.UncommittedSize);

        core.ReduceUncommittedSize(4);
        Assert.Equal(6UL, core.UncommittedSize);

        core.ReduceUncommittedSize(6);
        Assert.Equal(0UL, core.UncommittedSize);

        core.ReduceUncommittedSize(100);
        Assert.Equal(0UL, core.UncommittedSize);
    }

    [Fact]
    public void CommitDoesNotReleaseQuotaBeforeApplication()
    {
        RaftCore core = NewLeader(maxUncommittedSize: 10);
        core.Step(Proposal("12345"));
        Message selfAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.TakeMessages();

        core.Step(selfAck);
        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgAppResp,
            Index = 2,
        });

        Assert.Equal(2UL, core.Log.Committed);
        Assert.Equal(5UL, core.UncommittedSize);

        core.BecomeFollower(
            core.Term + 1,
            RaftMessageTargets.None);

        Assert.Equal(0UL, core.UncommittedSize);
    }

    [Fact]
    public void CounterOverflowDropsProposalWithoutMutation()
    {
        RaftCore core = NewLeader(
            maxUncommittedSize: ulong.MaxValue);
        core.UncommittedSize = ulong.MaxValue;
        ulong lastIndex = core.Log.LastIndex;

        Assert.Throws<ProposalDroppedException>(
            () => core.Step(Proposal("x")));

        Assert.Equal(ulong.MaxValue, core.UncommittedSize);
        Assert.Equal(lastIndex, core.Log.LastIndex);
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    private static RaftCore NewLeader(
        ulong maxUncommittedSize)
    {
        RaftCore core = Create(
            voters: [1, 2],
            maxUncommittedEntriesSize:
                maxUncommittedSize).Core;
        core.BecomeCandidate();
        core.BecomeLeader();
        Message noOpAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(noOpAck);
        core.TakeMessages();
        Assert.Equal(0UL, core.UncommittedSize);
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

    private static Message EmptyProposal()
    {
        var message = new Message
        {
            From = 1,
            To = 1,
            Type = MessageType.MsgProp,
        };
        message.Entries.Add(new Entry());
        return message;
    }
}
