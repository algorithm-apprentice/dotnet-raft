using DotnetRaft.Core;
using DotnetRaft.Protocol;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;
using static DotnetRaft.Tests.Core.RaftCoreReadIndexTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreReadIndexTests
{
    [Fact]
    public void LocalSingletonAnswersBeforeCurrentTermNoOpPersists()
    {
        RaftCore core = Create(voters: [1]).Core;
        core.BecomeCandidate();
        core.BecomeLeader();

        core.Step(ReadRequest("local"));

        ReadState state = Assert.Single(
            core.TakeReadStates());
        Assert.Equal(0UL, state.Index);
        Assert.Equal(
            "local",
            state.RequestContext.ToStringUtf8());
        Assert.Empty(core.TakeMessages());
    }

    [Fact]
    public void RemoteSingletonReceivesReadIndexResponse()
    {
        RaftCore core = NewLeader(voters: [1]);

        core.Step(ReadRequest(
            "remote",
            from: 2,
            to: 1));

        Assert.Empty(core.TakeReadStates());
        Message response = Assert.Single(
            core.TakeMessages());
        Assert.Equal(MessageType.MsgReadIndexResp, response.Type);
        Assert.Equal(1UL, response.From);
        Assert.Equal(2UL, response.To);
        Assert.Equal(core.Term, response.Term);
        Assert.Equal(core.Log.Committed, response.Index);
        Assert.Equal(
            "remote",
            Assert.Single(response.Entries).Data.ToStringUtf8());
    }

    [Fact]
    public void NewLeaderQueuesReadUntilCurrentTermCommit()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            commitCurrentTerm: false);
        Message request = ReadRequest("gated");

        core.Step(request);

        Assert.Empty(core.TakeReadStates());
        Assert.Empty(core.TakeMessages());
        Assert.Equal(0, core.ReadOnly.PendingCount);

        core.Step(AppResponse(
            core,
            from: 2,
            index: core.Log.LastIndex));

        Message[] released = core.TakeMessages();
        ByteString context =
            LatestHeartbeatContext(released);
        Assert.False(context.IsEmpty);
        Assert.Equal(1UL, core.Log.Committed);
        Assert.Equal(1, core.ReadOnly.PendingCount);

        core.Step(HeartbeatResponse(
            core,
            from: 2,
            context));

        ReadState state = Assert.Single(
            core.TakeReadStates());
        Assert.Equal(1UL, state.Index);
        Assert.Equal(
            "gated",
            state.RequestContext.ToStringUtf8());
        Assert.Equal(0, core.ReadOnly.PendingCount);
    }

    [Fact]
    public void ActiveReadKeepsCommitIndexCapturedAtAdmission()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);
        core.Step(ReadRequest("stable"));
        ByteString context = LatestHeartbeatContext(
            core.TakeMessages());

        core.Step(Proposal("later"));
        Message selfAck = Assert.Single(
            core.TakeMessagesAfterAppend());
        core.Step(selfAck);
        core.TakeMessages();
        core.Step(AppResponse(
            core,
            from: 2,
            index: core.Log.LastIndex));
        core.TakeMessages();
        Assert.Equal(2UL, core.Log.Committed);

        core.Step(HeartbeatResponse(
            core,
            from: 2,
            context));

        ReadState state = Assert.Single(
            core.TakeReadStates());
        Assert.Equal(1UL, state.Index);
        Assert.Equal(
            "stable",
            state.RequestContext.ToStringUtf8());
    }

    [Fact]
    public void GatedRequestOwnsCallerMessage()
    {
        RaftCore core = NewLeader(
            voters: [1, 2, 3],
            commitCurrentTerm: false);
        Message request = ReadRequest("owned");

        core.Step(request);
        request.Entries[0].Data =
            ByteString.CopyFromUtf8("changed");

        core.Step(AppResponse(
            core,
            from: 2,
            index: core.Log.LastIndex));
        ByteString context = LatestHeartbeatContext(
            core.TakeMessages());
        core.Step(HeartbeatResponse(
            core,
            from: 2,
            context));

        Assert.Equal(
            "owned",
            Assert.Single(core.TakeReadStates())
                .RequestContext.ToStringUtf8());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public void InvalidRequestEntryCountFailsBeforeMutation(
        int entryCount)
    {
        RaftCore core = NewLeader(voters: [1]);
        var request = new Message
        {
            From = 1,
            To = 1,
            Type = MessageType.MsgReadIndex,
        };
        for (var index = 0; index < entryCount; index++)
        {
            request.Entries.Add(new Entry
            {
                Data = ByteString.CopyFromUtf8("entry"),
            });
        }

        Assert.Throws<RaftInvariantException>(
            () => core.Step(request));

        Assert.Empty(core.TakeReadStates());
        Assert.Empty(core.TakeMessages());
        Assert.Equal(0, core.ReadOnly.PendingCount);
    }

    [Fact]
    public void DuplicateUserContextsRemainDistinctRequests()
    {
        RaftCore core = NewLeader(voters: [1, 2, 3]);
        core.Step(ReadRequest("duplicate"));
        core.TakeMessages();
        core.Step(ReadRequest("duplicate"));
        ByteString latest = LatestHeartbeatContext(
            core.TakeMessages());

        core.Step(HeartbeatResponse(
            core,
            from: 2,
            latest));

        ReadState[] states = core.TakeReadStates();
        Assert.Equal(2, states.Length);
        Assert.All(
            states,
            state =>
            {
                Assert.Equal(1UL, state.Index);
                Assert.Equal(
                    "duplicate",
                    state.RequestContext.ToStringUtf8());
            });
        Assert.Equal(0, core.ReadOnly.PendingCount);
    }

    [Fact]
    public void TakingReadStatesReturnsDetachedSnapshot()
    {
        RaftCore core = NewLeader(voters: [1]);
        core.Step(ReadRequest("first"));

        ReadState[] first = core.TakeReadStates();

        core.Step(ReadRequest("second"));
        ReadState[] second = core.TakeReadStates();

        Assert.Equal(
            "first",
            Assert.Single(first)
                .RequestContext.ToStringUtf8());
        Assert.Equal(
            "second",
            Assert.Single(second)
                .RequestContext.ToStringUtf8());
    }

    private static Message Proposal(string context)
    {
        var proposal = new Message
        {
            From = 1,
            To = 1,
            Type = MessageType.MsgProp,
        };
        proposal.Entries.Add(new Entry
        {
            Data = ByteString.CopyFromUtf8(context),
        });
        return proposal;
    }
}
