using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;

using Google.Protobuf;

using static DotnetRaft.Tests.Core.RaftCoreElectionTestSupport;
using static DotnetRaft.Tests.Core.RaftCoreReadIndexTestSupport;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreReadIndexForwardingTests
{
    [Fact]
    public void FollowerNormalizesZeroOriginWhenForwarding()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2, 3],
            term: 5).Core;
        core.BecomeFollower(5, leaderId: 1);
        Message request = ReadRequest(
            "local",
            from: 0,
            to: 2);
        Message original = request.Clone();

        core.Step(request);

        Assert.Equal(original, request);
        Message forwarded = Assert.Single(
            core.TakeMessages());
        Assert.Equal(MessageType.MsgReadIndex, forwarded.Type);
        Assert.Equal(2UL, forwarded.From);
        Assert.Equal(1UL, forwarded.To);
        Assert.Equal(0UL, forwarded.Term);
        Assert.Equal(
            "local",
            Assert.Single(forwarded.Entries)
                .Data.ToStringUtf8());
    }

    [Fact]
    public void OldLeaderPreservesRemoteOriginWhenForwardingAgain()
    {
        RaftCore core = Create(
            id: 1,
            voters: [1, 2, 3],
            term: 5).Core;
        core.BecomeFollower(5, leaderId: 3);

        core.Step(ReadRequest(
            "remote",
            from: 2,
            to: 1));

        Message forwarded = Assert.Single(
            core.TakeMessages());
        Assert.Equal(2UL, forwarded.From);
        Assert.Equal(3UL, forwarded.To);
        Assert.Equal(0UL, forwarded.Term);
    }

    [Fact]
    public void FollowerWithoutLeaderDropsReadRequest()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2, 3],
            term: 5).Core;

        core.Step(ReadRequest(
            "dropped",
            from: 0,
            to: 2));

        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeReadStates());
    }

    [Theory]
    [InlineData(ElectionTestRole.PreCandidate)]
    [InlineData(ElectionTestRole.Candidate)]
    public void CampaigningNodeIgnoresReadMessages(
        ElectionTestRole role)
    {
        RaftCore core = Create(voters: [1, 2, 3]).Core;
        EnterRole(core, role);

        core.Step(ReadRequest("ignored"));
        core.Step(new Message
        {
            From = 2,
            To = 1,
            Term = core.Term,
            Type = MessageType.MsgReadIndexResp,
            Index = 7,
            Entries =
            {
                new Entry
                {
                    Data =
                        ByteString.CopyFromUtf8("ignored"),
                },
            },
        });

        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeReadStates());
    }

    [Fact]
    public void ValidFollowerResponseProducesOwnedReadState()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2, 3],
            term: 5).Core;
        core.BecomeFollower(5, leaderId: 1);
        var response = new Message
        {
            From = 1,
            To = 2,
            Term = 5,
            Type = MessageType.MsgReadIndexResp,
            Index = 7,
        };
        response.Entries.Add(new Entry
        {
            Data = ByteString.CopyFromUtf8("response"),
        });

        core.Step(response);
        response.Entries[0].Data =
            ByteString.CopyFromUtf8("changed");

        ReadState state = Assert.Single(
            core.TakeReadStates());
        Assert.Equal(7UL, state.Index);
        Assert.Equal(
            "response",
            state.RequestContext.ToStringUtf8());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public void MalformedFollowerResponseIsLoggedAndIgnored(
        int entryCount)
    {
        var logger = new RecordingLogger();
        RaftCore core = Create(
            id: 2,
            voters: [1, 2, 3],
            term: 5,
            logger: logger).Core;
        core.BecomeFollower(5, leaderId: 1);
        var response = new Message
        {
            From = 1,
            To = 2,
            Term = 5,
            Type = MessageType.MsgReadIndexResp,
            Index = 7,
        };
        for (var index = 0; index < entryCount; index++)
        {
            response.Entries.Add(new Entry());
        }

        core.Step(response);

        Assert.Empty(core.TakeReadStates());
        (RaftLogLevel Level, string Message) logged =
            Assert.Single(
                logger.Entries,
                entry =>
                    entry.Level == RaftLogLevel.Error);
        Assert.Contains(
            "exactly one entry",
            logged.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void FollowerEchoesHeartbeatReadContext()
    {
        RaftCore core = Create(
            id: 2,
            voters: [1, 2, 3],
            term: 5).Core;
        ByteString context =
            ByteString.CopyFromUtf8("context");

        core.Step(new Message
        {
            From = 1,
            To = 2,
            Term = 5,
            Type = MessageType.MsgHeartbeat,
            Context = context,
        });

        Message response = Assert.Single(
            core.TakeMessages());
        Assert.Equal(MessageType.MsgHeartbeatResp, response.Type);
        Assert.Equal(context, response.Context);
    }

    [Fact]
    public void RemoteResponseOwnsRequestEntry()
    {
        RaftCore core = NewLeader(voters: [1]);
        Message request = ReadRequest(
            "original",
            from: 2,
            to: 1);

        core.Step(request);
        request.Entries[0].Data =
            ByteString.CopyFromUtf8("changed");

        Message response = Assert.Single(
            core.TakeMessages());
        Assert.Equal(
            "original",
            Assert.Single(response.Entries)
                .Data.ToStringUtf8());
    }

    private sealed class RecordingLogger : IRaftLogger
    {
        internal List<(RaftLogLevel Level, string Message)> Entries
        {
            get;
        } = [];

        public bool IsEnabled(RaftLogLevel level)
        {
            return true;
        }

        public void Log(
            RaftLogLevel level,
            string message)
        {
            Entries.Add((level, message));
        }
    }
}
