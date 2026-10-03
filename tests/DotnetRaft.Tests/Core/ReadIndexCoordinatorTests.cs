using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Quorum;
using DotnetRaft.Read;

using Google.Protobuf;

namespace DotnetRaft.Tests.Core;

public sealed class ReadIndexCoordinatorTests
{
    [Fact]
    public void ResetClearsActiveTrackerButPreservesGatedAndCompleted()
    {
        var coordinator = new ReadIndexCoordinator(
            ReadOnlyOption.Safe);
        Message gated = ReadRequest(
            from: 1,
            "gated");
        coordinator.Gate(gated);
        Assert.Null(
            coordinator.Complete(
                ReadRequest(from: 1, "done"),
                index: 7,
                localId: 1));
        JointConfig voters = Voters(1, 2, 3);
        ReadIndexAdvanceResult tracked =
            coordinator.TrackSafeRead(
                commitIndex: 7,
                ReadRequest(from: 1, "active"),
                localId: 1,
                voters);
        Assert.True(tracked.HeartbeatRequired);
        Assert.Equal(1, coordinator.PendingCount);

        coordinator.Reset();

        Assert.Equal(0, coordinator.PendingCount);
        Assert.Equal(1, coordinator.GatedCount);
        Assert.True(coordinator.HasReadStates);
        Assert.True(
            coordinator.TryDequeueGated(
                out Message? released));
        Assert.Equal(gated, released);
        Assert.Equal(
            "done",
            Assert.Single(
                coordinator.TakeReadStates())
                .RequestContext.ToStringUtf8());
    }

    [Fact]
    public void SafeReadTrackingAdvancesInFifoOrder()
    {
        var coordinator = new ReadIndexCoordinator(
            ReadOnlyOption.Safe);
        JointConfig voters = Voters(1, 2, 3);

        ReadIndexAdvanceResult first =
            coordinator.TrackSafeRead(
                commitIndex: 7,
                ReadRequest(from: 1, "first"),
                localId: 1,
                voters);
        ReadIndexAdvanceResult second =
            coordinator.TrackSafeRead(
                commitIndex: 8,
                ReadRequest(from: 1, "second"),
                localId: 1,
                voters);

        Assert.Empty(first.Completed);
        Assert.Empty(second.Completed);
        Assert.True(second.HeartbeatRequired);
        ByteString context =
            coordinator.GetHeartbeatContext();

        ReadIndexRequest[] completed =
            coordinator.Acknowledge(
                from: 2,
                context,
                voters);

        Assert.Equal(
            ["first", "second"],
            completed.Select(request =>
                request.Request.Entries[0]
                    .Data.ToStringUtf8()));
        Assert.Equal([7UL, 8UL], completed.Select(
            request => request.Index));
    }

    [Fact]
    public void SingletonSafeReadCompletesWithoutHeartbeat()
    {
        var coordinator = new ReadIndexCoordinator(
            ReadOnlyOption.Safe);

        ReadIndexAdvanceResult result =
            coordinator.TrackSafeRead(
                commitIndex: 7,
                ReadRequest(from: 1, "singleton"),
                localId: 1,
                Voters(1));

        Assert.Single(result.Completed);
        Assert.False(result.HeartbeatRequired);
    }

    [Fact]
    public void CompletionProducesLocalStateOrDetachedRemoteResponse()
    {
        var coordinator = new ReadIndexCoordinator(
            ReadOnlyOption.Safe);
        Message local = ReadRequest(
            from: 1,
            "local");

        Assert.Null(
            coordinator.Complete(
                local,
                index: 9,
                localId: 1));
        local.Entries[0].Data =
            ByteString.CopyFromUtf8("changed");
        ReadState state = Assert.Single(
            coordinator.PeekReadStates());
        Assert.Equal(9UL, state.Index);
        Assert.Equal(
            "local",
            state.RequestContext.ToStringUtf8());

        Message remote = ReadRequest(
            from: 2,
            "remote");
        Message response = Assert.IsType<Message>(
            coordinator.Complete(
                remote,
                index: 10,
                localId: 1));
        remote.Entries[0].Data =
            ByteString.CopyFromUtf8("changed");

        Assert.Equal(0UL, response.From);
        Assert.Equal(2UL, response.To);
        Assert.Equal(
            MessageType.MsgReadIndexResp,
            response.Type);
        Assert.Equal(10UL, response.Index);
        Assert.Equal(
            "remote",
            response.Entries[0].Data.ToStringUtf8());
    }

    [Fact]
    public void ResponseCompletionRejectsInvalidShapeWithoutMutation()
    {
        var coordinator = new ReadIndexCoordinator(
            ReadOnlyOption.Safe);
        var invalid = new Message
        {
            Type = MessageType.MsgReadIndexResp,
            Index = 7,
        };

        Assert.False(
            coordinator.TryCompleteResponse(invalid));
        Assert.False(coordinator.HasReadStates);

        var valid = invalid.Clone();
        valid.Entries.Add(new Entry
        {
            Data = ByteString.CopyFromUtf8("response"),
        });
        Assert.True(
            coordinator.TryCompleteResponse(valid));
        valid.Entries[0].Data =
            ByteString.CopyFromUtf8("changed");

        Assert.Equal(
            "response",
            Assert.Single(
                coordinator.TakeReadStates())
                .RequestContext.ToStringUtf8());
    }

    [Fact]
    public void RequestValidationRequiresOneEntry()
    {
        var coordinator = new ReadIndexCoordinator(
            ReadOnlyOption.Safe);

        RaftInvariantException exception =
            Assert.Throws<RaftInvariantException>(
                () => ReadIndexCoordinator.ValidateRequest(
                    new Message
                    {
                        Type = MessageType.MsgReadIndex,
                    }));

        Assert.Equal(
            "MsgReadIndex must contain exactly one entry.",
            exception.Message);
    }

    [Fact]
    public void NullInputsFailExplicitly()
    {
        var coordinator = new ReadIndexCoordinator(
            ReadOnlyOption.Safe);

        Assert.Equal(
            "message",
            Assert.Throws<ArgumentNullException>(
                    () => ReadIndexCoordinator
                        .ValidateRequest(null!))
                .ParamName);
        Assert.Equal(
            "message",
            Assert.Throws<ArgumentNullException>(
                    () => coordinator.Gate(null!))
                .ParamName);
        Assert.Equal(
            "request",
            Assert.Throws<ArgumentNullException>(
                    () => coordinator.Complete(
                        null!,
                        index: 1,
                        localId: 1))
                .ParamName);
        Assert.Equal(
            "message",
            Assert.Throws<ArgumentNullException>(
                    () => coordinator
                        .TryCompleteResponse(null!))
                .ParamName);
    }

    private static Message ReadRequest(
        ulong from,
        string context)
    {
        var message = new Message
        {
            From = from,
            To = 1,
            Type = MessageType.MsgReadIndex,
        };
        message.Entries.Add(new Entry
        {
            Data = ByteString.CopyFromUtf8(context),
        });
        return message;
    }

    private static JointConfig Voters(
        params ulong[] ids)
    {
        return new JointConfig(
            new MajorityConfig(ids));
    }
}
