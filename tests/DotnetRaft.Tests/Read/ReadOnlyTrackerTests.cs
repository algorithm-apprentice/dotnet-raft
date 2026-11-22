using System.Buffers.Binary;
using System.Runtime.CompilerServices;

using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Quorum;
using DotnetRaft.Read;

using Google.Protobuf;

namespace DotnetRaft.Tests.Read;

public sealed class ReadOnlyTrackerTests
{
    [Theory]
    [InlineData(ReadOnlyOption.Safe)]
    [InlineData(ReadOnlyOption.LeaseBased)]
    public void ConstructorStartsEmptyAndStoresOption(ReadOnlyOption option)
    {
        var tracker = new ReadOnlyTracker(option);

        Assert.Equal(option, tracker.Option);
        Assert.Equal(0, tracker.PendingCount);
        Assert.Equal(0UL, tracker.ConfirmedCount);
        Assert.Equal(ByteString.Empty, tracker.GetHeartbeatContext());
    }

    [Fact]
    public void HeartbeatContextUsesCumulativeLittleEndianPositions()
    {
        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);
        JointConfig voters = Voters(1);

        tracker.AddRequest(10, Request("first"));
        tracker.AddRequest(20, Request("second"));

        ByteString context = tracker.GetHeartbeatContext();
        Assert.Equal(8, context.Length);
        Assert.Equal(2UL, Decode(context));

        AcknowledgeAt(tracker, 1, 1);
        ReadIndexRequest confirmed = Assert.Single(tracker.Advance(voters));
        Assert.Equal("first", RequestContext(confirmed.Request));
        Assert.Equal(2UL, Decode(tracker.GetHeartbeatContext()));

        tracker.AddRequest(30, Request("third"));

        Assert.Equal(3UL, Decode(tracker.GetHeartbeatContext()));
    }

    [Fact]
    public void AddRequestClonesTheMessageAndPreservesCapturedCommitIndex()
    {
        var original = new Message
        {
            Type = MessageType.MsgReadIndex,
            Context = ByteString.CopyFromUtf8("message-context"),
        };
        original.Entries.Add(new Entry
        {
            Data = ByteString.CopyFromUtf8("request-context"),
        });

        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);
        ReadAcknowledgement local = tracker.AddRequest(42, original);

        original.Type = MessageType.MsgProp;
        original.Context = ByteString.CopyFromUtf8("changed-message");
        original.Entries[0].Data = ByteString.CopyFromUtf8("changed-request");
        original.Entries.Clear();

        tracker.ReceiveAcknowledgement(1, local);
        ReadIndexRequest confirmed = Assert.Single(
            tracker.Advance(Voters(1)));

        Assert.Equal(42UL, confirmed.Index);
        Assert.Equal(MessageType.MsgReadIndex, confirmed.Request.Type);
        Assert.Equal(
            "message-context",
            confirmed.Request.Context.ToStringUtf8());
        Assert.Equal("request-context", RequestContext(confirmed.Request));
        Assert.NotSame(original, confirmed.Request);
    }

    [Fact]
    public void IdenticalUserContextsRemainDistinctFifoRequests()
    {
        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);
        tracker.AddRequest(10, Request("same"));
        ReadAcknowledgement latest = tracker.AddRequest(20, Request("same"));

        tracker.ReceiveAcknowledgement(1, latest);
        ReadIndexRequest[] confirmed = tracker.Advance(Voters(1));

        Assert.Equal(2, confirmed.Length);
        Assert.Equal([10UL, 20UL], confirmed.Select(item => item.Index));
        Assert.All(
            confirmed,
            item => Assert.Equal("same", RequestContext(item.Request)));
    }

    [Fact]
    public void EmptyAcknowledgementIsValidAndIgnored()
    {
        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);
        tracker.AddRequest(10, Request("first"));

        Assert.Null(tracker.ParseAcknowledgement(ByteString.Empty, fromPeer: true));

        Assert.Empty(tracker.Advance(Voters(1)));
        Assert.Equal(1, tracker.PendingCount);
        Assert.Equal(0UL, tracker.ConfirmedCount);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(9)]
    public void NonEightByteAcknowledgementIsRejected(int length)
    {
        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);
        tracker.AddRequest(10, Request("first"));
        ByteString malformed = ByteString.CopyFrom(new byte[length]);

        ArgumentException input = Assert.Throws<ArgumentException>("context",
            () => tracker.ParseAcknowledgement(malformed, fromPeer: true));
        RaftInvariantException invariant = Assert.Throws<RaftInvariantException>(
            () => tracker.ParseAcknowledgement(malformed, fromPeer: false));

        Assert.StartsWith(invariant.Message, input.Message, StringComparison.Ordinal);
        Assert.Equal(1, tracker.PendingCount);
        Assert.Equal(0UL, tracker.ConfirmedCount);
    }

    [Theory]
    [InlineData(0UL)]
    [InlineData(2UL)]
    public void ZeroAndFutureAcknowledgementPositionsAreRejected(ulong position)
    {
        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);
        tracker.AddRequest(10, Request("first"));

        ByteString invalid = Encode(position);
        ArgumentException input = Assert.Throws<ArgumentException>("context",
            () => tracker.ParseAcknowledgement(invalid, fromPeer: true));
        RaftInvariantException invariant = Assert.Throws<RaftInvariantException>(
            () => tracker.ParseAcknowledgement(invalid, fromPeer: false));

        Assert.StartsWith(invariant.Message, input.Message, StringComparison.Ordinal);
        Assert.Empty(tracker.Advance(Voters(1)));
        Assert.Equal(1, tracker.PendingCount);
        Assert.Equal(0UL, tracker.ConfirmedCount);
    }

    [Fact]
    public void RejectedFutureMinorityAcknowledgementCannotConfirmLaterRead()
    {
        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);
        JointConfig voters = Voters(1, 2, 3);
        tracker.AddRequest(10, Request("first"));

        Assert.Throws<RaftInvariantException>(
            () => tracker.ParseAcknowledgement(Encode(2), fromPeer: false));

        AcknowledgeAt(tracker, 1, 1);
        AcknowledgeAt(tracker, 2, 1);
        Assert.Single(tracker.Advance(voters));

        ReadAcknowledgement next = tracker.AddRequest(20, Request("second"));
        tracker.ReceiveAcknowledgement(1, next);

        Assert.Empty(tracker.Advance(voters));
        Assert.Equal(1, tracker.PendingCount);
    }

    [Fact]
    public void AcknowledgementsNeverRegress()
    {
        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);
        JointConfig voters = Voters(1, 2, 3);
        tracker.AddRequest(10, Request("first"));
        tracker.AddRequest(20, Request("second"));
        tracker.AddRequest(30, Request("third"));

        AcknowledgeAt(tracker, 2, 3);
        AcknowledgeAt(tracker, 2, 1);
        AcknowledgeAt(tracker, 1, 2);

        ReadIndexRequest[] confirmed = tracker.Advance(voters);

        Assert.Equal([10UL, 20UL], confirmed.Select(item => item.Index));
        Assert.Equal(1, tracker.PendingCount);
        Assert.Equal(2UL, tracker.ConfirmedCount);
    }

    [Fact]
    public void SimpleMajorityReleasesOnlyNewlyConfirmedFifoPrefix()
    {
        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);
        JointConfig voters = Voters(1, 2, 3);
        tracker.AddRequest(10, Request("first"));
        tracker.AddRequest(20, Request("second"));
        tracker.AddRequest(30, Request("third"));

        AcknowledgeAt(tracker, 1, 3);
        AcknowledgeAt(tracker, 2, 1);

        ReadIndexRequest first = Assert.Single(tracker.Advance(voters));
        Assert.Equal(10UL, first.Index);
        Assert.Equal("first", RequestContext(first.Request));
        Assert.Equal(2, tracker.PendingCount);
        Assert.Equal(1UL, tracker.ConfirmedCount);

        AcknowledgeAt(tracker, 2, 3);

        ReadIndexRequest[] remainder = tracker.Advance(voters);
        Assert.Equal([20UL, 30UL], remainder.Select(item => item.Index));
        Assert.Equal(
            ["second", "third"],
            remainder.Select(item => RequestContext(item.Request)));
        Assert.Equal(0, tracker.PendingCount);
        Assert.Equal(3UL, tracker.ConfirmedCount);
        Assert.Equal(ByteString.Empty, tracker.GetHeartbeatContext());
        Assert.Empty(tracker.Advance(voters));
    }

    [Fact]
    public void JointConfigRequiresBothMajorities()
    {
        var voters = new JointConfig(
            new MajorityConfig([1, 2, 3]),
            new MajorityConfig([3, 4, 5]));
        var incomingOnly = new ReadOnlyTracker(ReadOnlyOption.Safe);
        incomingOnly.AddRequest(10, Request("incoming"));

        AcknowledgeAt(incomingOnly, 1, 1);
        AcknowledgeAt(incomingOnly, 2, 1);
        AcknowledgeAt(incomingOnly, 3, 1);

        Assert.Empty(incomingOnly.Advance(voters));

        AcknowledgeAt(incomingOnly, 4, 1);
        ReadIndexRequest incoming = Assert.Single(
            incomingOnly.Advance(voters));
        Assert.Equal("incoming", RequestContext(incoming.Request));

        var outgoingOnly = new ReadOnlyTracker(ReadOnlyOption.Safe);
        outgoingOnly.AddRequest(20, Request("outgoing"));
        AcknowledgeAt(outgoingOnly, 4, 1);
        AcknowledgeAt(outgoingOnly, 5, 1);

        Assert.Empty(outgoingOnly.Advance(voters));

        AcknowledgeAt(outgoingOnly, 1, 1);
        AcknowledgeAt(outgoingOnly, 2, 1);
        ReadIndexRequest outgoing = Assert.Single(
            outgoingOnly.Advance(voters));
        Assert.Equal("outgoing", RequestContext(outgoing.Request));
    }

    [Fact]
    public void LearnerAcknowledgementDoesNotAffectVoterQuorum()
    {
        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);
        JointConfig voters = Voters(1, 2, 3);
        tracker.AddRequest(10, Request("first"));

        AcknowledgeAt(tracker, 1, 1);
        AcknowledgeAt(tracker, 4, 1);
        Assert.Empty(tracker.Advance(voters));

        AcknowledgeAt(tracker, 2, 1);
        Assert.Single(tracker.Advance(voters));
    }

    [Fact]
    public void DuplicateAndStaleAcknowledgementsDoNotReleaseTwice()
    {
        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);
        JointConfig voters = Voters(1, 2, 3);
        tracker.AddRequest(10, Request("first"));

        AcknowledgeAt(tracker, 1, 1);
        AcknowledgeAt(tracker, 2, 1);
        Assert.Single(tracker.Advance(voters));

        AcknowledgeAt(tracker, 1, 1);
        AcknowledgeAt(tracker, 2, 1);

        Assert.Empty(tracker.Advance(voters));
        Assert.Equal(0, tracker.PendingCount);
        Assert.Equal(1UL, tracker.ConfirmedCount);
    }

    [Fact]
    public void ConfirmedRequestIsNotRetainedByLiveTracker()
    {
        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);

        WeakReference confirmedRequest = ConfirmAndForgetRequest(tracker);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.False(confirmedRequest.IsAlive);
        Assert.Equal(0, tracker.PendingCount);
        GC.KeepAlive(tracker);
    }

    [Fact]
    public void ImpossibleQuorumPositionFailsWithoutChangingQueue()
    {
        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);
        tracker.AddRequest(10, Request("first"));
        var emptyVoters = new JointConfig(new MajorityConfig());

        Assert.Throws<RaftInvariantException>(
            () => tracker.Advance(emptyVoters));

        Assert.Equal(1, tracker.PendingCount);
        Assert.Equal(0UL, tracker.ConfirmedCount);
        Assert.Equal(1UL, Decode(tracker.GetHeartbeatContext()));
    }

    [Fact]
    public void NullArgumentsFailExplicitly()
    {
        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);

        Assert.Throws<ArgumentNullException>(
            () => tracker.AddRequest(0, null!));
        Assert.Throws<ArgumentNullException>("context",
            () => tracker.ParseAcknowledgement(null!, fromPeer: true));
        Assert.Throws<ArgumentNullException>(
            () => tracker.Advance(null!));
    }

    [Fact]
    public void DefaultAcknowledgementIsNotEvidence()
    {
        var tracker = new ReadOnlyTracker(ReadOnlyOption.Safe);
        tracker.AddRequest(10, Request("first"));

        Assert.Throws<RaftInvariantException>(() => tracker.ReceiveAcknowledgement(1, default));
        Assert.Empty(tracker.Advance(Voters(1)));
    }

    private static void AcknowledgeAt(ReadOnlyTracker tracker, ulong from, ulong position)
    {
        ReadAcknowledgement? acknowledgement = tracker.ParseAcknowledgement(Encode(position), fromPeer: true);
        Assert.True(acknowledgement.HasValue);
        tracker.ReceiveAcknowledgement(from, acknowledgement.Value);
    }

    private static JointConfig Voters(params ulong[] ids)
    {
        return new JointConfig(new MajorityConfig(ids));
    }

    private static Message Request(string context)
    {
        var message = new Message
        {
            Type = MessageType.MsgReadIndex,
        };
        message.Entries.Add(new Entry
        {
            Data = ByteString.CopyFromUtf8(context),
        });
        return message;
    }

    private static string RequestContext(Message request)
    {
        return request.Entries[0].Data.ToStringUtf8();
    }

    private static ByteString Encode(ulong position)
    {
        var bytes = new byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes, position);
        return ByteString.CopyFrom(bytes);
    }

    private static ulong Decode(ByteString context)
    {
        return BinaryPrimitives.ReadUInt64LittleEndian(context.Span);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference ConfirmAndForgetRequest(
        ReadOnlyTracker tracker)
    {
        ReadAcknowledgement local = tracker.AddRequest(10, Request("collectable"));
        tracker.ReceiveAcknowledgement(1, local);
        ReadIndexRequest confirmed = Assert.Single(
            tracker.Advance(Voters(1)));
        return new WeakReference(confirmed.Request);
    }
}
