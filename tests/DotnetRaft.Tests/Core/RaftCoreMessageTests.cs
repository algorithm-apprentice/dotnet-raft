using System.Runtime.CompilerServices;

using DotnetRaft.Core;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreMessageTests
{
    [Fact]
    public void SendFillsSenderClonesInputAndAttachesCurrentTerm()
    {
        RaftCore core = NewCore(term: 5);
        var original = new Message
        {
            To = 2,
            Type = MessageType.MsgHeartbeat,
            Context = ByteString.CopyFromUtf8("original"),
        };

        core.Send(original);

        original.To = 3;
        original.Context = ByteString.CopyFromUtf8("changed");
        Message sent = Assert.Single(core.TakeMessages());

        Assert.NotSame(original, sent);
        Assert.Equal(1UL, sent.From);
        Assert.Equal(2UL, sent.To);
        Assert.Equal(5UL, sent.Term);
        Assert.Equal("original", sent.Context.ToStringUtf8());
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Theory]
    [InlineData(MessageType.MsgVote, false)]
    [InlineData(MessageType.MsgVoteResp, true)]
    [InlineData(MessageType.MsgPreVote, false)]
    [InlineData(MessageType.MsgPreVoteResp, true)]
    public void VoteFamilyRequiresAndPreservesExplicitTerm(
        MessageType type,
        bool afterAppend)
    {
        RaftCore core = NewCore(term: 5);

        Assert.Throws<RaftInvariantException>(
            () => core.Send(new Message
            {
                To = 2,
                Type = type,
            }));
        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());

        core.Send(new Message
        {
            To = 2,
            Type = type,
            Term = 6,
        });

        Message sent = Assert.Single(
            afterAppend
                ? core.TakeMessagesAfterAppend()
                : core.TakeMessages());
        Assert.Equal(6UL, sent.Term);
    }

    [Fact]
    public void OrdinaryMessageRejectsExplicitTermBeforeQueueMutation()
    {
        RaftCore core = NewCore(term: 5);

        Assert.Throws<RaftInvariantException>(
            () => core.Send(new Message
            {
                To = 2,
                Type = MessageType.MsgHeartbeat,
                Term = 4,
            }));

        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Theory]
    [InlineData(MessageType.MsgProp)]
    [InlineData(MessageType.MsgReadIndex)]
    public void ForwardedLocalRequestsRetainZeroTerm(MessageType type)
    {
        RaftCore core = NewCore(term: 5);

        core.Send(new Message
        {
            To = 2,
            Type = type,
        });

        Message sent = Assert.Single(core.TakeMessages());
        Assert.Equal(0UL, sent.Term);
        Assert.False(sent.HasTerm);
    }

    [Fact]
    public void DurabilityDependentResponsesUseAfterAppendQueue()
    {
        RaftCore core = NewCore(term: 5);
        core.Send(new Message
        {
            To = 1,
            Type = MessageType.MsgAppResp,
            Reject = true,
        });
        core.Send(new Message
        {
            To = 1,
            Type = MessageType.MsgVoteResp,
            Term = 5,
            Reject = true,
        });
        core.Send(new Message
        {
            To = 2,
            Type = MessageType.MsgPreVoteResp,
            Term = 6,
        });

        Assert.Empty(core.TakeMessages());
        Message[] durable = core.TakeMessagesAfterAppend();

        Assert.Equal(
            [
                MessageType.MsgAppResp,
                MessageType.MsgVoteResp,
                MessageType.MsgPreVoteResp,
            ],
            durable.Select(message => message.Type));
        Assert.Equal([1UL, 1UL, 2UL], durable.Select(message => message.To));
        Assert.True(durable[0].Reject);
        Assert.True(durable[1].Reject);
    }

    [Fact]
    public void OtherSelfAddressedOutputFailsBeforeQueueMutation()
    {
        RaftCore core = NewCore(term: 5);

        Assert.Throws<RaftInvariantException>(
            () => core.Send(new Message
            {
                To = 1,
                Type = MessageType.MsgHeartbeat,
            }));

        Assert.Empty(core.TakeMessages());
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void TakingQueuesPreservesOrderAndDoesNotDrainOtherQueue()
    {
        RaftCore core = NewCore(term: 5);
        core.Send(new Message
        {
            To = 2,
            Type = MessageType.MsgHeartbeat,
            Index = 1,
        });
        core.Send(new Message
        {
            To = 3,
            Type = MessageType.MsgApp,
            Index = 2,
        });
        core.Send(new Message
        {
            To = 1,
            Type = MessageType.MsgAppResp,
            Index = 3,
        });
        core.Send(new Message
        {
            To = 2,
            Type = MessageType.MsgVoteResp,
            Term = 5,
            Index = 4,
        });

        Message[] immediate = core.TakeMessages();
        Assert.Equal([1UL, 2UL], immediate.Select(message => message.Index));
        Assert.Empty(core.TakeMessages());

        Message[] durable = core.TakeMessagesAfterAppend();
        Assert.Equal([3UL, 4UL], durable.Select(message => message.Index));
        Assert.Empty(core.TakeMessagesAfterAppend());
    }

    [Fact]
    public void RoleTransitionsPreserveBothQueuesAndOriginalTerms()
    {
        RaftCore core = NewCore(term: 5);
        core.Send(new Message
        {
            To = 2,
            Type = MessageType.MsgHeartbeat,
            Context = ByteString.CopyFromUtf8("immediate"),
        });
        core.Send(new Message
        {
            To = 2,
            Type = MessageType.MsgVoteResp,
            Term = 5,
            Context = ByteString.CopyFromUtf8("durable"),
        });

        core.BecomeFollower(5, 2);
        core.BecomeFollower(6, 0);
        core.BecomePreCandidate();

        Message immediate = Assert.Single(core.TakeMessages());
        Message durable = Assert.Single(core.TakeMessagesAfterAppend());
        Assert.Equal(5UL, immediate.Term);
        Assert.Equal(5UL, durable.Term);
        Assert.Equal("immediate", immediate.Context.ToStringUtf8());
        Assert.Equal("durable", durable.Context.ToStringUtf8());
    }

    [Fact]
    public void TakenMessagesAreNotRetainedByLiveCore()
    {
        RaftCore core = NewCore(term: 5);

        WeakReference sent = SendTakeAndForget(core);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.False(sent.IsAlive);
        GC.KeepAlive(core);
    }

    private static RaftCore NewCore(ulong term)
    {
        var storage = new CoreTestStorage
        {
            InitialState = new StorageState(
                new HardState
                {
                    Term = term,
                },
                RaftCoreInitializationTests.ConfState(
                    voters: [1, 2, 3])),
        };

        return new RaftCore(
            new RaftConfig
            {
                Id = 1,
                Storage = storage,
            },
            _ => 0);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference SendTakeAndForget(RaftCore core)
    {
        core.Send(new Message
        {
            To = 2,
            Type = MessageType.MsgHeartbeat,
        });
        Message sent = Assert.Single(core.TakeMessages());
        return new WeakReference(sent);
    }
}
