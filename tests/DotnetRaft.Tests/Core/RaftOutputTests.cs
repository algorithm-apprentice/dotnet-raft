using DotnetRaft.Core;
using DotnetRaft.Protocol;

namespace DotnetRaft.Tests.Core;

public sealed class RaftOutputTests
{
    [Fact]
    public void ImmediateEnqueueNormalizesAndOwnsMessage()
    {
        var output = new RaftOutput();
        var message = new Message
        {
            To = 2,
            Type = MessageType.MsgHeartbeat,
        };
        Message original = message.Clone();

        Message outbound = output.Enqueue(
            message,
            senderId: 1,
            currentTerm: 5);

        Assert.Equal(original, message);
        Assert.Equal(1UL, outbound.From);
        Assert.Equal(2UL, outbound.To);
        Assert.Equal(5UL, outbound.Term);
        Assert.True(outbound.HasTerm);
        Assert.True(output.HasMessages);
        Assert.False(output.HasMessagesAfterAppend);

        Message[] peeked = output.PeekMessages();
        Message peek = Assert.Single(peeked);
        peek.Term = 9;
        Assert.Equal(
            5UL,
            Assert.Single(
                output.PeekMessages()).Term);

        Message taken = Assert.Single(
            output.TakeMessages());
        Assert.Equal(5UL, taken.Term);
        Assert.False(output.HasMessages);
        Assert.Empty(output.TakeMessages());
    }

    [Fact]
    public void DurableResponseUsesAfterAppendQueueBeforeSelfCheck()
    {
        var output = new RaftOutput();

        Message outbound = output.Enqueue(
            new Message
            {
                To = 1,
                Type = MessageType.MsgAppResp,
            },
            senderId: 1,
            currentTerm: 5);

        Assert.Equal(5UL, outbound.Term);
        Assert.False(output.HasMessages);
        Assert.True(output.HasMessagesAfterAppend);
        Message durable = Assert.Single(
            output.TakeMessagesAfterAppend());
        Assert.Equal(MessageType.MsgAppResp, durable.Type);
        Assert.Equal(1UL, durable.To);
        Assert.False(output.HasMessagesAfterAppend);
    }

    [Fact]
    public void ProposalAndReadIndexPreserveAbsentTerm()
    {
        foreach (MessageType type in new[]
                 {
                     MessageType.MsgProp,
                     MessageType.MsgReadIndex,
                 })
        {
            var output = new RaftOutput();

            Message outbound = output.Enqueue(
                new Message
                {
                    To = 2,
                    Type = type,
                },
                senderId: 1,
                currentTerm: 5);

            Assert.Equal(0UL, outbound.Term);
            Assert.False(outbound.HasTerm);
        }
    }

    [Fact]
    public void VoteAndNonVoteTermRulesRemainExact()
    {
        var output = new RaftOutput();

        RaftInvariantException missingVoteTerm =
            Assert.Throws<RaftInvariantException>(
                () => output.Enqueue(
                    new Message
                    {
                        To = 2,
                        Type = MessageType.MsgVote,
                    },
                    senderId: 1,
                    currentTerm: 5));
        Assert.Equal(
            "Term must be set when sending MsgVote.",
            missingVoteTerm.Message);

        RaftInvariantException suppliedTerm =
            Assert.Throws<RaftInvariantException>(
                () => output.Enqueue(
                    new Message
                    {
                        To = 2,
                        Type = MessageType.MsgHeartbeat,
                        Term = 4,
                    },
                    senderId: 1,
                    currentTerm: 5));
        Assert.Equal(
            "Term must not be set when sending MsgHeartbeat.",
            suppliedTerm.Message);

        Assert.False(output.HasMessages);
        Assert.False(output.HasMessagesAfterAppend);
    }

    [Fact]
    public void ImmediateSelfTargetFailsBeforeQueueMutation()
    {
        var output = new RaftOutput();

        RaftInvariantException exception =
            Assert.Throws<RaftInvariantException>(
                () => output.Enqueue(
                    new Message
                    {
                        To = 1,
                        Type = MessageType.MsgHeartbeat,
                    },
                    senderId: 1,
                    currentTerm: 5));

        Assert.Equal(
            "Immediate outbound MsgHeartbeat cannot target the local node.",
            exception.Message);
        Assert.False(output.HasMessages);
        Assert.False(output.HasMessagesAfterAppend);
    }
}
