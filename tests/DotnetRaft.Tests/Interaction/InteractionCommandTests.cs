using DotnetRaft.Protocol;

using Google.Protobuf;

namespace DotnetRaft.Tests.Interaction;

public sealed class InteractionCommandTests
{
    [Fact]
    public void QuotedProposalRetainsQuotes()
    {
        var environment = new InteractionEnvironment();

        Assert.Equal(
            "ok",
            environment.Handle(
                "log-level none",
                string.Empty));
        Assert.Equal(
            "ok",
            environment.Handle(
                "add-nodes 1 voters=(1) index=2",
                string.Empty));
        Assert.Equal(
            "ok",
            environment.Handle(
                "campaign 1",
                string.Empty));
        Assert.Equal(
            "ok",
            environment.Handle(
                "stabilize",
                string.Empty));
        Assert.Equal(
            "ok",
            environment.Handle(
                "propose 1 \"foo\"",
                string.Empty));
        Assert.Equal(
            "ok",
            environment.Handle(
                "stabilize",
                string.Empty));

        Assert.Equal(
            ByteString.CopyFromUtf8("\"foo\""),
            environment.Nodes[0]
                .GetApplicationSnapshot()
                .Data);
    }

    [Fact]
    public void ProposalWithQuotedWhitespaceFailsArity()
    {
        var environment = new InteractionEnvironment();

        string result = environment.Handle(
            "propose 1 \"foo bar\"",
            string.Empty);

        Assert.Contains(
            "exactly one payload token",
            result,
            StringComparison.Ordinal);
    }

    [Fact]
    public void StabilizeLogLevelOverrideIsTemporary()
    {
        var environment = new InteractionEnvironment();
        environment.Handle(
            "log-level none",
            string.Empty);
        environment.Handle(
            "add-nodes 1 voters=(1) index=2",
            string.Empty);
        environment.Handle(
            "campaign 1",
            string.Empty);

        Assert.NotEqual(
            "ok",
            environment.Handle(
                "stabilize log-level=debug",
                string.Empty));
        Assert.Equal(
            "ok",
            environment.Handle(
                "raft-state",
                string.Empty));
    }

    [Fact]
    public void ConfigurationCommandSupportsV1AndLeaveJoint()
    {
        var environment = new InteractionEnvironment();
        environment.Handle(
            "log-level none",
            string.Empty);
        environment.Handle(
            "add-nodes 1 voters=(1) index=2",
            string.Empty);
        environment.Handle(
            "campaign 1",
            string.Empty);
        environment.Handle(
            "stabilize",
            string.Empty);

        Assert.Equal(
            "ok",
            environment.Handle(
                "propose-conf-change 1 v1=true",
                "l2"));
        Assert.Equal(
            "ok",
            environment.Handle(
                "propose-conf-change 1 transition=explicit",
                "v3 r1"));
        Assert.Equal(
            "ok",
            environment.Handle(
                "propose-conf-change 1",
                string.Empty));
    }

    [Fact]
    public void AsyncWorkerCommandsAreAvailable()
    {
        var environment = new InteractionEnvironment();
        environment.Handle(
            "log-level none",
            string.Empty);
        environment.Handle(
            "add-nodes 1 async-storage-writes=true",
            string.Empty);
        environment.Handle(
            "log-level debug",
            string.Empty);

        string append = environment.Handle(
            "process-append-thread 1",
            string.Empty);
        string apply = environment.Handle(
            "process-apply-thread 1",
            string.Empty);

        Assert.Equal(
            "no append work to perform",
            append);
        Assert.Equal(
            "no apply work to perform",
            apply);
    }

    [Fact]
    public void EmptyRaftLogUsesPinnedText()
    {
        var environment = new InteractionEnvironment();
        environment.Handle(
            "log-level none",
            string.Empty);
        environment.Handle(
            "add-nodes 1 voters=(1) index=5",
            string.Empty);
        environment.Handle(
            "log-level debug",
            string.Empty);

        Assert.Equal(
            "log is empty: first index=6, last index=5",
            environment.Handle(
                "raft-log 1",
                string.Empty));
    }

    [Fact]
    public void DeliveryPreservesDirectiveRecipientOrder()
    {
        var environment = new InteractionEnvironment();
        environment.AddNodes(
            2,
            new InteractionNodeOptions());
        environment.QueueMessageForTesting(
            new Message
            {
                From = 1,
                To = 2,
                Type = MessageType.MsgHeartbeat,
            });
        environment.QueueMessageForTesting(
            new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgHeartbeat,
            });

        string result = environment.Handle(
            "deliver-msgs drop=(2) 1",
            string.Empty);

        int dropped = result.IndexOf(
            "dropped: 1->2",
            StringComparison.Ordinal);
        int delivered = result.IndexOf(
            "2->1",
            StringComparison.Ordinal);
        Assert.True(dropped >= 0);
        Assert.True(dropped < delivered);
    }

    [Fact]
    public void MissingDeliveryDestinationFailsBeforeQueueMutation()
    {
        var environment = new InteractionEnvironment();

        string result = environment.Handle(
            "deliver-msgs 99",
            string.Empty);

        Assert.Contains(
            "Node 99 is not instantiated.",
            result,
            StringComparison.Ordinal);
    }

    [Fact]
    public void RuntimeLexicalErrorsIncludeSourceLocation()
    {
        var environment = new InteractionEnvironment();

        string result = environment.Handle(
            "add-nodes nope",
            string.Empty,
            "scenario.txt",
            12);

        Assert.Contains(
            "scenario.txt:12",
            result,
            StringComparison.Ordinal);
        Assert.Contains(
            "count value nope",
            result,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ExactCommandContractsRejectExtraOptions()
    {
        var environment = new InteractionEnvironment();

        string timing = environment.Handle(
            "add-nodes 1 election-tick=10",
            string.Empty);
        Assert.Contains(
            "Unknown add-nodes option election-tick",
            timing,
            StringComparison.Ordinal);

        environment.Handle(
            "log-level none",
            string.Empty);
        environment.Handle(
            "add-nodes 1 voters=(1) index=2",
            string.Empty);
        environment.Handle(
            "campaign 1",
            string.Empty);
        environment.Handle(
            "stabilize",
            string.Empty);
        string compact = environment.Handle(
            "compact 1 2 ignored=value",
            string.Empty);
        Assert.Contains(
            "compact does not accept keyed options",
            compact,
            StringComparison.Ordinal);
    }
}
