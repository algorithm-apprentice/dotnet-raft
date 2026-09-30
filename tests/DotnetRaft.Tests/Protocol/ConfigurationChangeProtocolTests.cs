using DotnetRaft.ConfChange;
using DotnetRaft.Protocol;

using Google.Protobuf;

using LegacyConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Protocol;

public sealed class ConfigurationChangeProtocolTests
{
    [Fact]
    public void LegacyChangeConvertsToEquivalentV2Change()
    {
        var context = ByteString.CopyFromUtf8("context");
        var change = new LegacyConfChange
        {
            Type = ConfChangeType.ConfChangeAddLearnerNode,
            NodeId = 42,
            Context = context,
        };

        ConfChangeV2 converted = change.AsV2();

        ConfChangeSingle single = Assert.Single(converted.Changes);
        Assert.Equal(change.Type, single.Type);
        Assert.Equal(change.NodeId, single.NodeId);
        Assert.Equal(context, converted.Context);
    }

    [Fact]
    public void LeaveJointClassificationMatchesPinnedTable()
    {
        var cases = new[]
        {
            new LeaveCase(
                "empty",
                new ConfChangeV2(),
                true),
            new LeaveCase(
                "explicit-auto",
                new ConfChangeV2
                {
                    Transition =
                        ConfChangeTransition.Auto,
                },
                true),
            new LeaveCase(
                "context-only",
                new ConfChangeV2
                {
                    Transition =
                        ConfChangeTransition.Auto,
                    Context = ByteString.CopyFromUtf8("context"),
                },
                true),
            new LeaveCase(
                "one-change",
                ChangeV2(
                    ConfChangeTransition.Auto,
                    Voter(1)),
                false),
            new LeaveCase(
                "implicit-empty",
                new ConfChangeV2
                {
                    Transition =
                        ConfChangeTransition.JointImplicit,
                },
                false),
            new LeaveCase(
                "explicit-empty",
                new ConfChangeV2
                {
                    Transition =
                        ConfChangeTransition.JointExplicit,
                },
                false),
            new LeaveCase(
                "auto-multiple",
                ChangeV2(
                    ConfChangeTransition.Auto,
                    Voter(1),
                    Remove(2)),
                false),
        };

        foreach (LeaveCase testCase in cases)
        {
            Assert.Equal(
                testCase.Expected,
                testCase.Change.IsLeaveJoint());
        }
    }

    [Fact]
    public void JointTransitionClassificationMatchesPinnedRules()
    {
        Assert.False(
            new ConfChangeV2().TryGetJointTransition(
                out bool emptyAutoLeave));
        Assert.False(emptyAutoLeave);

        Assert.False(
            ChangeV2(
                    ConfChangeTransition.Auto,
                    Voter(1))
                .TryGetJointTransition(out bool singleAutoLeave));
        Assert.False(singleAutoLeave);

        Assert.True(
            ChangeV2(
                    ConfChangeTransition.Auto,
                    Voter(1),
                    Remove(2))
                .TryGetJointTransition(out bool multipleAutoLeave));
        Assert.True(multipleAutoLeave);

        Assert.True(
            ChangeV2(
                    ConfChangeTransition.JointImplicit,
                    Voter(1))
                .TryGetJointTransition(out bool implicitAutoLeave));
        Assert.True(implicitAutoLeave);

        Assert.True(
            ChangeV2(
                    ConfChangeTransition.JointExplicit,
                    Voter(1))
                .TryGetJointTransition(out bool explicitAutoLeave));
        Assert.False(explicitAutoLeave);

        var unknown = new ConfChangeV2
        {
            Transition = (ConfChangeTransition)99,
        };
        Assert.Throws<ConfigurationChangeException>(
            () => unknown.TryGetJointTransition(out _));
    }

    private static ConfChangeV2 ChangeV2(
        ConfChangeTransition transition,
        params ConfChangeSingle[] changes)
    {
        var result = new ConfChangeV2
        {
            Transition = transition,
        };
        result.Changes.Add(changes);
        return result;
    }

    private static ConfChangeSingle Voter(ulong id)
    {
        return new ConfChangeSingle
        {
            Type = ConfChangeType.ConfChangeAddNode,
            NodeId = id,
        };
    }

    private static ConfChangeSingle Remove(ulong id)
    {
        return new ConfChangeSingle
        {
            Type = ConfChangeType.ConfChangeRemoveNode,
            NodeId = id,
        };
    }

    private sealed record LeaveCase(
        string Name,
        ConfChangeV2 Change,
        bool Expected)
    {
        public override string ToString()
        {
            return Name;
        }
    }
}
