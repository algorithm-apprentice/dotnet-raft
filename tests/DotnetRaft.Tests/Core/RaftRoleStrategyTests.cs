using DotnetRaft.Core;

namespace DotnetRaft.Tests.Core;

public sealed class RaftRoleStrategyTests
{
    [Fact]
    public void ResolverReturnsCachedStrategyForEveryRole()
    {
        var cases = new[]
        {
            (
                RaftRole.Follower,
                typeof(FollowerRoleStrategy)),
            (
                RaftRole.PreCandidate,
                typeof(PreCandidateRoleStrategy)),
            (
                RaftRole.Candidate,
                typeof(CandidateRoleStrategy)),
            (
                RaftRole.Leader,
                typeof(LeaderRoleStrategy)),
        };

        foreach ((RaftRole role, Type type) in cases)
        {
            IRaftRoleStrategy first =
                RaftRoleStrategies.Resolve(role);
            IRaftRoleStrategy second =
                RaftRoleStrategies.Resolve(role);

            Assert.IsType(type, first);
            Assert.Same(first, second);
        }
    }

    [Fact]
    public void ResolverRejectsUnknownRole()
    {
        RaftInvariantException exception =
            Assert.Throws<RaftInvariantException>(
                () => RaftRoleStrategies.Resolve(
                    (RaftRole)99));

        Assert.Equal(
            "Unknown Raft role 99.",
            exception.Message);
    }
}
