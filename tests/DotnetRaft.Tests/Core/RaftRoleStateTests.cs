using DotnetRaft.Core;
using DotnetRaft.Protocol;

namespace DotnetRaft.Tests.Core;

public sealed class RaftRoleStateTests
{
    [Fact]
    public void ResetClearsLeaderAndTransferButPreservesRole()
    {
        var state = new RaftRoleState();
        state.Load(term: 5, vote: 2);
        state.BecomeLeader(1);
        state.StartTransfer(3);

        state.Reset(5);

        Assert.Equal(5UL, state.Term);
        Assert.Equal(2UL, state.Vote);
        Assert.Equal(
            RaftMessageTargets.None,
            state.LeaderId);
        Assert.Equal(
            RaftMessageTargets.None,
            state.LeaderTransferee);
        Assert.Equal(RaftRole.Leader, state.Role);

        state.Reset(6);

        Assert.Equal(6UL, state.Term);
        Assert.Equal(
            RaftMessageTargets.None,
            state.Vote);
        Assert.Equal(RaftRole.Leader, state.Role);
    }

    [Fact]
    public void RoleTransitionsMutateOnlyOwnedScalars()
    {
        var state = new RaftRoleState();
        state.Load(term: 4, vote: 2);

        state.BecomeFollower(3);
        Assert.Equal(RaftRole.Follower, state.Role);
        Assert.Equal(3UL, state.LeaderId);
        Assert.Equal(2UL, state.Vote);

        state.BecomePreCandidate();
        Assert.Equal(
            RaftRole.PreCandidate,
            state.Role);
        Assert.Equal(
            RaftMessageTargets.None,
            state.LeaderId);
        Assert.Equal(2UL, state.Vote);

        state.BecomeCandidate(1);
        Assert.Equal(RaftRole.Candidate, state.Role);
        Assert.Equal(1UL, state.Vote);

        state.BecomeLeader(1);
        Assert.Equal(RaftRole.Leader, state.Role);
        Assert.Equal(1UL, state.LeaderId);
        Assert.Equal(1UL, state.Vote);
        Assert.Equal(4UL, state.Term);
    }

    [Fact]
    public void LeaderVoteAndTransferMutationsAreExplicit()
    {
        var state = new RaftRoleState();

        state.SetLeader(2);
        Assert.Equal(2UL, state.LeaderId);
        state.ForgetLeader();
        Assert.Equal(
            RaftMessageTargets.None,
            state.LeaderId);

        state.GrantVote(3);
        Assert.Equal(3UL, state.Vote);

        state.StartTransfer(4);
        Assert.Equal(4UL, state.LeaderTransferee);
        state.AbortTransfer();
        Assert.Equal(
            RaftMessageTargets.None,
            state.LeaderTransferee);
    }

    [Fact]
    public void StateSnapshotsAreFreshAndComplete()
    {
        var state = new RaftRoleState();
        state.Load(term: 5, vote: 2);
        state.BecomeCandidate(1);
        state.SetLeader(3);
        state.StartTransfer(4);

        SoftState soft = state.GetSoftState();
        HardState hard = state.GetHardState(
            commit: 7);

        Assert.Equal(
            new SoftState(3, RaftRole.Candidate),
            soft);
        Assert.Equal(5UL, hard.Term);
        Assert.Equal(1UL, hard.Vote);
        Assert.Equal(7UL, hard.Commit);

        hard.Term = 9;
        hard.Vote = 9;
        Assert.Equal(5UL, state.Term);
        Assert.Equal(1UL, state.Vote);
        Assert.Equal(4UL, state.LeaderTransferee);
    }
}
