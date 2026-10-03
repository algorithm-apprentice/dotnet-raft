using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal sealed class RaftRoleState
{
    internal ulong Term { get; private set; }

    internal ulong Vote { get; private set; }

    internal ulong LeaderId { get; private set; }

    internal RaftRole Role { get; private set; }

    internal ulong LeaderTransferee { get; private set; }

    internal void Reset(ulong term)
    {
        if (Term != term)
        {
            Term = term;
            Vote = RaftMessageTargets.None;
        }

        LeaderId = RaftMessageTargets.None;
        LeaderTransferee = RaftMessageTargets.None;
    }

    internal void BecomeFollower(ulong leaderId)
    {
        LeaderId = leaderId;
        Role = RaftRole.Follower;
    }

    internal void BecomeCandidate(ulong localId)
    {
        Vote = localId;
        Role = RaftRole.Candidate;
    }

    internal void BecomePreCandidate()
    {
        LeaderId = RaftMessageTargets.None;
        Role = RaftRole.PreCandidate;
    }

    internal void BecomeLeader(ulong localId)
    {
        LeaderId = localId;
        Role = RaftRole.Leader;
    }

    internal void GrantVote(ulong candidateId)
    {
        Vote = candidateId;
    }

    internal void SetLeader(ulong leaderId)
    {
        LeaderId = leaderId;
    }

    internal void ForgetLeader()
    {
        LeaderId = RaftMessageTargets.None;
    }

    internal void StartTransfer(ulong transferee)
    {
        LeaderTransferee = transferee;
    }

    internal void AbortTransfer()
    {
        LeaderTransferee = RaftMessageTargets.None;
    }

    internal void Load(ulong term, ulong vote)
    {
        Term = term;
        Vote = vote;
    }

    internal SoftState GetSoftState()
    {
        return new SoftState(LeaderId, Role);
    }

    internal HardState GetHardState(ulong commit)
    {
        return new HardState
        {
            Term = Term,
            Vote = Vote,
            Commit = commit,
        };
    }
}
