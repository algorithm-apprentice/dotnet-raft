# D19 Availability Extensions Design

Historical [port milestone](README.md#completed-reference-port). See the
[availability walkthrough](../learning/04-membership-reads-and-liveness.md#put-availability-features-in-their-place)
and the [current read contract](../public-api.md#read-index).

`PreVote` tests whether an election could succeed before raising the term;
`CheckQuorum` makes a leader step down after losing recent quorum activity.
They address different problems, and neither substitutes for a read barrier.
Lease-based reads depend on `CheckQuorum` and on host clock assumptions. A
follower must not discard its leader hint via `ForgetLeader` in lease-based
mode: doing so could bypass the protection against disruptive votes.

See [pre-vote tests](../../tests/DotnetRaft.Tests/Core/RaftCorePreVoteTests.cs)
and [forget-leader and lease tests](../../tests/DotnetRaft.Tests/Core/RaftCoreForgetLeaderAndLeaseReadTests.cs).
