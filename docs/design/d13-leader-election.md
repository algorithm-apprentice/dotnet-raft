# D13 Leader Election Design

Historical [port milestone](README.md#completed-reference-port). See
[election safety](../learning/01-safety-and-elections.md)
for the proof, current path, and a split-vote exercise.

The election needs two independent protections: a voter checks the
candidate's last term and index, and its vote must survive restart before
the grant becomes visible. Real self-votes participate in the same durability
boundary; a candidate does not become a leader merely by updating its own
in-memory tally.

Term routing is shared by every role, not implemented inside just the vote
handler. D19 adds pre-vote and active-leader exceptions. See the
[election tests](../../tests/DotnetRaft.Tests/Core/RaftCoreElectionTests.cs)
and the [pre-vote tests](../../tests/DotnetRaft.Tests/Core/RaftCorePreVoteTests.cs).
