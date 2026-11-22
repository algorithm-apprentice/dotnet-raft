# D09 Progress Tracker Design

Historical [port milestone](README.md#completed-reference-port). See the
[C# ownership map](../reference-architecture.md#implemented-c-core-boundaries)
and the [joint-quorum walkthrough](../learning/04-membership-reads-and-liveness.md).

[`ProgressTracker`](../../src/DotnetRaft/Tracker/ProgressTracker.cs) combines
the installed configuration with per-node evidence. The voter sets decide
which records count: a learner can have a larger `Match` than every voter and
still cannot move the quorum commit point. Joint commitment takes the smaller
of the two majority indexes. The leader's current-term commit check remains
in consensus, not in the quorum calculation.

See [tracker tests](../../tests/DotnetRaft.Tests/Tracker/ProgressTrackerTests.cs)
for votes, activity, and commit derivation.
