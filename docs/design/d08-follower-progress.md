# D08 Follower Progress Design

Historical [port milestone](README.md#completed-reference-port). See the
[replication walkthrough](../learning/02-log-and-replication.md) for current
probe, replicate, and snapshot examples.

[`Progress`](../../src/DotnetRaft/Tracker/Progress.cs) separates evidence
(`Match`) from a prediction (`Next`). Within a follower's progress record, a
send can move `Next` optimistically but only acknowledgement can raise `Match`.
Probe limits how far the leader speculates when matching is uncertain;
replicate pipelines known progress; snapshot mode pauses appends while an
inaccessible prefix is being repaired.

Transport feedback can leave snapshot mode without proving a match. This is
why a successful `ReportSnapshot` is not a replication vote. See the
[progress tests](../../tests/DotnetRaft.Tests/Tracker/ProgressTests.cs).
