# D15 Replication Flow Control Design

Historical [port milestone](README.md#completed-reference-port). The
[replication walkthrough](../learning/02-log-and-replication.md)
works through a conflicting suffix and explains the different size limits.

Rejection hints skip runs of conflicting terms rather than requiring one
round trip per entry. They are hints for the next probe, not evidence that
the peer matched anything. A delayed rejection must not undo acknowledged
progress. A paused follower has its own inflight budget, so it need not hold
up traffic to the rest of the quorum.

See [rejection hint tests](../../tests/DotnetRaft.Tests/Core/RaftCoreRejectionHintTests.cs)
and [flow-control tests](../../tests/DotnetRaft.Tests/Core/RaftCoreFlowControlTests.cs)
for stale responses, retries, and admission limits.
