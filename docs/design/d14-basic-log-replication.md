# D14 Basic Log Replication Design

Historical [port milestone](README.md#completed-reference-port). See the
[log walkthrough](../learning/02-log-and-replication.md)
for current append and heartbeat behavior.

An append proves a preceding tuple and may then replace an uncommitted
suffix. A heartbeat does not prove a match, so it cannot serve as an append
acknowledgement. Even the leader counts its own append through a deferred
self-response, not as soon as it writes to memory.

Committing the new leader's no-op can make inherited entries committed as a
prefix. A quorum of copies of an older-term entry is not enough to advance
leader commit by itself. See the
[replication tests](../../tests/DotnetRaft.Tests/Core/RaftCoreReplicationTests.cs)
and [self-append tests](../../tests/DotnetRaft.Tests/RawNode/RawNodeReadyTests.cs).
