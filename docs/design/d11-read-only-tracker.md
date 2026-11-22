# D11 Read-Only Request Tracker Design

Historical [port milestone](README.md#completed-reference-port). See the
[read walkthrough](../learning/04-membership-reads-and-liveness.md#trace-a-read-through-this-port)
and [read contract](../public-api.md#read-index) for the current path.

[`ReadOnlyTracker`](../../src/DotnetRaft/Read/ReadOnlyTracker.cs) numbers
pending requests in a FIFO stream. A heartbeat response acknowledging a
position covers the earlier positions as well. Internal positions are not
log indexes or client request contexts: two requests with identical caller
bytes can still occupy distinct positions.

The tracker solves quorum confirmation, not leader admission or host read
completion. See the [tracker tests](../../tests/DotnetRaft.Tests/Read/ReadOnlyTrackerTests.cs)
for cumulative acknowledgement and joint quorums.
