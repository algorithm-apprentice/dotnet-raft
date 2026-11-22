# D07 Inflight Append Window Design

Historical [port milestone](README.md#completed-reference-port). See
[replication progress](../learning/02-log-and-replication.md#replication-has-two-distinct-types-of-knowledge)
for how the window is used today.

[`InflightWindow`](../../src/DotnetRaft/Tracker/InflightWindow.cs) counts and
tracks bytes in one follower's outstanding append batches. Record each
batch's final index: success at that index covers the earlier batches too.
The byte threshold is a soft limit on starting another batch, not a reason
to reject an oversized batch when the window still has room to send.

The [window tests](../../tests/DotnetRaft.Tests/Tracker/InflightWindowTests.cs)
exercise cumulative freeing, wraparound, and the count and byte limits.
