# D25 Asynchronous Storage Writes Design

Historical [port milestone](README.md#completed-reference-port). Use the
[async storage contract](../public-api.md#asynchronous-storage-writes)
for worker ordering, response routing, and application reconciliation.

Local append and apply messages let Ready acceptance continue while work is
in flight. Append and apply each need FIFO ordering, but have distinct
completion streams. Dependent network replies are nested inside append work
so they cannot get ahead of the write. Local confirmations go back to the
node rather than through the network.

## Append response and ABA defense

In overlapping terms, the same unstable entry tuple can disappear and
reappear while older append responses are still queued. An old response with
the right tuple can then refer to a different durable generation. Compare
the [worked ABA race](../learning/03-ready-and-recovery.md) with
[`AsyncStorageResponseTests`](../../tests/DotnetRaft.Tests/AsyncStorage/AsyncStorageResponseTests.cs).
Append confirmation checks both the current Raft term and the entry tuple.
Snapshot completion is separate: it can cross a term change, but a stale
completion cannot replace newer membership. This shares the
[synchronous snapshot acknowledgement path](d21-rawnode-ready.md#advance).

## Interaction harness

[Async interaction tests](../../tests/DotnetRaft.Tests/Interaction/InteractionAsyncStorageTests.cs)
can process each worker independently to reproduce interleavings. The
[counter](../../examples/DotnetRaft.Counter/README.md#asynchronous-storage-writes)
uses the same public protocol to show commit advancing while apply is paused.
