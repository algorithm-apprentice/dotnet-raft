# D16 Snapshot Design

Historical [port milestone](README.md#completed-reference-port). Use the current
[snapshot and compaction](../public-api.md#snapshots-and-compaction)
and [restart](../public-api.md#restart) rules in a host.

A sender uses a snapshot when the peer needs a prefix that cannot be sent as
entries. At the receiver, a matching `(index, term)` can advance commit
without installing the snapshot. An otherwise eligible newer snapshot with
no matching tuple installs a new log boundary and consensus configuration.
The host must still persist it and restore the application.

Do not confuse the sender's transport report, the receiver's durable append
response, and the receiver's physical restore. While a new snapshot is
unstable, the core can still need the [earlier application's configuration](d21-rawnode-ready.md#advance).
See the [transfer tests](../../tests/DotnetRaft.Tests/Core/RaftCoreSnapshotSendingTests.cs)
and [restore tests](../../tests/DotnetRaft.Tests/Core/RaftCoreSnapshotRestoreTests.cs).
