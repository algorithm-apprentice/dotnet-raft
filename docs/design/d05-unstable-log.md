# D05 Unstable Log Design

Historical [port milestone](README.md#completed-reference-port). See the
[log walkthrough](../learning/02-log-and-replication.md#stable-storage-and-the-unstable-overlay)
and [async ABA example](../learning/03-ready-and-recovery.md) for current code.

[`UnstableLog`](../../src/DotnetRaft/Core/UnstableLog.cs) overlays entries and
snapshots not yet known to be stable. Accepting a Ready marks a particular
prefix of work in flight; it does not make the work durable. Entries added
after that acceptance still need their own persistence request.

An entry acknowledgement must still match the unstable `(index, term)` it
names. That match alone is not enough with overlapping writes: a different
suffix could have been written between two appearances of the same tuple.
D25 adds an ordered, term-checked acknowledgement to protect that case.
See the [unstable log tests](../../tests/DotnetRaft.Tests/Core/UnstableLogTests.cs)
and [async response tests](../../tests/DotnetRaft.Tests/AsyncStorage/AsyncStorageResponseTests.cs).
