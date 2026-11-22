# Inside SQLite Raft storage

Part of the [implementation map](README.md). For operating instructions, use the
[SQLite package guide](../../src/DotnetRaft.Sqlite/README.md) to open,
persist, and recover a node, and [ADR 0006](../adr/0006-sqlite-durable-raft-storage.md)
for the decision to keep SQLite out of the core package.

## Connection policy

[`SqliteStorage`](../../src/DotnetRaft.Sqlite/SqliteStorage.cs) serializes
access to one nonpooled connection. It resolves existing symlinks so an alias
cannot accidentally create a separate lock or WAL path. Exclusive ownership
is acquired before construction returns. Hard-link aliases cannot be detected
portably; the [operational constraints](../../src/DotnetRaft.Sqlite/README.md#operational-constraints)
exclude them rather than promising detection.

Initialization checks the effective exclusive, WAL, and FULL settings:
SQLite can ignore an unsupported pragma without an error. WAL with FULL
synchronization puts the durability boundary at transaction commit, not at a
later checkpoint. This package uses the same policy for every write, even
when the generic host protocol would permit a non-forced commit-only write.

## Schema

The [schema source](../../src/DotnetRaft.Sqlite/SqliteStorageSchema.cs) and
[schema approval](../../tests/DotnetRaft.Sqlite.Tests/Schema/SqliteSchema.approved.sql)
own the exact format. A singleton metadata row tracks hard state, the snapshot,
retained boundaries, and any pending application snapshot. Ordered entry rows
include a dummy entry at the retained boundary for the preceding-term check.

Fixed-width big-endian BLOB keys preserve unsigned `ulong` order under
SQLite's bytewise comparison, including above `long.MaxValue`. Open verifies
the schema, uniquely keyed range cardinality and endpoints, and boundary
payloads. That checks index continuity without deserializing the entire log.
Reads then check the key, separate term, and protobuf against each other for
the entries they decode. A size-limited page checks lengths before
materializing the next payload, so it need not decode an unused suffix.

For `GetTerm`, the backend still reads the row and compares the term and
index with the protobuf. The common wire encodings let it skip the opaque
command bytes without creating an `Entry` or copying them into a
`ByteString`; unusual tags use the generated parser. Reopen still checks
the retained endpoints, and transactions still check the resulting tail
before publishing. The term-only path is not a query of the SQL term
column in isolation.

## Atomic Ready publication

`PersistReady` and `PersistStorageAppend` share one backend transaction rule.
The transaction installs an incoming snapshot before appending its suffix and
validates the resulting bounds and hard state. Checking the final generation
matters: validating an interim commit against a log not yet extended would
reject a legal batch. The write must not publish a commit beyond the final
log, a regressed term or commit, or a changed nonzero vote in the same term.

Metadata updates omit the snapshot BLOB unless the snapshot changed. A vote,
commit, or compaction write need not serialize or bind unchanged application
bytes. Neither persistence entry point calls the node or sends a response:
the host owns those effects under the [Ready contract](../integration/ready.md).

### Application snapshot acknowledgement

The Raft transaction can atomically store an incoming snapshot and its pending
marker, but cannot also commit a separate application database. The marker
identifies that cross-store gap. Ordinary publication and `GetInitialState`
cannot bypass it. The storage checks the exact pending index and durable
commit coverage before clearing it; the host must verify application
restoration before acknowledging it.

For example, a process can stop after the application transaction commits
but before acknowledgement clears the marker. The next process still has
to reconcile it; skipping the restore based only on an index would not
establish a coherent application state. See the package's
[live ordering](../../src/DotnetRaft.Sqlite/README.md#persisting-ready-and-local-append-work)
and [restart procedure](../../src/DotnetRaft.Sqlite/README.md#open-and-restart).
Locally created snapshots describe application state that already exists;
they do not introduce that incoming-restore gap.

## Failure and recovery

An expected validation error leaves the prior transaction intact. An
unexpected SQLite, decoding, schema, or I/O failure faults the storage
instance rather than letting a node build on a suspect view. Unsupported
schemas are rejected, not repaired or silently migrated.

The [durability tests](../../tests/DotnetRaft.Sqlite.Tests/SqliteStorageDurabilityTests.cs)
exercise rejected writes, reopen, pending reconciliation, and abrupt process
termination. They test transactional publication and WAL recovery, not
power loss on a storage device. The
[node integration tests](../../tests/DotnetRaft.Sqlite.Tests/SqliteStorageRawNodeTests.cs)
also verify the Ready and local append boundaries against real SQLite files.
