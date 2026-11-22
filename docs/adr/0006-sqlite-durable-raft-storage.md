# ADR 0006: Package SQLite storage separately

- **Status:** Accepted
- **Date:** 2026-10-04

## Context

A durable backend needs to publish a snapshot, log suffix, and hard state as
one generation. Embedding a database in the core would force native assets
on users who provide their own storage. SQLite's signed integers also
cannot represent the full unsigned Raft index and term range as ordered keys.

## Decision

Offer `DotnetRaft.Sqlite` separately with atomic Ready and local append
methods. Use SQLite WAL with FULL synchronization for every mutation and
exclusive ownership of the database. Use fixed-width big-endian keys for
unsigned ordering. Preserve the pending application snapshot in the same
transaction as an incoming snapshot, because no SQLite transaction in this
package can finish an independent application's restore.

## Trade-offs

One durable policy is simpler to reason about than varying flush settings
with each Ready batch, at the cost of syncing even commit-only writes.
Separate storage calls are not a substitute for atomic batch publication.
Exclusive local ownership rules out unsupported paths and online backup
assumptions. A relaxed durability option would need an explicit recovery
model; the package does not introduce it implicitly.

Use the [package guide](../../src/DotnetRaft.Sqlite/README.md) for operations
and [storage internals](../internals/sqlite-storage.md) for schema, recovery
mechanics, and executable evidence.
