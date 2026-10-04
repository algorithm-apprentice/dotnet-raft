# SQLite Durable Storage Design

- **Status:** Implemented
- **Date:** 2026-10-04
- **Scope:** durable Raft consensus storage package
- **Related decision:** ADR 0006

## Objective

Provide a production-oriented SQLite implementation of the existing
`IStorage` and host mutation contracts without changing consensus behavior or
coupling the core package to SQLite.

## Project and package

```text
src/DotnetRaft.Sqlite/
  DotnetRaft.Sqlite.csproj
  README.md
  SqliteStorage.cs
```

The project:

- targets `net10.0`;
- is packable as `DotnetRaft.Sqlite 1.0.0`;
- references `src/DotnetRaft`;
- references `Microsoft.Data.Sqlite 10.0.12`;
- packages `LICENSE`, its own README, and `THIRD-PARTY-NOTICES.md`; and
- produces a portable-PDB symbol package.

The adapter uses only the public `DotnetRaft` surface. Storage-specific
validation and size limiting are source-local and protected by cross-
implementation characterization tests. The packed dependency range is
`1.0.0` (NuGet minimum-version semantics). Public semantic-version
compatibility applies and no runtime access to core internals is required.
Consumers forcing a future incompatible core major must update the SQLite
package in lockstep.

## Public API

```csharp
namespace DotnetRaft.Storage.Sqlite;

public sealed class SqliteStorage
    : IStorage,
      IDisposable
{
    public SqliteStorage(string databasePath);

    public string DatabasePath { get; }

    public StorageState GetInitialState();
    public IReadOnlyList<Entry> GetEntries(
        ulong lowInclusive,
        ulong highExclusive,
        ulong maxSize);
    public ulong GetTerm(ulong index);
    public ulong GetLastIndex();
    public ulong GetFirstIndex();
    public Snapshot GetSnapshot();
    public HardState? GetHardState();
    public Snapshot? GetPendingApplicationSnapshot();

    public void PersistReady(Ready ready);
    public void PersistStorageAppend(Message request);
    public void AcknowledgeApplicationSnapshot(
        ulong index);

    public void SetHardState(HardState state);
    public void ApplySnapshot(Snapshot snapshot);
    public Snapshot CreateSnapshot(
        ulong index,
        ConfState? confState,
        ByteString data);
    public void Compact(ulong compactIndex);
    public void Append(IEnumerable<Entry> entries);

    public void Dispose();
}
```

The API is synchronous because `IStorage` and the Ready durability boundary
are synchronous. `Microsoft.Data.Sqlite` does not provide asynchronous SQLite
file I/O; async ADO.NET methods would not make the underlying commit
asynchronous.

## File ownership

`databasePath`:

- must be nonempty;
- is normalized with `Path.GetFullPath`;
- resolves an existing symbolic-link target before opening;
- cannot be `:memory:`, a URI filename, or a temporary empty filename;
- has its parent directory created when missing; and
- is exposed through `DatabasePath`.

Existing symbolic links are canonicalized to their final target. A database
file with a hard-link alias is unsupported because SQLite WAL/shm names are
path-derived while the database inode is shared. The package cannot portably
enumerate every hard-link alias and therefore documents this as a filesystem
precondition instead of claiming complete mechanical prevention.

## Connection policy

The storage owns one nonpooled connection:

```text
Mode=ReadWriteCreate
Cache=Private
Pooling=False
Default Timeout=30
```

Initialization executes and verifies:

```sql
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA fullfsync = ON;
PRAGMA checkpoint_fullfsync = ON;
PRAGMA busy_timeout = 30000;
PRAGMA trusted_schema = OFF;
```

Exclusive locking must return `exclusive`; WAL mode must return `wal`; and the
effective synchronous value must be `2` (`FULL`). The storage performs an
immediate write transaction during initialization so the exclusive database
lock is acquired before construction returns. A second ordinary SQLite
connection through the canonical path or a symlink alias must fail while the
storage is open.

Unknown PRAGMAs cannot be trusted because SQLite silently ignores them, so
required values are queried after setting.

The default 1000-page WAL autocheckpoint remains enabled. Checkpointing is
not part of commit durability: in WAL + FULL mode, SQLite syncs the WAL after
each committed transaction.

SQLite WAL mode is unsupported on network filesystems. The package documents
that the database, WAL, and shared-memory files must reside on one local
filesystem.

## Schema

Schema version 1:

```sql
CREATE TABLE raft_metadata (
    singleton       INTEGER PRIMARY KEY CHECK (singleton = 1),
    format          TEXT NOT NULL,
    schema_version  INTEGER NOT NULL,
    hard_state      BLOB NULL,
    snapshot        BLOB NOT NULL,
    compacted_index BLOB NOT NULL CHECK (length(compacted_index) = 8),
    last_index      BLOB NOT NULL CHECK (length(last_index) = 8),
    pending_snapshot_index
                    BLOB NULL CHECK (
                        pending_snapshot_index IS NULL
                        OR length(pending_snapshot_index) = 8)
) STRICT;

CREATE TABLE raft_entries (
    entry_index BLOB PRIMARY KEY CHECK (length(entry_index) = 8),
    entry_term  BLOB NOT NULL CHECK (length(entry_term) = 8),
    payload     BLOB NOT NULL
) WITHOUT ROWID, STRICT;

PRAGMA user_version = 1;
```

The singleton format value is:

```text
dotnet-raft-sqlite
```

An empty database contains:

- normalized empty snapshot;
- null hard state;
- compacted index `0`;
- last index `0`; and
- null pending application snapshot; and
- one dummy entry `(index=0, term=0)`.

## Unsigned encoding

Indexes and terms are encoded as eight-byte big-endian BLOBs:

```text
00 00 00 00 00 00 00 01 < ... < FF FF FF FF FF FF FF FE
```

SQLite BLOB comparison is bytewise, so ordered queries and range deletes
match unsigned `ulong` order. Entry payloads still contain the protobuf index
and term. Reads verify that key, separate term, and payload agree.

`ulong.MaxValue` remains rejected where the public storage contract requires a
representable successor.

## Open and schema validation

On a new file:

1. require no existing user tables;
2. create schema in one transaction;
3. insert metadata and dummy entry;
4. set `user_version = 1`; and
5. commit durably.

On an existing file:

1. require `user_version == 1`;
2. require exactly one metadata row with the expected format and schema
   version;
3. parse and normalize snapshot and optional hard state;
4. reject snapshot, compacted, or last indexes without a representable
   successor;
5. verify `snapshot.index <= last_index` and
   `compacted_index <= last_index`;
6. require the exact expected tables, columns, constraints, primary keys, and
   no triggers, views, or unexpected user schema objects;
7. verify entry count equals
   `last_index - compacted_index + 1`, with minimum/maximum keys exactly equal
   to the metadata bounds; this proves the unique ordered key set is
   contiguous without parsing every payload;
8. verify the compacted and last entry payload key/term values agree;
9. verify a pending snapshot index, when present, equals the retained snapshot
   index and identifies a non-bootstrap snapshot; and
10. reject unsupported or malformed state with
   `StorageException(StorageError.Unknown, ...)`.

No automatic migration or repair is attempted.

## Read operations

All operations hold one private monitor and reject use after disposal.

### Initial state

Read metadata, parse detached protobufs, and return snapshot `ConfState`.
When an application snapshot restore is pending, `GetInitialState` fails
explicitly so `RawNode`/`RaftNode` construction cannot bypass restart
reconciliation.

### First and last indexes

Decode metadata:

```text
FirstIndex = compactedIndex + 1
LastIndex  = lastIndex
```

### Term

- below compacted index: `Compacted`;
- above last index: `Unavailable`;
- otherwise query exactly one entry and return its stored term.

### Entries

Validate the same half-open range rules as `MemoryStorage`, then:

```sql
SELECT entry_index, entry_term, payload
FROM raft_entries
WHERE entry_index >= $low
  AND entry_index < $high
ORDER BY entry_index;
```

Every returned row is parsed and checked for continuity. The encoded protobuf
size budget returns at least the first entry for a nonempty range.

### Snapshot

Parse and return a detached normalized snapshot from metadata.

### Hard state

`GetHardState` returns a detached nullable hard state even while application
snapshot reconciliation is pending. Recovery uses it to preserve term and vote
while durably repairing commit before acknowledgement. A missing hard state or
term below the pending snapshot term is not repairable from snapshot metadata
and is rejected.

### Pending application snapshot

`GetPendingApplicationSnapshot` returns:

- `null` when no durable application restore is pending; or
- the detached retained snapshot when
  `pending_snapshot_index == snapshot.Metadata.Index`.

The host must call this before constructing a restarted node.

## Write operations

Every write begins a nondeferred serializable transaction. All mutable
protobuf inputs are cloned or serialized before the transaction mutates the
database.

The singleton metadata update excludes the snapshot column unless the
snapshot object actually changed. Ordinary entry, hard-state, acknowledgement,
and compaction writes therefore do not serialize or bind retained application
snapshot bytes.

SQLite failures roll back the transaction and are wrapped as
`StorageException(StorageError.Unknown, ...)`. Expected storage errors retain their existing codes. Invalid caller ranges,
gaps, or malformed batches use `InvalidOperationException`; no core internal
exception type is referenced.

### Append

The algorithm matches `MemoryStorage`:

1. validate non-null contiguous entries and final-index successor;
2. discard an entirely compacted range;
3. trim the compacted incoming prefix;
4. reject a gap;
5. delete stored entries from the incoming first index onward;
6. insert the incoming entries; and
7. update `last_index`.

### Compact

Require:

```text
compactedIndex < requestedIndex <= lastIndex
```

Read the requested term, delete earlier rows, replace the retained row with a
dummy entry, and update `compacted_index`.

### Create snapshot

Validate snapshot freshness and retained bounds, read the term at the target
index, clone the optional configuration, replace snapshot metadata/data, and
return a detached snapshot.

### Apply snapshot

Validate staleness and successor, delete all entries, insert one dummy entry
at the snapshot index/term, and atomically replace snapshot,
`compacted_index`, and `last_index`. `HardState` is retained, matching
`MemoryStorage`.

A non-bootstrap incoming snapshot also sets
`pending_snapshot_index = snapshot.index`. The marker is not set by
`CreateSnapshot`, because locally created snapshots already describe
physically applied application state.

While a pending marker exists, all mutation methods except `SetHardState` and
`AcknowledgeApplicationSnapshot` are rejected. `SetHardState` is allowed only
to durably repair commit so it covers the pending snapshot without exceeding
the retained last index. This prevents later log or snapshot state from
advancing before the host establishes the corresponding application state.
Reads needed to obtain and restore the pending snapshot remain available.

### Hard state

Serialize and replace the singleton hard-state BLOB.

## Atomic Ready publication

`PersistReady` serializes every input before starting one transaction:

```text
apply Snapshot, when present
append Entries
replace HardState, when present
commit
```

Any validation or SQL failure rolls back all three categories. The method
does not send messages, apply committed entries, or call `Advance`.

Before commit, the resulting state must satisfy:

```text
snapshot.index <= resulting last index
hardState.commit <= resulting last index, when HardState is present
snapshot.index <= hardState.commit, when both are present
max(snapshot.term, lastEntry.term) <= hardState.term
```

Term and commit cannot regress, and a nonzero vote cannot change within one
term. Direct mutation methods use the same final-state validator, so no
successful call can create a database that reopening would reject.

Because every transaction uses FULL synchronous durability, successful return
satisfies `Ready.MustSync` whether it is true or false.

## Asynchronous storage append

`PersistStorageAppend` accepts only:

```text
Type = MsgStorageAppend
To   = RaftLocalMessageTargets.AppendThread
```

Hard-state presence must be all-or-none across `Term`, `Vote`, and `Commit`.
Validation also requires:

- present, nonzero, nonlocal `From`;
- absent `LogTerm`, `Index`, `Reject`, `RejectHint`, and `Context`;
- snapshot absent or present with nonzero index and normalized metadata;
- non-null contiguous entries with nonzero indexes;
- resulting retained log boundaries that contain the snapshot; and
- persisted or incoming commit not beyond the resulting last index and not
  below a persisted snapshot index.

The method reconstructs a `HardState` only when all hard-state fields are
present, then atomically persists snapshot, entries, and hard state.

`request.Responses` are not sent or mutated. The append worker may deliver
them only after this method succeeds and, for a snapshot request, after the
host restores the application snapshot under the D25 application barrier.

### Application snapshot acknowledgement

After restoring the exact pending snapshot's application bytes,
physical-applied cursor, and `ConfState`, the host calls:

```csharp
storage.AcknowledgeApplicationSnapshot(index);
```

The method requires `index` to equal the pending index and clears the marker
durably. A missing or mismatched acknowledgement is rejected without
mutation.

For synchronous Ready processing, acknowledgement occurs after application
snapshot restore and before `Advance`. For D25 append work, acknowledgement
occurs before delivering the request's responses.

## Failure and recovery

After a failed write:

- the storage instance remains usable only when SQLite successfully rolled
  back and the failure was an expected validation/storage condition;
- unexpected SQLite, parse, schema, or I/O failure marks the instance
  faulted, closes its connection, and causes every later call to throw the
  same terminal `StorageException`;
- disposal remains idempotent.

Reopening after process termination relies on SQLite WAL recovery. Startup
validation then reconstructs the same IStorage view.

Before constructing a restarted `RawNode` or `RaftNode`, the host must:

1. call `GetPendingApplicationSnapshot`;
2. when non-null, atomically restore application state, physical-applied index,
   and `ConfState` from that snapshot;
3. require trusted durable `HardState.Term` to cover both the snapshot term and
   retained last-entry term;
4. repair `HardState.Commit` when required by the D21 physical-applied
   recovery rule;
5. call `AcknowledgeApplicationSnapshot`; and
6. only then construct the node.

This closes the crash window after SQLite snapshot commit but before
application restore.

## Testing

### Contract

Run the complete `MemoryStorage` behavior matrix against `SqliteStorage`:

- initial state;
- term and entry range errors;
- size limits;
- append overlap/replacement/gap cases;
- compaction;
- snapshot creation/application;
- ownership and full-`ulong` keys; and
- concurrent readers with atomic writer batches.

### Durability and recovery

- close/reopen after every direct mutation;
- close/reopen after combined Ready and storage-append publication;
- crash/reopen after durable snapshot publication but before application
  acknowledgement, then verify mandatory reconciliation;
- verify hard state, snapshot, entries, compacted index, and configuration;
- force a statement failure inside a multi-category transaction and verify
  complete rollback after reopen;
- child process commits through `SqliteStorage` then terminates abruptly;
  parent reopens and observes it;
- a child using a raw connection leaves a schema-valid uncommitted
  transaction then terminates; parent reopens and observes none of it;
- reject truncated/corrupt protobuf metadata and entry payloads;
- reject unknown schema versions, altered schema objects, triggers, unrelated
  databases, impossible successor indexes, boundary/count discontinuity, and
  corrupt payloads;
- reject a second storage instance, raw SQLite connection, and symlink alias
  while exclusive ownership is held;
- document and fixture-test that hard-linked paths are unsupported rather than
  falsely claiming reliable detection.

Wall-clock timeouts are deadlock guards only. No sleep-based race test belongs
in the normal suite. Process termination demonstrates transaction atomicity
and WAL recovery, not forced-media durability. Power-loss durability is based
on verified `synchronous=FULL`/`fullfsync` settings and SQLite's documented
contract; an instrumented VFS is outside this milestone.

### Package

- separate API approval;
- exact package metadata/dependencies/files;
- isolated consumer creates, persists, disposes, reopens, and reads a store;
- symbols and Source Link;
- two-worktree deterministic package comparison; and
- native SQLite smoke on the CI Linux RID and local macOS arm64 host.

## Non-goals

- application MVCC or client APIs;
- cross-database atomic application/Raft writes;
- encrypted SQLite;
- network filesystems;
- multiple writers/processes for one database;
- hard-linked database paths;
- online schema migrations;
- relaxed durability;
- incremental/streaming snapshots beyond the current `IStorage` contract;
- automatic WAL checkpoint scheduling or database vacuum policy; and
- replacing the educational sample's volatile application state with a
  partially durable configuration.

## Validation sequence

1. focused SQLite storage tests;
2. subprocess recovery tests;
3. API/schema approvals;
4. full Debug and Release tests;
5. formatting;
6. benchmark smoke;
7. core and SQLite package verification/consumers;
8. both-package reproducibility;
9. independent GPT-5.6 Sol code review;
10. commit, push, and green CI.

## Validation outcome

| Gate | Result |
|---|---:|
| focused SQLite tests | 33 passed |
| `SqliteStorage` line coverage | 95.24% |
| `SqliteStorage` branch coverage | 84.68% |
| `SqliteStorageCodec` line coverage | 86.66% |
| focused mutation | 691 killed / 696 tested |
| reviewed liveness timeout mutants | 5 |
| surviving or no-coverage mutants | 0 |
| generated compile-error mutants | 28 |

Additional completed gates:

- no Stryker Safe Mode;
- synchronous and asynchronous RawNode persistence/restart integration;
- committed and uncommitted abrupt-process recovery;
- exact schema and managed API approval;
- rollback after injected multi-category transaction failure;
- 8 MiB snapshot regression proving small metadata updates do not reserialize
  unchanged snapshot bytes;
- non-regressing term, vote, and commit validation across every direct and
  batched mutation;
- core and SQLite package verification;
- isolated `DotnetRaft.Sqlite` consumer running the macOS arm64 native SQLite
  asset; and
- deterministic Source Link and symbol verification.
