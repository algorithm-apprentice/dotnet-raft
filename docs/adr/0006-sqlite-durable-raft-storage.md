# ADR 0006: SQLite Durable Raft Storage

- **Status:** Accepted
- **Date:** 2026-10-04
- **Decision owners:** dotnet-raft maintainers

## Context

`DotnetRaft` defines the complete stable-storage and Ready durability
contract, but ships only `MemoryStorage`. A production host therefore has to
implement:

- durable log entries;
- `HardState`;
- retained snapshots and configuration;
- overlapping suffix replacement;
- prefix compaction;
- restart reads; and
- atomic publication of each synchronous Ready or asynchronous
  `MsgStorageAppend`.

The persistence implementation must not make every consensus-only package
consumer carry a native database dependency. It must also preserve the full
`ulong` Raft index/term domain even though SQLite `INTEGER` is signed.

## Decision

Add a separately packaged `DotnetRaft.Sqlite` assembly with
`DotnetRaft.Storage.Sqlite.SqliteStorage`.

1. **Package boundary**
   - `DotnetRaft` remains unchanged as the consensus package and has no SQLite
     runtime dependency.
   - `DotnetRaft.Sqlite` targets `net10.0`, depends on `DotnetRaft 1.0.0` and
     `Microsoft.Data.Sqlite 10.0.12`, and starts at package version `1.0.0`.
   - The SQLite package receives its own public API approval, package
     verification, consumer build, symbols, and reproducibility checks.
2. **Durability**
   - use SQLite WAL mode;
   - use `synchronous=FULL`, so every committed storage transaction syncs the
     SQLite WAL before returning;
   - enable `fullfsync` and `checkpoint_fullfsync`; they are effective on
     macOS and harmless elsewhere;
   - always provide stronger durability than a commit-only
     `Ready.MustSync == false` batch rather than dynamically weakening
     `synchronous`;
   - reject opening the database if WAL mode cannot be established.
3. **Atomic publication**
   - `PersistReady(Ready)` atomically applies `Snapshot`, `Entries`, and
     `HardState` in one SQLite transaction;
   - `PersistStorageAppend(Message)` validates and atomically applies one
     D25 `MsgStorageAppend`;
   - direct `SetHardState`, `ApplySnapshot`, `Append`, `Compact`, and
     `CreateSnapshot` operations are individually durable transactions;
   - responses and network messages remain host-owned and can be delivered
     only after the persistence method succeeds.
   - persisting an incoming non-bootstrap snapshot records an application
     restore-pending marker; the host must restore application state and
     acknowledge that exact snapshot before normal restart or D25 responses.
4. **Schema**
   - one singleton metadata row stores format identity, schema version,
     `HardState`, retained `Snapshot`, compacted dummy index, and last index;
   - one `WITHOUT ROWID` entry table stores key, term, and serialized entry;
   - indexes and terms use fixed-width eight-byte big-endian BLOBs so SQLite
     binary ordering matches unsigned `ulong` ordering;
   - protobuf values are stored as canonical serialized bytes and parsed into
     detached objects on read.
5. **Ownership**
   - one `SqliteStorage` instance exclusively owns one database file;
   - SQLite `locking_mode=EXCLUSIVE` is established and verified before WAL
     initialization, and the connection acquires its exclusive file lock
     before construction succeeds;
   - one private monitor serializes all operations because
     `SqliteConnection` is not a concurrent API;
   - canonical symlink targets are used when possible; hard-linked database
     files and network filesystems are explicitly unsupported because they can
     associate one database inode with different WAL paths;
   - returned protobufs are detached, matching `MemoryStorage`.
6. **Compatibility**
   - schema version 1 is created only for an empty/new database;
   - unknown, older, or newer schema versions are rejected explicitly;
   - no migration framework is added before a deployed schema exists;
   - corruption, parse failures, and SQLite failures surface as
     `StorageException(StorageError.Unknown, ...)` with the original cause.
7. **Scope**
   - this package persists Raft consensus state only;
   - it does not implement MVCC, application commands, lease/watch/auth,
     application snapshot creation, or cross-database application/Raft
     transactions;
   - application snapshot restore and physical-applied metadata remain a host
     responsibility and must be coordinated before D25 responses are sent;
   - restart must reconcile any pending application snapshot before
     constructing `RawNode` or `RaftNode`.

## Rationale

SQLite provides mature crash recovery, serializable transactions, broad
native RID coverage, and an actively maintained Microsoft .NET provider.
The transaction boundary maps directly to the Ready publication contract.

A separate package keeps the deterministic core lightweight and avoids
forcing native SQLite assets into applications that provide another storage
engine.

Fixed-width big-endian keys avoid narrowing indexes to `long` and make range,
suffix-delete, and ordered-entry queries preserve Raft's unsigned index order.

Always using FULL synchronous commits favors correctness and a simple durable
contract. A future opt-in relaxed durability mode would require a separate ADR
and failure model.

## Consequences

### Positive

- Hosts receive a supported durable `IStorage` implementation.
- Ready publication is atomic instead of relying on three independent calls.
- Restart, snapshot, compaction, and log replacement become executable
  package behavior.
- Core consumers retain the existing dependency and package footprint.

### Tradeoffs

- Every mutation performs a durable SQLite transaction and may be slower than
  a specialized segmented WAL.
- SQLite WAL mode requires a local filesystem and creates `-wal` and `-shm`
  sibling files.
- One writer and one storage monitor serialize operations.
- Snapshot payloads are materialized as protobuf BLOBs by the existing public
  storage contract.
- The SQLite package adds native runtime assets transitively.

## Rejected alternatives

- **Add SQLite directly to `DotnetRaft`:** forces all consensus users to carry
  a native database dependency.
- **Persist with separate `ApplySnapshot`, `Append`, and `SetHardState`
  transactions only:** violates the atomic Ready publication requirement.
- **Store indexes as SQLite INTEGER:** loses indexes above `long.MaxValue`.
- **Use `synchronous=NORMAL` for commit-only batches:** complicates recovery
  and permits power-loss rollback of a completed persistence call.
- **Use one SQLite database for both Raft and application state now:** invents
  an application schema and transaction model outside the current scope.
- **Build migration machinery:** no deployed SQLite schema exists yet.
- **Use RocksDB first:** the selected milestone prioritizes the lowest-risk
  correctness baseline; another backend can implement the same contract
  later.

## Acceptance criteria

This decision is implemented when:

1. every `MemoryStorage` read/mutation behavior has a SQLite-backed equivalent;
2. Ready and D25 append publication are atomic and durable on return;
3. reopen, transaction rollback, stale snapshot, compaction, suffix
   replacement, full-`ulong`, lock ownership, corruption, and concurrent
   reader/writer tests pass;
4. deterministic subprocess recovery tests cover process termination around
   committed/uncommitted SQLite transactions without claiming to emulate
   power loss;
5. the SQLite public API and on-disk schema version are approved;
6. Debug/Release tests, formatting, benchmark smoke, both package consumers,
   symbols, and two-worktree reproducibility pass;
7. an independent GPT-5.6 Sol design and code review has no unresolved
   substantiated finding; and
8. the reviewed change is committed, pushed, and green in CI.

## Outcome

Implemented as the separate `DotnetRaft.Sqlite 1.0.0` package.

- SQLite WAL, exclusive locking, `synchronous=FULL`, macOS full-fsync flags,
  exact schema validation, and unsigned big-endian indexes are enforced.
- `PersistReady` and `PersistStorageAppend` publish durable categories in one
  transaction.
- every committed state validates non-regressing HardState and requires its
  term/commit to cover retained snapshot and log terms/indexes;
- unchanged snapshot BLOBs are not rebound for ordinary hard-state, append,
  acknowledgement, or compaction transactions;
- incoming snapshots create a durable application-reconciliation marker;
  startup is blocked until application restore, commit repair, and exact
  acknowledgement complete.
- synchronous and D25 RawNode restart integration, subprocess termination,
  transaction rollback, schema corruption, ownership, snapshot, compaction,
  and full-`ulong` behavior are executable tests.
- the core package retains no SQLite dependency.
- API and schema approvals, symbols, package metadata, native macOS arm64
  consumer execution, and package provenance are verified.
