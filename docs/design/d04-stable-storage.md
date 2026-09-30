# D04 Stable Storage Design

- **Status:** Accepted
- **Date:** 2026-09-30
- **DAG node:** D04
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D04 defines the boundary through which the Raft core reads durable consensus
state. It also provides an in-memory implementation for tests, examples, and
applications that restore their durable state into memory before starting.

The storage API is synchronous because the deterministic Raft state machine
must not await I/O. Applications with asynchronous persistence consume work
through `Ready` or local storage messages at later DAG nodes; their storage
worker is responsible for completing the I/O before acknowledging it to Raft.

## Goals

- Preserve the stable-storage semantics of the pinned `etcd/raft`
  implementation.
- Make compacted, unavailable, stale-snapshot, and temporarily unavailable
  states distinguishable.
- Keep the interface implementable by file, database, or remote-backed storage
  adapters without coupling the Raft core to any one technology.
- Provide a thread-safe `MemoryStorage`.
- Prevent mutable protobuf messages from corrupting stored state through
  accidental aliasing.

## Non-goals

- Implementing a WAL, database, filesystem layout, or asynchronous I/O.
- Defining the `Ready` persistence protocol; that belongs to D21 and D25.
- Implementing the combined stable/unstable Raft log; that belongs to D06.
- Enforcing every Raft log invariant at the storage boundary. D04 validates
  entry continuity, while term and committed-entry safety remain owned by the
  Raft log and state machine.

## Public API

The public interface uses C# naming while preserving the reference semantics:

```csharp
namespace DotnetRaft.Storage;

public interface IStorage
{
    StorageState GetInitialState();

    IReadOnlyList<Entry> GetEntries(
        ulong lowInclusive,
        ulong highExclusive,
        ulong maxSize);

    ulong GetTerm(ulong index);

    ulong GetLastIndex();

    ulong GetFirstIndex();

    Snapshot GetSnapshot();
}

public sealed record StorageState(
    HardState? HardState,
    ConfState ConfState);
```

`MemoryStorage` additionally exposes the mutation operations required by a
host:

```csharp
public sealed class MemoryStorage : IStorage
{
    public void SetHardState(HardState state);

    public void ApplySnapshot(Snapshot snapshot);

    public Snapshot CreateSnapshot(
        ulong index,
        ConfState? confState,
        ByteString data);

    public void Compact(ulong compactIndex);

    public void Append(IEnumerable<Entry> entries);
}
```

## Error model

Expected Raft storage conditions use one typed exception carrying a stable
error code:

```csharp
public enum StorageError
{
    Unknown = 0,
    Compacted,
    Unavailable,
    SnapshotOutOfDate,
    SnapshotTemporarilyUnavailable,
}

public sealed class StorageException : Exception
{
    public StorageException(
        StorageError error,
        string message,
        Exception? innerException = null)
        : base(message, innerException)
    {
        Error = error;
    }

    public StorageError Error { get; }
}
```

The mapping is:

| Error | Meaning |
|---|---|
| `Compacted` | `GetEntries` or `Compact` was requested at or before the retained dummy entry, or `GetTerm` was requested before it. |
| `Unavailable` | The requested entry is newer than the available log or the storage contains only its dummy entry. |
| `SnapshotOutOfDate` | `CreateSnapshot` was requested at or before the current snapshot index, or `ApplySnapshot` would replace an equal or newer non-bootstrap snapshot. |
| `SnapshotTemporarilyUnavailable` | A custom storage implementation cannot provide a snapshot yet. |
| `Unknown` | A storage-specific failure has no Raft sentinel equivalent. |

The core may branch on `StorageError`. Unexpected implementation failures, such
as database or filesystem exceptions, may propagate directly and make the Raft
instance inoperable, matching the reference contract.

Exceptions are chosen instead of a custom generic result type to keep the
public storage interface small and idiomatic. Sentinel failures are uncommon
relative to successful term and entry reads. If profiling later demonstrates
that exception frequency is material, changing this boundary requires a new
ADR because it affects every log call site.

## Internal representation

`MemoryStorage` contains:

```text
gate       object used by lock
hardState HardState? persisted term, vote, and commit
snapshot  Snapshot, always contains Metadata and ConfState; IStorage reads
          normalize scalar proto2 default presence
entries   List<Entry>, always containing at least one dummy entry
```

The dummy entry retains the `(index, term)` immediately preceding the first
entry that can be returned by `GetEntries`.

```text
entries[0].Index = offset
entries[n].Index = offset + n
FirstIndex       = offset + 1
LastIndex        = offset + entries.Count - 1
```

An empty storage starts with a dummy entry at `(0, 0)`.

The half-open entry API and `FirstIndex = offset + 1` require every stored
index to have a representable successor. `ApplySnapshot` and `Append` reject
`ulong.MaxValue` before mutating storage.

## Ownership and mutability

Generated protobuf messages are mutable, unlike the logical immutable values
assumed by the Go implementation. `MemoryStorage` therefore:

- clones every mutable protobuf input before normalizing or retaining it,
  including `HardState`, `Snapshot`, a non-null `ConfState` supplied to
  `CreateSnapshot`, and appended `Entry` values;
- returns clones from `GetInitialState`, `GetEntries`, `GetSnapshot`, and
  `CreateSnapshot`;
- never exposes its internal `List<Entry>`.

This is intentionally safer than relying on callers to obey an immutability
comment. Normalization helpers operate only on storage-owned clones so they
never add nested messages to a caller-owned object. `ByteString` itself is
immutable.

Custom `IStorage` implementations must return values that can be treated as
immutable by the Raft core and application.

## Operation semantics

### `GetInitialState`

- Returns a cloned nullable `HardState`.
- Returns a non-null cloned `ConfState` from the current snapshot metadata.
- An uninitialized storage returns `HardState = null` and an empty
  `ConfState`.

### `GetEntries`

- Interprets the range as `[lowInclusive, highExclusive)`.
- Throws `Compacted` when `lowInclusive <= offset`.
- Throws `RaftInvariantException` when the range is reversed or extends past
  `LastIndex + 1`.
- Throws `Unavailable` when the storage contains only its dummy entry.
- For an in-bounds empty range, returns an empty list when real entries are
  retained; dummy-only storage still returns `Unavailable`, matching the
  reference `MemoryStorage`.
- Applies the encoded protobuf `maxSize` limit through `EntrySizing.LimitSize`.
- Returns at least the first entry when the range is non-empty, even when
  `maxSize` is zero.

### `GetTerm`

- Returns the term of an index in `[FirstIndex - 1, LastIndex]`.
- Throws `Compacted` below the dummy entry.
- Throws `Unavailable` above the last entry.

### `Append`

- Treats an empty input as a no-op.
- Requires non-null entries with contiguous indexes.
- Throws `RaftInvariantException` when the final incoming index is
  `ulong.MaxValue`.
- Discards an incoming prefix that has already been compacted.
- Treats an incoming range entirely older than `FirstIndex` as a no-op.
- Truncates an overlapping stored suffix before appending replacements.
- Appends directly when the incoming range begins at `LastIndex + 1`.
- Throws `RaftInvariantException` when the incoming range leaves a gap.

### `Compact`

- The caller must not compact beyond the highest index that the Raft core has
  acknowledged as applied, equivalent to `raftLog.applied` in the reference.
  `MemoryStorage` cannot enforce this cross-component precondition.
- Throws `Compacted` when `compactIndex <= offset`.
- Throws `RaftInvariantException` when `compactIndex > LastIndex`.
- Replaces the current dummy entry with a new dummy retaining the compacted
  entry's index and term.
- Retains entries after `compactIndex`.

### `CreateSnapshot`

- Throws `SnapshotOutOfDate` when `index` is not newer than the current
  snapshot.
- Throws `RaftInvariantException` when a non-stale `index` is below
  `FirstIndex - 1`, whose dummy entry is the oldest retained term.
- Throws `RaftInvariantException` when `index > LastIndex`.
- Records the term from the entry at `index`.
- Clones and replaces the snapshot configuration when a non-null `ConfState`
  is supplied.
- A null `ConfState` is valid only when the retained snapshot configuration is
  already the latest configuration applied at `index`. If configuration
  changes have been applied since the retained snapshot, the caller must pass
  the latest `ApplyConfChange` result.
- Returns a clone of the resulting snapshot.

### `ApplySnapshot`

- Throws `SnapshotOutOfDate` exactly when the current stored snapshot index is
  non-zero and greater than or equal to the incoming snapshot index. This also
  rejects an incoming zero-index snapshot after real snapshot state exists.
- Throws `RaftInvariantException` when the incoming snapshot index is
  `ulong.MaxValue`.
- When the current stored snapshot index is zero, allows the bootstrap form
  whose index and term are both zero but whose `ConfState` is populated.
- Replaces the stored snapshot with a clone.
- Replaces the log with one dummy entry at the snapshot `(index, term)`.

## Concurrency

Every `MemoryStorage` read and mutation acquires the same private `lock`.
`ReaderWriterLockSlim` is intentionally not used: the storage is small, lock
hold times are bounded, and one monitor keeps the ownership model easy to
audit. Returned values are cloned before releasing the lock.

The Raft core itself remains single-threaded. The lock exists because
applications may persist a `Ready` batch on a worker while the Raft event loop
performs reads.

## Test plan

Tests translate the complete behavior matrix from `storage_test.go`:

1. term lookup before, within, and after the retained range;
2. entry slicing, compaction errors, upper bounds, and encoded-size limits;
3. first and last index changes after append and compaction;
4. compaction at every retained index;
5. snapshot creation at retained indexes, including rejection below
   `FirstIndex - 1` after compaction without a newer snapshot;
6. append cases covering obsolete, identical, overlapping, replacing, and
   direct-append ranges;
7. empty entry ranges for dummy-only and non-empty storage;
8. normal, stale, and bootstrap snapshot application, including rejection of
   a zero-index bootstrap after a non-zero snapshot;
9. rejection of `ulong.MaxValue` snapshot and entry indexes without mutation.

Additional C# ownership tests verify that:

- mutating an appended `Entry` after `Append` does not change storage;
- mutating a `ConfState` after `CreateSnapshot` does not change storage;
- mutating an entry returned by `GetEntries` does not change storage;
- mutating returned state or snapshots does not change storage;
- concurrent readers and a writer do not observe invalid list ranges.

No test uses wall-clock waits. The concurrency case uses bounded tasks and a
barrier to create overlap deterministically.

## Completion criteria

D04 is complete when:

- the public contract and error mapping above are implemented;
- all translated and ownership tests pass;
- formatting and build checks pass;
- design and code reviews contain no unresolved substantiated findings;
- the reviewed changes are committed before D05 starts.
