# D06 Unified Raft Log Design

- **Status:** Accepted
- **Date:** 2026-09-30
- **DAG node:** D06
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D06 combines durable `IStorage` state with the D05 `UnstableLog` into one
logical Raft log. It is the source of truth for log matching, conflict
replacement, commit progress, application progress, snapshot restore, and
bounded entry retrieval.

The unified view must remain correct while:

- stable storage is compacted;
- unstable entries overlap or replace stable entries;
- persistence is pending or in progress;
- committed entries are being applied asynchronously;
- a snapshot temporarily supersedes both log layers.

## Goals

- Preserve the pinned `etcd/raft` log behavior and cursor invariants.
- Present one continuous logical log across stable and unstable storage.
- Reject attempts to overwrite committed entries.
- Distinguish committed, applying, and durably applied progress.
- Bound entry retrieval and outstanding application work by encoded size.
- Treat storage compaction as an expected sentinel where the reference does.
- Preserve deterministic synchronous execution and defensive ownership.

## Non-goals

- Election or replication message handling; those begin in D12-D15.
- Applying entries to an application state machine; the host does that through
  D21/D24.
- Persisting entries or snapshots; D21 and D25 coordinate persistence.
- Membership, read-index, or progress tracking.
- Concurrent method calls. One serialized Raft event loop owns `RaftLog`.

## Type and construction

```csharp
internal sealed class RaftLog
{
    internal RaftLog(
        IStorage storage,
        IRaftLogger logger,
        ulong maxApplyingEntriesSize = ulong.MaxValue);
}
```

Construction:

1. reads `FirstIndex` and `LastIndex` from storage;
2. creates an empty `UnstableLog` at `LastIndex + 1`;
3. initializes `Committed`, `Applying`, and `Applied` to
   `FirstIndex - 1`;
4. stores the application byte limit.

D04 guarantees that `LastIndex + 1` is representable. Storage failures other
than expected Raft sentinels propagate and make construction fail.

## State and invariants

```text
storage                    stable log and snapshot
unstable                   pending/in-progress entries and snapshot
committed                  highest quorum-committed index
applying                   highest index handed to the application
applied                    highest index logically acknowledged by the host
maxApplyingEntriesSize     outstanding application byte limit
applyingEntriesSize        outstanding encoded bytes
applyingEntriesPaused      application delivery backpressure
logger                     diagnostic sink
```

The cursor invariants are:

```text
Applied <= Applying <= Committed <= LastIndex
```

During snapshot restore, `Committed` moves to the snapshot index immediately.
`Applying` and `Applied` advance only when D21 accepts and acknowledges the
snapshot; while any unstable snapshot exists, committed entries are not
returned for application.

`Applied` is a logical host acknowledgement cursor. The synchronous `Advance`
optimization may move it before slow physical entry or snapshot application
finishes. D21 and D24 must enforce the host ordering rule that no committed
entries from a later `Ready` are physically applied until all entries and the
snapshot from the preceding `Ready` finish.

When no unstable snapshot exists, the additional recovery bound holds:

```text
FirstIndex - 1 <= Applied
```

`applyingEntriesSize` counts encoded bytes handed to the application but not
yet acknowledged. Accounting is defensive against subtraction underflow.

## Internal API

### Read-only state

```csharp
internal ulong Committed { get; }
internal ulong Applying { get; }
internal ulong Applied { get; }
internal ulong ApplyingEntriesSize { get; }
internal bool ApplyingEntriesPaused { get; }
internal UnstableLog Unstable { get; }

internal ulong FirstIndex { get; }
internal ulong LastIndex { get; }
internal EntryId LastEntryId { get; }
```

### Matching and mutation

```csharp
internal bool MaybeAppend(
    LogSlice slice,
    ulong committed,
    out ulong lastNewIndex);

internal ulong Append(IEnumerable<Entry> entries);

internal ulong FindConflict(IReadOnlyList<Entry> entries);

internal EntryId FindConflictByTerm(ulong index, ulong term);

internal bool IsUpToDate(EntryId candidate);

internal bool MatchTerm(EntryId entryId);

internal bool MaybeCommit(EntryId entryId);

internal void CommitTo(ulong index);

internal void Restore(Snapshot snapshot);
```

### Persistence integration

```csharp
internal IReadOnlyList<Entry> GetNextUnstableEntries();
internal bool HasNextUnstableEntries { get; }
internal bool HasUnstableEntries { get; }
internal Snapshot? GetNextUnstableSnapshot();
internal bool HasNextUnstableSnapshot { get; }
internal bool HasUnstableSnapshot { get; }
internal Snapshot GetSnapshot();
internal void AcceptUnstable();
internal void StableTo(EntryId entryId);
internal void AcknowledgeSnapshot(ulong index);
```

### Application integration

```csharp
internal IReadOnlyList<Entry> GetNextCommittedEntries(
    bool allowUnstable);

internal bool HasNextCommittedEntries(bool allowUnstable);

internal void AcceptApplying(
    ulong index,
    ulong encodedSize,
    bool allowUnstable);

internal void AppliedTo(
    ulong index,
    ulong encodedSize);
```

### Entry access

```csharp
internal ulong GetTerm(ulong index);

internal IReadOnlyList<Entry> GetEntries(
    ulong startIndex,
    ulong maxSize = ulong.MaxValue);

internal IReadOnlyList<Entry> GetAllEntries();

internal IReadOnlyList<Entry> Slice(
    ulong lowInclusive,
    ulong highExclusive,
    ulong maxSize);

internal void Scan(
    ulong lowInclusive,
    ulong highExclusive,
    ulong pageSize,
    Action<IReadOnlyList<Entry>> visitor);
```

All returned protobuf messages are caller-owned clones inherited from D04/D05
or newly cloned during concatenation.

## Error model

`StorageException` with `StorageError.Compacted` and
`StorageError.Unavailable` remains the sentinel mechanism.

- `GetTerm` propagates `Compacted` or `Unavailable` for indexes outside the
  retained logical range.
- `Slice` propagates `Compacted` when its lower bound races with storage
  compaction.
- `GetAllEntries` retries from the new `FirstIndex` after `Compacted`.
- `MatchTerm` treats either sentinel as no match.
- `FindConflictByTerm` treats either sentinel as an unknown term and returns
  the current guess.
- `Unavailable` from storage for an in-bounds stable slice is an invariant
  failure because the caller already checked the logical range.
- `SnapshotTemporarilyUnavailable` from `GetSnapshot` is propagated as an
  expected retryable sentinel so D16 can defer snapshot transmission.
- Other storage exceptions propagate as fatal implementation failures.

Programming and safety violations throw `RaftInvariantException`.

## Logical indexes and terms

### `FirstIndex`

Returns the unstable snapshot index plus one when a snapshot exists;
otherwise delegates to storage.

### `LastIndex`

Returns the last unstable entry, otherwise the unstable snapshot index,
otherwise the storage last index.

### `GetTerm`

1. checks unstable entries and snapshot first;
2. validates the logical range `[FirstIndex - 1, LastIndex]`;
3. delegates to stable storage.

Checking unstable first is required because determining `FirstIndex` or
`LastIndex` may itself read storage that is being compacted.

## Append and conflict semantics

### `Append`

- Empty input returns the current `LastIndex`.
- Entries must be non-null and contiguous.
- The first incoming index minus one must not be below `Committed`.
- The entries are passed to `UnstableLog.TruncateAndAppend`.
- Returns the resulting `LastIndex`.

### `FindConflict`

Returns the first incoming index whose term does not match the logical log.

- Returns zero when every incoming entry already exists with the same term.
- Returns the first new index when the matching prefix extends beyond the
  current log.

### `MaybeAppend`

The supplied `LogSlice` is validated before use.

1. If its previous `(index, term)` does not match, returns `false`.
2. Finds the first conflict.
3. Throws if the conflict is at or before `Committed`.
4. Appends only the conflicting/new suffix.
5. Advances `Committed` to
   `min(leaderCommitted, lastNewIndex)`.

An empty entry list may still advance commit through the matching previous
entry. Commit never decreases.

### `FindConflictByTerm`

Walks backward from the supplied index until:

- the local term is less than or equal to the supplied term; or
- the local term is unknown due to compaction or unavailability.

Returns `EntryId(Term: term, Index: index)` with term zero when unknown. Named
arguments are required at the construction site to avoid reversing the
existing `EntryId(ulong Term, ulong Index)` field order. This is a best guess
used by rejection hints, not a proof of a match.

## Commit and freshness

### `CommitTo`

- Never decreases `Committed`.
- Throws when advancing beyond `LastIndex`.

### `MaybeCommit`

Commits only when:

- the supplied term is non-zero;
- the supplied index is greater than `Committed`; and
- the logical log contains the same term at that index.

This is the current-term commit check used by leaders.

### `IsUpToDate`

Compares last-entry terms first, then indexes. A candidate with the same last
term and index is considered up to date.

## Application lifecycle

### Available committed entries

No committed entries are returned while:

- application delivery is paused; or
- an unstable snapshot is pending or in progress.

The lower bound is `Applying + 1`.

The upper bound is:

- `Committed` when unstable application is allowed; or
- `min(Committed, Unstable.Offset - 1)` when only durable entries may be
  applied.

`GetNextCommittedEntries` applies the remaining byte allowance
`maxApplyingEntriesSize - applyingEntriesSize`. It returns at least one entry
when work exists, matching entry-size limiting elsewhere.

### `AcceptApplying`

- Requires `Applying <= index <= Committed`.
- Must acknowledge the exact batch returned by the immediately preceding
  `GetNextCommittedEntries` call in the same serialized Ready acceptance, with
  no intervening Raft mutation.
- Sets `Applying = index`.
- Adds the accepted encoded byte size using checked arithmetic.
- Pauses delivery when the outstanding size reaches the configured limit.
- Also pauses when the returned batch ended before the maximum currently
  applicable index, proving the byte limit truncated the batch.

### `AppliedTo`

- Requires `Applied <= index <= Committed`.
- Sets `Applied = index`.
- Raises `Applying` to at least `index`.
- Subtracts acknowledged bytes without underflow.
- Recomputes pause state from the remaining outstanding byte size.

## Snapshot lifecycle

`GetSnapshot` prefers the unstable snapshot and otherwise reads storage.
Storage-provided snapshots are cloned before they are returned.

`Restore`:

- requires `snapshot.Metadata.Index > Committed`;
- clones and validates the incoming snapshot through `UnstableLog`;
- sets `Committed` to the snapshot index;
- clears/replaces unstable entries through `UnstableLog.Restore`;
- leaves `Applying`, `Applied`, and byte accounting unchanged until the
  snapshot application lifecycle acknowledges progress.

Pending and in-progress snapshots block committed-entry delivery to avoid
ambiguous apply ordering.

`AcknowledgeSnapshot` is the single serialized logical-progress transition:

1. advances `Applied` and `Applying` to the supplied snapshot index with zero
   entry bytes; and
2. clears the matching unstable snapshot.

D21 and D25 call this after stable-storage persistence once the host accepts
responsibility for the corresponding work. Physical application state-machine
restoration may still be running. D21 and D24 must prevent entries from later
`Ready` batches from being physically applied until that restoration finishes.
The snapshot must not be cleared independently, because doing so could expose
committed ranges below the restored `FirstIndex`.

D16 performs the higher-level reference checks before `Restore`: obsolete
snapshots at or below `Committed` are ignored, and a snapshot whose
`(index, term)` already matches the log fast-forwards commit instead of
replacing the log.

## Slicing

`Slice` uses `[lowInclusive, highExclusive)` and first validates:

- the range is not reversed;
- `lowInclusive >= FirstIndex`, otherwise `Compacted`;
- `highExclusive <= LastIndex + 1`.

Behavior:

1. If the range is entirely unstable, slice unstable and apply `maxSize`.
2. Otherwise read the stable prefix through `IStorage.GetEntries`.
3. If the range ends in stable storage, return that prefix.
4. If storage already truncated the prefix due to `maxSize`, return it without
   adding unstable entries.
5. If the stable prefix consumes the limit, return it.
6. Limit the unstable suffix by the remaining bytes.
7. If one unstable entry alone exceeds the remaining allowance, omit it
   because at least one stable entry is already present.
8. Concatenate cloned stable and unstable results.

This preserves the global rule that a non-empty request returns at least one
entry while never returning a second layer's entry over the limit.

## Scanning

`Scan` repeatedly calls `Slice` with `pageSize` until the requested range is
consumed.

- Each callback receives one or more consecutive entries.
- A single oversized entry may exceed `pageSize`.
- An unexpected empty page throws `RaftInvariantException`.
- Exceptions from the callback propagate immediately and stop scanning.

## Ownership

`RaftLog` never exposes mutable internal collections:

- unstable results are already cloned;
- every storage-sourced entry and snapshot is cloned before leaving
  `RaftLog`, including stable-only and size-truncated paths;
- merged ranges use new arrays;
- snapshots returned to callers are clones;
- append inputs are cloned when retained by `UnstableLog`.

No additional lock is used; serialized ownership is mandatory.

## Test plan

Tests port the relevant `log_test.go` matrix:

1. conflict discovery and backward term search;
2. candidate log freshness;
3. append replacement and committed-entry protection;
4. `MaybeAppend` match, conflict, commit, and failure cases;
5. behavior after stable-storage compaction;
6. pending and in-progress unstable entry access;
7. commit advancement and current-term commit checks;
8. term lookup with stable and unstable snapshots;
9. first/last indexes and restore;
10. committed-entry availability with stable-only and unstable application;
11. applying byte-limit pause and acknowledgement accounting;
12. atomic snapshot application acknowledgement and stale restore rejection;
13. out-of-bounds and compacted slicing;
14. size-limited slices entirely stable, entirely unstable, and crossing the
    boundary;
15. paged scanning and callback exception propagation.

Additional C# tests verify:

- storage sentinel exceptions are translated only at the documented call
  sites;
- temporary snapshot unavailability remains retryable;
- custom storage entries and snapshots are cloned before exposure;
- returned entries and snapshots cannot mutate either backing layer;
- arithmetic and cursor invariant failures leave state unchanged.

No test uses wall-clock waits.

## Development sequence

D06 uses strict test-driven development:

1. add the ported log behavior tests before `RaftLog` exists;
2. run the focused target and record the expected red compile state;
3. implement the smallest behavior needed to pass each test group;
4. refactor shared range, size, and cursor logic;
5. run formatting and the full suite before code review.

## Completion criteria

D06 is complete when:

- this design is reviewed and accepted;
- the unified log and cursor lifecycle are implemented;
- all translated and ownership tests pass;
- formatting, build, and full tests pass;
- code review has no unresolved substantiated findings;
- the reviewed changes are committed before D07 starts.
