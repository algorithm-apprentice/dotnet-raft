# D05 Unstable Log Design

- **Status:** Accepted
- **Date:** 2026-09-30
- **DAG node:** D05
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D05 implements the in-memory portion of the Raft log that is not yet
guaranteed to be present in stable storage. It holds newly appended entries and
an optional incoming snapshot while preserving a complete log view during
asynchronous or synchronous persistence.

The buffer has two distinct responsibilities:

1. retain entries and snapshots that have not yet been handed to a storage
   worker; and
2. continue retaining work already handed to storage until a matching
   completion proves that it is durable.

This separation is required by D06, D21, and D25. A `Ready` or asynchronous
storage message may be in progress while the Raft core appends or replaces a
later unstable suffix.

## Goals

- Preserve the pinned `etcd/raft` unstable-log state transitions.
- Distinguish pending persistence from persistence already in progress.
- Ignore stale or mismatched persistence completions without discarding newer
  entries.
- Preserve snapshot and entry persistence as independent lifecycles.
- Prevent mutable protobuf aliasing across core and host boundaries.
- Keep all behavior synchronous and deterministic.

## Non-goals

- Reading stable entries from `IStorage`; D06 combines both log layers.
- Performing persistence or exposing `Ready`; those belong to D21 and D25.
- Tracking commit, apply, or applying cursors; those belong to D06.
- Synchronizing concurrent callers. The Raft core owns `UnstableLog` from one
  serialized event loop.

## Type and construction

The implementation is an internal class:

```csharp
internal sealed class UnstableLog
{
    internal UnstableLog(
        ulong offset,
        IEnumerable<Entry> entries,
        Snapshot? snapshot,
        ulong offsetInProgress,
        bool snapshotInProgress,
        IRaftLogger logger);
}
```

D06 will normally construct an empty buffer with both offsets equal to the
stable storage `LastIndex + 1`. The complete constructor remains internal so
ported D05 tests can exercise every lifecycle state directly.

Constructor inputs are validated:

- entries are non-null and contiguous;
- when entries are present, the first entry index equals `offset`;
- `offset <= offsetInProgress <= offset + entries.Count`;
- a retained snapshot index is strictly less than `offset`;
- `snapshotInProgress` requires a snapshot;
- no retained entry or snapshot index is `ulong.MaxValue`, because the
  exclusive next index must remain representable.

## State and invariants

```text
snapshot            optional incoming snapshot
entries             contiguous unstable entries
offset              entries[0] index, or next unstable entry index if empty
snapshotInProgress  snapshot has been handed to persistence
offsetInProgress    exclusive end of entries handed to persistence
logger              diagnostic sink
```

The invariants are:

```text
entries[i].Index = offset + i
offset <= offsetInProgress <= offset + entries.Count
snapshot == null || snapshot.Metadata.Index < offset
```

`offset` may be less than the stable storage last index. Persisting the
unstable suffix may therefore require stable storage to truncate an existing
suffix before appending it.

Entry and snapshot progress are independent. A snapshot may be in progress
while newly appended entries remain pending, and entry persistence may finish
before snapshot persistence.

## Internal API

```csharp
internal ulong Offset { get; }

internal ulong OffsetInProgress { get; }

internal bool HasEntries { get; }

internal bool HasSnapshot { get; }

internal bool TryGetFirstIndex(out ulong index);

internal bool TryGetLastIndex(out ulong index);

internal bool TryGetTerm(ulong index, out ulong term);

internal IReadOnlyList<Entry> GetNextEntries();

internal Snapshot? GetNextSnapshot();

internal Snapshot? GetSnapshot();

internal void AcceptInProgress();

internal void StableTo(EntryId entryId);

internal void StableSnapshotTo(ulong index);

internal void Restore(Snapshot snapshot);

internal void TruncateAndAppend(IEnumerable<Entry> entries);

internal IReadOnlyList<Entry> Slice(
    ulong lowInclusive,
    ulong highExclusive);
```

Empty entry results use an empty collection rather than a nullable collection.
The optional snapshot remains nullable because absence is semantically
distinct from an empty snapshot.

`Offset` is the authoritative boundary used by D06 to split stable and
unstable ranges and to cap application when unstable entries are not yet
durable. It cannot be reconstructed from storage, snapshot, or pending-work
queries because it may precede `Storage.LastIndex` and
`OffsetInProgress`. `OffsetInProgress` is exposed internally for lifecycle
invariant checks and focused tests.

## Ownership

Generated protobuf messages are mutable. `UnstableLog` therefore:

- clones constructor, restore, and append inputs before retaining them;
- returns cloned entries from `GetNextEntries` and `Slice`;
- returns cloned snapshots from `GetNextSnapshot` and `GetSnapshot`;
- never exposes its mutable internal `List<Entry>`.

The reference implementation shares entry objects under an immutability
contract. Defensive cloning is used here because `Ready` eventually crosses
the library boundary and C# collections do not provide the same slice-capacity
protection as Go. `ByteString` remains immutable.

## Lookup semantics

### `TryGetFirstIndex`

- Returns `snapshot.Metadata.Index + 1` when a snapshot exists.
- Returns `false` when no snapshot exists, regardless of entries.

The method describes the first index made possible by an unstable snapshot,
not the first unstable entry.

### `TryGetLastIndex`

- Returns the last unstable entry index when entries exist.
- Otherwise returns the snapshot index when a snapshot exists.
- Returns `false` when both are absent.

Entries take precedence even if a snapshot is also retained.

### `TryGetTerm`

- For an index below `offset`, returns the snapshot term only when the index
  exactly equals the snapshot index.
- For an index at or above `offset`, returns the matching retained entry term.
- Returns `false` for gaps, indexes after the unstable suffix, and snapshot
  indexes not equal to the requested index.

## Persistence lifecycle

### Pending work

`GetNextEntries` returns the suffix beginning at `offsetInProgress`.
`GetNextSnapshot` returns the snapshot only when it exists and is not already
in progress.

Both methods return clones. Calling either method does not change progress.

### `AcceptInProgress`

- Advances `offsetInProgress` to one past the final retained entry.
- Marks the snapshot in progress when one exists.
- Is idempotent when all retained work is already in progress.
- Newly appended entries after the call remain pending until a later call.

D21 must collect both pending entries and the pending snapshot and call
`AcceptInProgress` as one synchronous `Ready` operation with no intervening
Raft mutation. The parameterless method deliberately marks the current
high-water marks, so it must never be called later by a worker after the event
loop has resumed.

### `StableTo`

A persistence completion removes entries only when:

1. the supplied index is present in unstable entries rather than only in the
   snapshot; and
2. the supplied term equals the currently retained term at that index.

Missing, snapshot-only, and term-mismatched completions are ignored and logged
at information level. A matching completion:

- removes entries through the supplied index;
- sets `offset` to `index + 1`;
- raises `offsetInProgress` to at least the new `offset`;
- releases references to removed entries.

The `(index, term)` check is necessary but does not by itself prevent an
A-to-B-to-A replacement race. The caller must additionally attest that no
in-progress append can later overwrite the acknowledged entries.

D25 must provide that attestation by term- or generation-gating ordered append
responses before calling `StableTo`. Later append or `HardState` responses must
cumulatively acknowledge the current last unstable entry even when their own
batch contains no entries, so dropping a stale response does not lose
liveness. Snapshot completion remains independent and may still be processed
from a response whose entry acknowledgement is stale.

### `StableSnapshotTo`

Clears the retained snapshot and its in-progress marker only when the supplied
index exactly matches the current snapshot index. Other indexes are ignored.

## Mutation semantics

### `Restore`

Restoring a snapshot:

- clones and normalizes the snapshot;
- rejects index `ulong.MaxValue`;
- clears all unstable entries;
- sets both offsets to `snapshot.Metadata.Index + 1`;
- stores the snapshot;
- resets snapshot progress.

### `TruncateAndAppend`

The input must contain at least one contiguous entry and its final index must
have a representable successor.

When a snapshot is retained, the first incoming entry must be strictly greater
than the snapshot index. Entries represented by the snapshot cannot re-enter
the unstable suffix.

Given `fromIndex = incoming[0].Index`:

- if `fromIndex == offset + entries.Count`, append directly and preserve
  existing progress;
- if `fromIndex <= offset`, replace the complete unstable suffix, set
  `offset = fromIndex`, and reset `offsetInProgress = offset`;
- otherwise retain entries before `fromIndex`, replace entries from
  `fromIndex` onward, and clamp `offsetInProgress` to at most `fromIndex`.

Replacing entries invalidates only persistence progress that overlaps the
replacement. Progress strictly before the replacement remains valid.

## Slicing

`Slice` uses the half-open range `[lowInclusive, highExclusive)`.

It throws `RaftInvariantException` when:

- the range is reversed;
- `lowInclusive < offset`; or
- `highExclusive > offset + entries.Count`.

An empty in-bounds range returns an empty collection. Returned entries are
clones.

## Diagnostics

The logger is required but may be `NullRaftLogger.Instance`. Information-level
messages are emitted only for ignored `StableTo` completions and suffix
replacement/truncation. Logging does not affect state transitions.

## Test plan

Tests port the complete `log_unstable_test.go` behavior matrix:

1. first-index lookup with and without a snapshot;
2. last-index lookup precedence between entries and snapshot;
3. term lookup in entries, snapshot, gaps, and out-of-range indexes;
4. restore behavior and progress reset;
5. pending entry and snapshot selection;
6. accepting zero, partial, and complete work in progress;
7. matching, missing, snapshot-only, and term-mismatched stabilization;
8. snapshot stabilization by exact index;
9. direct append, complete replacement, and partial truncation;
10. valid and invalid slices;
11. constructor and append rejection when snapshot placement would violate
    `snapshot.Index < offset`.

Additional C# tests verify:

- constructor, append, and restore input aliasing cannot mutate retained state;
- returned entries and snapshots cannot mutate retained state;
- `ulong.MaxValue` mutations are rejected before state changes;
- invariant failures leave the buffer unchanged.

D25 must additionally port the upstream ABA interaction scenario in which an
older append completion arrives after A-to-B-to-A suffix replacement.

No test uses wall-clock waits.

## Development sequence

D05 uses strict test-driven development after design acceptance:

1. add the ported behavior and ownership tests before production code;
2. run the focused test target and record the expected compile or assertion
   failure;
3. implement only enough unstable-log behavior to make the focused tests pass;
4. refactor without changing behavior;
5. run formatting and the complete test suite before code review.

## Completion criteria

D05 is complete when:

- this design is reviewed and accepted;
- the unstable buffer and lifecycle above are implemented;
- all translated and ownership tests pass;
- formatting, build, and full tests pass;
- code review has no unresolved substantiated findings;
- the reviewed changes are committed before D06 starts.
