# D08 Follower Progress Design

- **Status:** Accepted
- **Date:** 2026-09-30
- **DAG node:** D08
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D08 models the leader's current knowledge about one follower. The progress
object determines:

- which log index should be sent next;
- whether the leader is probing, replicating optimistically, or waiting for a
  snapshot;
- whether append flow is currently paused;
- which append messages remain in flight; and
- whether a newer commit index may need to be sent.

D09 stores one progress object per voter or learner. D15 and D16 drive its
replication and snapshot transitions.

## Goals

- Preserve the pinned `tracker.Progress` state-machine behavior.
- Maintain the `Match < Next` replication invariant.
- Support probe, replicate, and snapshot transitions.
- Integrate D07 count- and byte-based append flow control.
- Ignore stale acknowledgements and rejections without regressing progress.
- Regress genuine rejected probes to a safe next index.
- Track the most recently sent commit index to avoid redundant empty appends.
- Fail atomically when an index calculation is not representable.

## Non-goals

- Owning the collection of peers; D09 implements `ProgressTracker`.
- Choosing entries or snapshots to send; D15 and D16 query `RaftLog`.
- Processing Raft messages or changing core roles.
- Synchronizing concurrent callers. One serialized Raft event loop owns each
  progress object.
- Exposing progress as a public status model; that is added with the public
  status surface.

## States

```csharp
namespace DotnetRaft.Tracker;

internal enum ProgressState
{
    Probe,
    Replicate,
    Snapshot,
}
```

### Probe

The follower's exact log position is uncertain. At most one non-empty append
probe is sent until a response or heartbeat response resumes append flow.
Sending entries does not optimistically advance `Next`.

### Replicate

The follower is actively catching up. Sending entries optimistically advances
`Next` and adds the message's last index and payload bytes to the D07 inflight
window. Flow pauses when that window reaches either configured limit.

### Snapshot

The follower needs log entries no longer retained by the leader. A snapshot
has been selected for it, all append flow is paused, and `Next` remains one
greater than the selected snapshot index.

## Type and construction

```csharp
internal sealed class Progress
{
    internal Progress(
        ulong match,
        ulong next,
        int maxInflightMessages,
        ulong maxInflightBytes,
        bool isLearner = false,
        bool recentActive = false);

    internal ulong Match { get; }
    internal ulong Next { get; }
    internal ulong LastSentCommit { get; }
    internal ProgressState State { get; }
    internal ulong PendingSnapshot { get; }
    internal bool RecentActive { get; set; }
    internal bool AppendFlowPaused { get; set; }
    internal InflightWindow Inflights { get; }
    internal bool IsLearner { get; set; }
}
```

Construction creates an empty inflight window and starts in `Probe`.
`next` must be nonzero and strictly greater than `match`. Negative inflight
capacity is rejected by D07. A zero capacity remains valid at this layer and
represents an always-full window; D12 configuration validation decides
whether it is acceptable for a running node.

`Match`, `Next`, state, pending snapshot, and sent-commit bookkeeping are
changed only by progress methods. Activity, flow-pause, and learner flags
remain writable by the owning core because message handling and membership
changes update them directly.

## State transitions

### ResetState

```csharp
private void ResetState(ProgressState state);
```

This shared transition primitive:

- clears `AppendFlowPaused`;
- clears `PendingSnapshot`;
- sets `State`; and
- resets the inflight window.

It preserves match, next, sent-commit, activity, and learner status.

### BecomeProbe

```csharp
internal void BecomeProbe();
```

From `Snapshot`, the next probe index is:

```text
max(Match + 1, PendingSnapshot + 1)
```

This lets a successfully transmitted snapshot remain the retry basis even
before an append acknowledgement arrives.

From any other state, the next probe index is:

```text
Match + 1
```

After transition, `LastSentCommit` is capped at `Next - 1` because a rejected
or snapshot-recovering follower may not have received a later commit.

`ResetState` is private so callers cannot create a snapshot state without the
required snapshot fields.

### BecomeReplicate

```csharp
internal void BecomeReplicate();
```

The state and inflight window are reset, then:

```text
Next = Match + 1
```

### BecomeSnapshot

```csharp
internal void BecomeSnapshot(ulong snapshotIndex);
```

The state and inflight window are reset, then:

```text
PendingSnapshot = snapshotIndex
Next = snapshotIndex + 1
LastSentCommit = snapshotIndex
```

Snapshot state is always paused regardless of `AppendFlowPaused`.

Every transition computes required successor indexes before mutating state.
`ulong.MaxValue + 1` is unrepresentable and throws
`RaftInvariantException` without partially resetting the progress.

`snapshotIndex` must be nonzero and at least `Match`. This preserves
`Match < Next` and rejects attempts to send an empty or already-obsolete
snapshot.

### Snapshot reports

```csharp
internal void ReportSnapshot(bool succeeded);
```

This operation is valid only in `Snapshot` state and atomically implements the
order-sensitive transition used by D16.

For a successful report:

1. preserve `PendingSnapshot` as the retry basis;
2. transition through `BecomeProbe()`; and
3. set `AppendFlowPaused = true`.

The resulting probe starts at:

```text
max(Match + 1, previous PendingSnapshot + 1)
```

and remains paused until an append acknowledgement or heartbeat response.

For a failed report:

1. ignore `PendingSnapshot` as a retry basis;
2. transition to probe with `Next = Match + 1`; and
3. set `AppendFlowPaused = true`.

This waits for a heartbeat interval before another snapshot attempt. Required
successor indexes are computed before mutation, so either outcome is atomic on
overflow.

## Recording sent appends

```csharp
internal void SentEntries(int entryCount, ulong bytes);
```

Negative entry counts are rejected.

In `Replicate`:

- a non-empty append computes `newNext = Next + entryCount`;
- its last index is `newNext - 1`;
- the inflight is added before `Next` is assigned, so an inflight or arithmetic
  failure leaves `Next` unchanged; and
- `AppendFlowPaused` becomes the inflight window's current fullness.

An empty append does not add an inflight or change `Next`, but still refreshes
`AppendFlowPaused` from the window. This is required after a heartbeat response
temporarily resumes a follower whose existing inflight window is still full.

In `Probe`, a non-empty append sets `AppendFlowPaused` without advancing
`Next` or adding an inflight. An empty append preserves the current pause flag,
matching the pinned behavior.

Sending append entries in `Snapshot` or an unknown state is an invariant
violation.

## Commit-send tracking

```csharp
internal bool CanBumpCommit(ulong index);
internal void RecordSentCommit(ulong commit);
```

`RecordSentCommit` overwrites the previous value with the commit index carried
by an append or heartbeat. It does not take the maximum. The value may regress
because heartbeats carry `min(Match, Committed)`, as well as when entering
probe state or handling a rejection.

`CanBumpCommit(index)` is true only when:

```text
index > LastSentCommit
    && LastSentCommit < Next - 1
```

The second condition means the follower could advance its commit at least to
the preceding-log index of a new empty append. It avoids sending a redundant
empty append when the already-sent commit reaches that bound.

## Acknowledgements

```csharp
internal bool MaybeUpdate(ulong acknowledgedIndex);
```

An acknowledgement at or below `Match` is stale and returns `false` without
mutation.

A newer acknowledgement:

```text
Match = acknowledgedIndex
Next = max(Next, acknowledgedIndex + 1)
AppendFlowPaused = false
```

and returns `true`. The successor index is validated before any mutation.
Freeing acknowledged inflights remains a D15 caller responsibility after it
has interpreted the current progress state.

## Rejections

```csharp
internal bool MaybeDecrementTo(
    ulong rejectedIndex,
    ulong matchHint);
```

### Replicate rejection

If `rejectedIndex <= Match`, the rejection is stale and is ignored.
Otherwise optimistic replication is abandoned:

```text
Next = Match + 1
LastSentCommit = min(LastSentCommit, Next - 1)
```

The caller subsequently transitions to `Probe`, which clears inflights and
append-flow pause state.

### Probe or snapshot rejection

Only a rejection for the latest probe is accepted:

```text
rejectedIndex == Next - 1
```

Other rejections are stale or reordered and are ignored.

A genuine rejection computes:

```text
Next = max(
    min(rejectedIndex, matchHint + 1),
    Match + 1)
```

The `min` calculation avoids evaluating `matchHint + 1` when
`matchHint >= rejectedIndex`, so a maximum-valued hint cannot overflow when
the bounded result is already known. The transition then caps
`LastSentCommit` at `Next - 1`, clears `AppendFlowPaused`, and returns `true`.

## Pause behavior

```csharp
internal bool IsPaused { get; }
```

| State | Result |
|---|---|
| `Probe` | `AppendFlowPaused` |
| `Replicate` | `AppendFlowPaused` |
| `Snapshot` | always `true` |

An unknown enum value throws `RaftInvariantException`.

In replicate state, D15 explicitly synchronizes `AppendFlowPaused` with D07
fullness through `SentEntries`. Heartbeat responses may temporarily clear the
flag even while the window remains full so one empty append can test whether
the follower is reachable.

## Diagnostics

`ToString()` preserves the pinned diagnostic shape:

```text
StateReplicate match=5 next=8 learner paused pendingSnap=7 inactive inflight=2[full]
```

Optional suffixes appear only when applicable:

- `learner`;
- `paused`;
- `pendingSnap=<index>`;
- `inactive`;
- `inflight=<count>` and `[full]`.

All numeric formatting is culture invariant.

## Invariants

For every valid progress object:

```text
Match < Next
Next >= 1
StateSnapshot implies PendingSnapshot > 0
states other than StateSnapshot imply PendingSnapshot == 0
StateSnapshot implies IsPaused
Inflights contains strictly increasing append-message last indexes
```

Immediately after `BecomeSnapshot`, the additional postconditions are:

```text
Next == PendingSnapshot + 1
LastSentCommit == PendingSnapshot
```

These equalities are not permanent invariants. `MaybeUpdate` and
`MaybeDecrementTo` preserve the pinned behavior while snapshot transmission is
outstanding and may change `Match`, `Next`, or `LastSentCommit` independently
of `PendingSnapshot`.

D16 aborts snapshot state after a successful append acknowledgement whenever:

```text
Match + 1 >= RaftLog.FirstIndex
```

This condition intentionally does not compare the acknowledgement with
`PendingSnapshot`. A system may install a snapshot from another source at an
index ahead of or behind the leader-selected snapshot. If the follower's
acknowledgement reconnects it to the leader's retained log, D16 calls
`BecomeProbe()` and then `BecomeReplicate()`. The final replicate transition
uses `Match + 1`, regardless of the pending snapshot index.

Stale acknowledgements and rejections leave all fields unchanged. Arithmetic,
state, and inflight failures must not leave partial progress updates.

## Test plan

Tests first port `tracker/progress_test.go`:

1. exact diagnostic string shape;
2. pause behavior in all three states;
3. acknowledgement and rejection resume behavior;
4. probe transitions from replicate, successful snapshot, and failed
   snapshot;
5. replicate and snapshot transitions;
6. stale and advancing acknowledgement cases;
7. replicate and probe rejection tables.

Additional C# tests cover behavior now present in the pinned implementation
but not directly exercised by that file:

8. reset clears pending snapshot, pause state, and inflights;
9. replicate sends advance `Next`, track bytes, and pause at D07 limits;
10. empty replicate sends re-pause a still-full window;
11. probe sends pause without optimistic advancement;
12. snapshot sends are rejected;
13. commit bump eligibility, including a high-to-low heartbeat-style overwrite
    followed by renewed bump eligibility;
14. successful and failed snapshot reports both enter paused probe state with
    the correct retry basis;
15. an acknowledgement below `PendingSnapshot` can still reconnect snapshot
    state to the retained log through probe and replicate;
16. snapshot-state rejection preserves pinned non-replicate rejection
    behavior;
17. `matchHint == ulong.MaxValue` uses the bounded rejection result without
    wrapping or throwing;
18. a replicate send against a full inflight window fails without changing
    `Next` or any other progress field;
19. invalid construction, negative entry counts, unknown states, and successor
    index overflow fail without partial mutation.

## Development sequence

D08 uses strict test-driven development:

1. review and accept this design;
2. add all progress tests before production types;
3. run the focused target and record the expected compile failure;
4. implement the minimum state machine that passes;
5. refactor while preserving the translated matrix;
6. run formatting and the full suite before code review.

## Completion criteria

D08 is complete when:

- this design is reviewed and accepted;
- follower progress states and transitions are implemented;
- all translated and additional tests pass;
- formatting, build, and full tests pass;
- code review has no unresolved substantiated findings;
- the reviewed changes are committed before D09 starts.
