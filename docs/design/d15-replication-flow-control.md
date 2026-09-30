# D15 Replication Flow Control Design

- **Status:** Accepted
- **Date:** 2026-09-30
- **DAG node:** D15
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D15 turns D14's one-probe-at-a-time replication into the bounded optimistic
pipeline used by the pinned reference. Leaders transition followers from
probe to replicate state, send size-limited append batches until inflight
limits are reached, recover stalled pipelines through acknowledgements and
heartbeats, and use term-aware rejection hints to avoid linear backtracking
through divergent logs.

The node also activates the configured uncommitted-payload quota. The
deterministic core accounts for accepted proposals now and exposes the
explicit reduction primitive that D21 will call when committed entries are
acknowledged as applied.

## Goals

- Pause probe state after one append containing entries.
- Enter replicate state after a successful probe acknowledgement.
- Optimistically advance replicate `Next` as append batches are sent.
- Bound append messages by `MaxSizePerMessage`.
- Bound optimistic pipelines by message count and payload bytes.
- Free inflight messages through acknowledged indexes.
- Use heartbeat responses to recover full or lost pipelines.
- Return follower-side rejection hints and log terms.
- Skip incompatible leader terms when handling rejection hints.
- Return replicate progress to probe state after rejection or unreachable
  reporting.
- Track proposal payload bytes against
  `MaxUncommittedEntriesSize`.
- Preserve D14 durability, ownership, quorum-commit, and current-term safety.

## Non-goals

- Sending, receiving, or reporting snapshots; D16 owns snapshot behavior.
- Recovering `ProgressState.Snapshot` through append acknowledgements; D16
  adds that transition with retained-log boundary checks.
- Validating or applying membership changes; D17 owns live membership.
- Read-index contexts; D18 owns safe-read acknowledgement processing.
- Quorum checks and leases; D19 owns liveness policy.
- Leadership transfer; D20 owns transfer-specific flow.
- Producing `Ready` batches or application acknowledgements; D21 owns the
  public integration lifecycle.
- Asynchronous storage response messages; D25 owns them.

## Core API addition

```csharp
internal sealed class RaftCore
{
    internal void ReduceUncommittedSize(ulong payloadSize);
}
```

The method saturates at zero, matching the reference's tolerance for
underestimation. D21 will call it with the payload size of committed entries
whose application has completed. D15 tests call it directly because `Ready`
and application acknowledgement do not exist yet.

`Step` additionally handles `MsgUnreachable` for current-term leaders.

## Append sending

The D14 sender becomes:

```text
MaybeSendAppend(destination, sendIfEmpty)
```

It returns `true` only when it emits a message.

### Pause gate

If `progress.IsPaused` is true, no append is sent. This gives probe state one
outstanding nonempty append and stops replicate state when its flow-control
window is full.

A heartbeat response may temporarily clear `AppendFlowPaused` while the
replicate inflight window remains full. In that case the sender is allowed
through the pause gate but deliberately chooses an empty append. Stepping
`Progress.SentEntries(0, 0)` synchronizes the pause flag back to the still-full
window.

### Entry selection

For an eligible progress record:

```text
previousIndex = progress.Next - 1
previousTerm  = Log.GetTerm(previousIndex)
```

Entries are selected as follows:

```text
replicate and Inflights.IsFull
    => no entries

otherwise
    => Log.GetEntries(progress.Next, MaxMessageSize)
```

The D06 size limiter always permits the first entry, even when that single
entry exceeds the configured limit. Therefore `MaxSizePerMessage = 0` means
at most one entry per append, not zero entries.

If no entries are selected and `sendIfEmpty` is false, no message is sent.
When a message is emitted, it carries the D14 previous index/term and current
commit fields.

Only `StorageError.Compacted` defers to D16 snapshot transmission. Retained
storage unavailability and invalid progress continue to fail explicitly.

### Progress update after send

After queuing the append:

```text
payloadBytes = sum(entry.Data.Length)
progress.SentEntries(entryCount, payloadBytes)
progress.RecordSentCommit(Log.Committed)
```

Probe state does not advance `Next`; a nonempty send pauses it.

Replicate state:

- advances `Next` by the number of entries sent;
- adds one inflight record ending at the final entry index;
- tracks payload bytes, not encoded protobuf bytes; and
- pauses when either the message-count or soft byte limit is full.

Empty append messages do not consume inflight slots.

## Broadcast behavior

Stable sorted progress traversal remains unchanged.

- Proposal and commit broadcasts call `MaybeSendAppend` once per remote peer
  with `sendIfEmpty = true`.
- After an acknowledgement changes flow state or frees inflights, the leader
  repeatedly calls `MaybeSendAppend(peer, false)` until no additional
  nonempty append can be sent.

This fills the available replicate window without generating redundant empty
messages.

## Successful append responses

Only a current-term leader with progress for `message.From` handles the
response. Future acknowledgements still fail before progress mutation.

An accepted response is actionable when either:

```text
progress.MaybeUpdate(message.Index)
```

advances `Match`, or the response confirms the existing `Match` while the
peer is in probe state. The latter case lets an unreachable but already
caught-up peer return to replicate state.

The actionable transition is:

```text
Probe:
    BecomeReplicate()

Replicate:
    Inflights.FreeThrough(message.Index)

Snapshot:
    retain snapshot state until D16
```

The leader then:

1. attempts current-term quorum commit;
2. broadcasts append messages if commit advances;
3. otherwise, only for a remote peer, sends an append when the peer may need a
   newer commit according to
   `progress.CanBumpCommit(Log.Committed)`; and
4. for remote peers, fills all newly available nonempty inflight capacity.

Self acknowledgements enter neither the commit-bump send path nor the remote
fill loop. This preserves D14's protection for multiple pending local
persistence acknowledgements.

Stale acknowledgements remain behavior-free apart from setting
`RecentActive`.

## Heartbeat responses

A known peer's current-term `MsgHeartbeatResp`:

```text
RecentActive = true
AppendFlowPaused = false
```

The leader sends one append when either:

- the peer's `Match` is behind `Log.LastIndex`; or
- the peer is in probe state, including an already caught-up peer recovering
  from `MsgUnreachable`.

For a full replicate inflight window this produces one empty append and
re-pauses the peer. The response to that append can then free or reset lost
pipeline state.

Snapshot progress remains paused until D16.

Every outbound heartbeat records the exact bounded commit carried to that
peer:

```text
heartbeatCommit = min(progress.Match, Log.Committed)
Send(MsgHeartbeat(Commit = heartbeatCommit))
progress.RecordSentCommit(heartbeatCommit)
```

`RecordSentCommit` is an overwrite, not a monotonic maximum. A later heartbeat
may legitimately carry a lower bounded commit after progress state changes.
Keeping this value synchronized prevents `CanBumpCommit` from emitting a
redundant commit-only append after the heartbeat already communicated the
latest safe commit.

## Follower rejection hints

On previous-entry mismatch, the follower computes:

```text
hintStart = min(message.Index, Log.LastIndex)
hint      = Log.FindConflictByTerm(hintStart, message.LogTerm)
```

It queues the durable rejection:

```text
Type       = MsgAppResp
To         = leader
Index      = message.Index
Reject     = true
RejectHint = hint.Index
LogTerm    = hint.Term
```

The hint is the greatest local `(index, term)` at or below the rejected index
whose term is no greater than the leader's rejected previous term. This skips
large follower suffixes whose terms are known to be incompatible.

The zero-index anchor remains non-rejectable. A received rejection for index
zero is ignored without retry.

## Leader rejection handling

The leader starts with the follower's `RejectHint`. When `message.LogTerm` is
nonzero, it further searches its own log:

```text
nextProbe = Log.FindConflictByTerm(
    message.RejectHint,
    message.LogTerm).Index
```

This skips leader-side runs of terms that cannot match the follower's
reported term.

If `Progress.MaybeDecrementTo(message.Index, nextProbe)` accepts the response:

1. replicate state transitions to probe state;
2. inflights and optimistic `Next` are reset by `BecomeProbe`; and
3. one new append probe is sent.

Stale rejections do not alter progress.

## Unreachable reporting

`MsgUnreachable` is local and handled only by a leader with progress for the
reported sender.

```text
Replicate => BecomeProbe()
Probe     => unchanged
Snapshot  => unchanged until D16
```

The transition resets optimistic `Next` to `Match + 1`, clears inflights, and
does not immediately send. A later heartbeat response or proposal broadcast
retries from probe state.

## Uncommitted proposal quota

Quota accounting uses entry payload bytes:

```text
sum(entry.Data.Length)
```

It excludes protobuf metadata, indexes, terms, and entry type.

Before appending a leader proposal, D15 computes the proposed new total
without mutating the log or accounting state.

A proposal is rejected with `ProposalDroppedException` only when all are
true:

```text
UncommittedSize > 0
proposalPayload > 0
UncommittedSize + proposalPayload > MaxUncommittedEntriesSize
```

Consequences matching the reference:

- the first nonempty proposal after the counter reaches zero is accepted even
  when it alone exceeds the limit;
- later nonempty proposals are rejected while the total is over the limit;
- empty proposals are always accepted;
- the leader no-op remains zero-sized and always succeeds; and
- overflow while computing or adding payload sizes fails explicitly before
  mutation.

After a successful append, `UncommittedSize` is set to the computed total.
Rejected proposals do not change the log, queues, progress, or counter.

`ReduceUncommittedSize(payloadSize)` subtracts with saturation:

```text
payloadSize >= UncommittedSize
    => 0

otherwise
    => UncommittedSize - payloadSize
```

Commit advancement alone does not reduce the counter. The reference releases
quota when committed entries are acknowledged as applied; D21 will wire that
lifecycle.

## Ownership and durability

- Size limiting reads cloned log entries and never exposes mutable storage.
- Outbound append messages remain queue-owned clones.
- `Progress.SentEntries` runs only after `Send` accepts the outbound message.
- Follower rejection responses remain in the after-append queue.
- Proposal quota rejection occurs before log or queue mutation.
- Inbound append responses and rejection hints are never modified.

## Invariants

```text
Probe has at most one outstanding nonempty append.
Replicate Next may lead Match but never moves below Match + 1.
Inflight records are strictly increasing and correspond to sent entry batches.
Inflight payload bytes use entry Data lengths.
A full pipeline emits no new entries until capacity is recovered.
Heartbeat recovery may emit one empty append through a full window.
Acknowledgement frees every inflight ending at or below its index.
Append and heartbeat sends keep LastSentCommit synchronized with their payload.
Replicate rejection and unreachable reporting discard optimistic pipeline state.
Rejection hints never move Next below Match + 1.
Quota rejection is atomic.
No-op and empty-entry proposals never consume quota.
```

## Test plan

### Size-limited sending

1. probe state emits one bounded append and pauses;
2. repeated proposals while probe-paused emit no append;
3. `MaxSizePerMessage = 0` still sends exactly one entry;
4. one oversized entry is sent alone;
5. stable and unstable entries obey the same message bound;
6. append fields and caller ownership remain unchanged.

### Probe and replicate transitions

7. a successful probe response enters replicate state;
8. an acknowledgement equal to probe `Match` also recovers replicate state;
9. replicate sends optimistically advance `Next`;
10. response handling fills all available nonempty pipeline slots;
11. stale responses do not free inflights or change progress;
12. with a nonzero commit and multiple pending local appends, an earlier self
    acknowledgement enters neither the commit-bump path nor the remote fill
    loop;

### Inflight flow control

13. message-count fullness pauses sends;
14. soft byte fullness may exceed the limit by one accepted batch and then
    pauses;
15. acknowledgements free all covered inflights and refill capacity;
16. a full window rejects further proposal broadcasts to that peer;
17. heartbeat response through a full window emits one empty append and
    re-pauses;
18. empty append messages do not consume inflight slots.
19. heartbeat sends overwrite `LastSentCommit` with their bounded commit, and
    a later acknowledgement does not emit a redundant commit-only append.

### Rejection optimization

20. follower mismatch responses include exact `RejectHint` and `LogTerm`;
21. follower hints skip a higher-term divergent suffix;
22. leader hints skip incompatible runs in its own log;
23. replicate rejection becomes probe and clears inflights;
24. stale and zero-index rejections remain behavior-free;
25. a long divergent suffix converges in probes proportional to term runs,
    not entry count.

### Unreachable recovery

26. replicate `MsgUnreachable` becomes probe at `Match + 1`;
27. probe and unknown-peer reports are no-ops;
28. heartbeat response retries a caught-up unreachable peer with an empty
    append and restores replicate state after acknowledgement.

### Uncommitted quota

29. proposal payload bytes increase `UncommittedSize`;
30. proposals at or below the limit are accepted;
31. a proposal exceeding a nonzero accumulated limit is dropped atomically;
32. one oversized proposal is accepted when the counter is zero;
33. a later nonempty proposal is then dropped;
34. empty proposals and the leader no-op consume no quota;
35. explicit reduction subtracts or saturates at zero;
36. commit without application does not release quota;
37. term reset clears quota.

### Interaction

38. a bounded two-node pipeline fills, frees, and commits in stable order;
39. lost optimistic appends recover through heartbeat and rejection;
40. the D14 three-node election and proposal scenario still converges.

## Design review resolution

The GPT-5.6 Sol design review found two correctness gaps:

1. the commit-bump retry path did not explicitly exclude self
   acknowledgements; and
2. heartbeat sends did not update `LastSentCommit`.

Both findings are accepted. Commit-bump and pipeline-fill sends are now
remote-only, and every heartbeat records its exact bounded commit. The test
plan covers both regressions.

## Development sequence

D15 uses strict test-driven development:

1. review and accept this design;
2. add D15 tests and update D14 expectations first;
3. record the focused red state;
4. implement only D15 flow-control and quota behavior;
5. preserve the D16-D21 boundaries above;
6. run formatting and the full suite;
7. resolve substantiated code-review findings before commit.

## Completion criteria

D15 is complete when:

- this design is reviewed and accepted;
- probe, replicate, inflight, heartbeat-recovery, and unreachable behavior
  match the pinned reference;
- rejection hints avoid linear divergent-log backtracking;
- configured message and quota limits are enforced;
- D14 safety and ownership invariants remain intact;
- snapshot, membership, read, lease, transfer, and `Ready` behavior are not
  implemented early;
- formatting, build, and the full suite pass;
- code review has no unresolved substantiated findings; and
- the reviewed changes are committed before D16 starts.
