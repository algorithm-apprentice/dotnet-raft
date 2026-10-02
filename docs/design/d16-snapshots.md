# D16 Snapshot Design

- **Status:** Accepted
- **Date:** 2026-09-30
- **DAG node:** D16
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D16 closes the compacted-log replication gap left by D14-D15. A leader whose
retained log cannot reach a follower sends a persisted snapshot, pauses that
peer's replication progress, and resumes probing after the host reports the
send result. A follower receiving a snapshot either ignores it, fast-forwards
an already matching log, or restores both log and membership atomically.

The deterministic core continues to expose explicit durability ordering. A
snapshot acknowledgement is queued after append and may be delivered only
after the host has persisted the complete captured batch: updated
`HardState`, unstable entries, and the unstable snapshot. D21 later packages
that work into `Ready`/`Advance`.

## Goals

- Send a snapshot when append replication reaches compacted history.
- Avoid sending snapshots to peers that are not recently active.
- Treat temporarily unavailable snapshots as retryable.
- Pause progress while a snapshot is in flight.
- Resume probe state after reported snapshot success or failure.
- Abort snapshot state when an append acknowledgement proves retained-log
  recovery is possible.
- Receive and own snapshot protobufs without mutating caller data.
- Ignore obsolete snapshots.
- Fast-forward commit when the local log already contains the snapshot point.
- Restore the log and `ConfState` for a genuinely newer snapshot.
- Preserve learner, joint-consensus, and local-membership state.
- Delay `MsgAppResp` until snapshot durability.

## Non-goals

- Exposing `ReportSnapshot` publicly; D21 adds `RawNode.ReportSnapshot`.
- Persisting or applying snapshots inside the core; the host owns storage and
  state-machine I/O.
- Applying live configuration-change log entries; D17 owns them.
- Safe reads, leases, transfer, or asynchronous storage messages; D18-D20 and
  D25 own those behaviors.
- Snapshot generation or compaction policy; the host creates and retains
  snapshots through `IStorage`.

## Core message behavior

`Step` gains D16 payload handling for:

```text
MsgSnap
MsgSnapStatus
```

`MsgSnap` remains a leader message for D13 term and role handling. Followers,
candidates, and pre-candidates that accept the leader identity process the
snapshot payload. An equal-term leader still ignores a competing snapshot.

`MsgSnapStatus` is local, termless, and handled only by a leader with progress
for the reported sender.

## Snapshot sending

D15's append sender still distinguishes compacted history from retained
storage failure:

```text
StorageError.Compacted
    => MaybeSendSnapshot(peer, progress)

StorageError.Unavailable or other failures
    => propagate explicitly
```

### Activity gate

The leader sends no snapshot when:

```text
progress.RecentActive == false
```

Snapshot transmission can be expensive. A recent append, heartbeat, or
rejection response first proves the peer is reachable.

### Snapshot acquisition

The leader calls `Log.GetSnapshot()`.

- `StorageError.SnapshotTemporarilyUnavailable` returns `false` without
  changing progress or queues.
- Other storage and invariant failures propagate.
- Snapshot index zero is invalid for transmission and throws
  `RaftInvariantException`.

The returned snapshot is already persisted storage state. `Send` clones the
message, so later caller or storage mutation cannot affect the queue.

### Progress transition

Before queuing `MsgSnap`:

```text
progress.BecomeSnapshot(snapshot.Metadata.Index)
```

This sets:

```text
State           = Snapshot
PendingSnapshot = snapshot index
Next            = snapshot index + 1
LastSentCommit  = snapshot index
Inflights       = empty
AppendFlowPaused = false
```

`ProgressState.Snapshot` is always paused, so proposals, commit broadcasts,
and heartbeat responses cannot send appends until snapshot state ends.

The outbound message contains:

```text
Type     = MsgSnap
To       = peer
Snapshot = owned snapshot clone
```

## Snapshot send reporting

A leader ignores `MsgSnapStatus` from unknown peers or peers not currently in
snapshot state.

### Success

For `Reject == false`:

```text
progress.ReportSnapshot(succeeded: true)
```

The peer enters paused probe state with:

```text
Next = max(Match + 1, old PendingSnapshot + 1)
PendingSnapshot = 0
```

### Failure

For `Reject == true`:

```text
progress.ReportSnapshot(succeeded: false)
```

The peer enters paused probe state from:

```text
Next = Match + 1
PendingSnapshot = 0
```

Both outcomes remain paused until a later heartbeat response clears the pause
and sends the next probe. Reporting does not itself emit a message.

## Append acknowledgement aborting snapshot state

D15's accepted `MsgAppResp` handling gains the pinned snapshot recovery
branch.

After `MaybeUpdate(message.Index)`, a snapshot-state peer may resume
replication when:

```text
progress.Match + 1 >= Log.FirstIndex
```

The condition proves the leader still retains the entry immediately after the
peer's acknowledged prefix. The transition is:

```text
BecomeProbe()
BecomeReplicate()
```

It clears `PendingSnapshot`, resets inflights, and lets the existing D15 fill
loop send retained entries.

An acknowledgement below the retained boundary leaves the peer in snapshot
state. D15's commit and fill paths remain unable to send while snapshot state
is paused.

## Snapshot receiving

The receiver clones and normalizes the inbound snapshot:

```text
Snapshot       = non-null
Metadata       = non-null
Metadata.ConfState = non-null
missing scalar fields = zero
```

The caller's `Message`, `Snapshot`, metadata, data, and `ConfState` remain
unchanged and independently owned.

`HandleSnapshot` calls the restore algorithm and always queues a durable
`MsgAppResp`:

```text
restored == true
    => Index = Log.LastIndex

restored == false
    => Index = Log.Committed
```

The response is after-append even when the snapshot is ignored, preserving
the existing response durability classification and the reference behavior.

## Restore algorithm

### Obsolete snapshot

When:

```text
snapshot.Metadata.Index <= Log.Committed
```

the snapshot is ignored without changing log or configuration.

### Role defense

Restore normally runs only after D13 has made the receiver a follower. If a
non-follower reaches the private restore method, the core defensively becomes
a follower in the next term and ignores the snapshot. This path is not a
normal protocol transition.

### Local membership defense

The snapshot is ignored unless the local ID appears in at least one of:

```text
ConfState.Voters
ConfState.Learners
ConfState.VotersOutgoing
```

`LearnersNext` does not need a separate check because a valid joint
configuration also contains those IDs in `VotersOutgoing`.

This prevents installing a configuration that removes the receiving core from
its own progress tracker. D17 later handles live removal through committed
configuration entries.

### Matching snapshot point

If the local log already matches:

```text
(snapshot.Metadata.Index, snapshot.Metadata.Term)
```

the core advances commit to the snapshot index and returns `false`. It does
not install an unstable snapshot or replace the current configuration.

This fast path preserves retained entries after the snapshot point.

### Full restore

For a genuinely newer, nonmatching snapshot:

1. validate and restore its `ConfState` into a fresh scratch
   `ProgressTracker` using the existing D10 algorithm;
2. fail before log or tracker mutation if the configuration is invalid;
3. call `Log.Restore(snapshot)`;
4. replace the current tracker configuration and progress map with the
   validated result;
5. clear recorded votes;
6. recompute `IsLearner`; and
7. return `true`.

The fresh tracker preserves configured inflight limits. Progress records use
the snapshot index as their last-index basis:

```text
Match = 0
Next  = max(snapshot index, 1)
```

For every full restore the snapshot index is nonzero, so `Next` equals the
snapshot index. This is the existing D10 `ConfigurationRestore` behavior and
matches the pinned configuration changer. A later role reset initializes
normal replication progress from `Log.LastIndex + 1`.

The local core may become a learner or be promoted from learner to voter.
Incoming/outgoing voters and `LearnersNext` are restored through D10's
equivalence-checked algorithm.

`Log.Restore` makes the snapshot unstable:

```text
Committed = snapshot index
LastIndex = snapshot index
FirstIndex = snapshot index + 1
unstable entries = empty
unstable snapshot = owned clone
```

Until the host persists and acknowledges that snapshot, `Promotable` remains
false and committed-entry delivery remains blocked.

## Persistence ordering

For a received snapshot:

1. `Step(MsgSnap)` installs the unstable snapshot and queues an after-append
   `MsgAppResp`;
2. the host captures the complete batch containing the updated `HardState`,
   unstable entries, unstable snapshot, and after-append messages;
3. the host persists the batch's `HardState`, entries, and snapshot before
   releasing any captured after-append response;
4. the host logically accepts snapshot application through the D21 lifecycle,
   which will call the existing `Log.AcknowledgeSnapshot(index)` while
   preserving ordered physical state-machine application; and
5. only the responses captured with that durable batch may be delivered.

D16 tests perform these explicit hard-state and log/storage operations
directly. D21 later owns batch capture, logical acknowledgement, and ordered
application delivery. D16 does not require slow physical state-machine
restoration to finish before the durability response; it requires the same
logical persistence boundary as the pinned `Ready` contract.

For an ignored or fast-forwarded snapshot, there is no unstable snapshot, but
the queued response still follows the after-append queue contract because the
message may have advanced durable term or commit. A higher-term obsolete
snapshot, for example, requires a `HardState` write even though it creates no
unstable snapshot.

## Interaction with storage errors

```text
SnapshotTemporarilyUnavailable while sending
    => no message; retry on later replication activity

Compacted previous entry while sending append
    => try snapshot

Unavailable previous entry or retained entries
    => explicit failure

SnapshotOutOfDate while host persists a received snapshot
    => host/storage lifecycle error, outside Step
```

The core never converts unavailable retained state into a snapshot-shaped
success.

## Ownership

- `Step` never mutates inbound snapshot messages.
- Restore normalizes a clone.
- `RaftLog`, `UnstableLog`, tracker configuration, and outbound messages own
  independent clones.
- `Log.GetSnapshot()` already returns an owned value.
- `Send` clones the complete `MsgSnap`.
- Mutating the input or a dequeued message after `Step` cannot change core
  state.

## Invariants

```text
Snapshot progress is always paused.
Only recently active peers receive snapshots.
PendingSnapshot equals the index carried by the sent snapshot.
Snapshot status applies only to snapshot-state progress.
Received snapshot responses wait for durability.
Committed index never decreases.
Obsolete snapshots never replace log or membership.
Matching snapshot points only fast-forward commit.
Full restore changes log and membership atomically.
The local node remains represented in restored progress.
An unstable snapshot blocks campaigning until persistence acknowledgement.
```

## Test plan

### Sending

1. compacted append history sends `MsgSnap` to a recently active peer;
2. the message contains an owned snapshot clone;
3. progress enters snapshot state with the exact pending index;
4. snapshot state suppresses proposal and heartbeat-response appends;
5. an inactive peer receives no snapshot;
6. a temporarily unavailable snapshot leaves progress and queues unchanged;
7. an empty storage snapshot fails explicitly;
8. unavailable retained state still propagates rather than sending a
   snapshot.

### Reporting and recovery

9. successful status enters paused probe at `PendingSnapshot + 1`;
10. failed status enters paused probe at `Match + 1`;
11. unknown-peer and wrong-state reports are no-ops;
12. a later heartbeat response clears the pause and retries;
13. with `FirstIndex = 6` and `PendingSnapshot = 10`, an acknowledgement at
    index 5 aborts snapshot state, clears `PendingSnapshot`, enters replicate
    state, and sends retained entries even though the acknowledgement is below
    the pending snapshot index;
14. in the same setup, an acknowledgement at index 4 leaves snapshot state
    paused.

### Receiving and restore

15. a valid snapshot restores log, commit, configuration, and learner state;
16. the input message and nested protobufs remain unchanged;
17. the durable response acknowledges the restored last index;
18. restored progress records use `Match = 0` and
    `Next = snapshot.Metadata.Index`;
19. persistence acknowledgement clears the unstable snapshot and restores
    promotability;
20. full restore persists a matching `HardState` and restarts successfully
    from the saved snapshot boundary;
21. obsolete higher-term snapshots persist the new term before their response
    even though no unstable snapshot exists;
22. a matching snapshot point persists the fast-forwarded commit before its
    response and restarts successfully;
23. obsolete snapshots otherwise leave state unchanged and acknowledge
    current commit;
24. a matching snapshot point only fast-forwards commit;
25. a snapshot excluding the local ID is ignored;
26. invalid `ConfState` fails before log or tracker mutation;
27. a valid configuration combined with an unrepresentable snapshot index
    fails without changing log, tracker configuration/progress, votes,
    `IsLearner`, or queues;
28. null snapshot, null metadata, null `ConfState`, and absent scalar fields
    are normalized defensively without mutating input presence;
29. voter-to-learner and learner-to-voter restores update `IsLearner`;
30. incoming/outgoing joint voters restore equivalently;
31. candidates and pre-candidates become followers before restoring;
32. equal-term leaders ignore competing snapshot payloads;
33. higher-term leaders become followers and may restore.

### Interaction

34. a compacted leader snapshots a slow voter, the follower persists the
    snapshot, acknowledges it, and resumes append replication;
35. the same path works for a learner;
36. the D14-D15 election, replication, and flow-control scenarios remain
    green.

## Design review resolution

The GPT-5.6 Sol design review found five gaps:

1. snapshot responses were gated only on snapshot persistence rather than the
   entire captured durable batch;
2. restored progress incorrectly specified `Next = snapshot index + 1`
   instead of D10's `Next = snapshot index`;
3. the snapshot-abort test did not distinguish the retained boundary from
   `PendingSnapshot`;
4. inbound null/default normalization lacked integration coverage; and
5. restore atomicity was tested only for configuration validation failure.

All findings are accepted. The durability gate now includes `HardState`,
entries, and snapshot state without waiting for slow physical application;
restored progress matches D10; and the test plan adds retained-boundary,
normalization, restart, and post-validation failure cases.

## Development sequence

D16 uses strict test-driven development:

1. review and accept this design;
2. add D16 tests and update D13-D15 snapshot expectations first;
3. record the focused red state;
4. implement only snapshot send, receive, restore, and reporting;
5. preserve D17-D21 host and protocol boundaries;
6. run formatting and the full suite;
7. resolve substantiated code-review findings before commit.

## Completion criteria

D16 is complete when:

- this design is reviewed and accepted;
- compacted peers recover through persisted snapshots;
- progress pause/report/abort behavior matches the pinned reference;
- restore preserves log, configuration, membership, and ownership safety;
- response durability is explicit and tested;
- no live membership, read, lease, transfer, or `Ready` behavior is
  implemented early;
- formatting, build, and the full suite pass;
- code review has no unresolved substantiated findings; and
- the reviewed changes are committed before D17 starts.
