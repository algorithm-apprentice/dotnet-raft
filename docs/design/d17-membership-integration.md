# D17 Membership Integration Design

- **Status:** Accepted
- **Date:** 2026-09-30
- **DAG node:** D17
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D17 connects the transactional D10 configuration engine to the deterministic
Raft core. Leaders validate configuration proposals before appending them,
committed configuration changes update quorum and progress state atomically,
joint configurations can leave automatically after application, and a leader
reacts consistently when the applied configuration removes or demotes it.

The host still owns committed-entry application. D17 exposes internal core
operations that D21 will later surface through `RawNode.ApplyConfChange` and
`Ready`/`Advance`.

## Goals

- Protect the log from overlapping unapplied configuration changes.
- Preserve both V1 and V2 configuration entry encodings.
- Convert rejected configuration proposals into empty normal entries.
- Allow proposal-time validation to be disabled explicitly.
- Apply simple, joint-entry, and joint-exit changes through D10.
- Update voter, learner, and staged-learner state atomically.
- Recompute the local learner and promotable state.
- Re-evaluate commitment under the new quorum.
- Probe newly added replicas immediately.
- Preserve the reference behavior when a leader removes or demotes itself.
- Automatically propose a joint-exit entry after the joint entry is applied.
- Avoid duplicate automatic joint-exit proposals.

## Non-goals

- Public proposal or application APIs; D21 adds `RawNode`.
- Delivering committed entries or owning application acknowledgement; D21 adds
  `Ready`/`Advance`.
- Transport peer creation or removal.
- Leadership transfer; D20 implements transfer messages and timeout behavior.
- `CheckQuorum`, pre-vote, or lease behavior; D19 owns those features.
- Bootstrap helpers and status reporting; D22 owns them.
- Applying arbitrary configuration entries automatically inside the core. The
  host must still decode and apply each committed configuration entry in log
  order.

## Internal core surface

D17 adds:

```csharp
internal ConfState ApplyConfigurationChange(ConfChange change);

internal ConfState ApplyConfigurationChange(ConfChangeV2 change);

internal void AppliedTo(
    ulong index,
    ulong encodedSize);
```

The V1 overload converts through the existing `AsV2` helper. Both application
overloads own neither caller protobuf and do not mutate it.

`AppliedTo` advances the existing Raft-log application cursor, then checks
whether an automatic transition out of joint consensus is due. D21 will call
this method after the host confirms ordered application.

## Proposal-time configuration protection

### Owned preprocessing

`HandleProposal` clones the caller's entries before inspecting or rewriting
them. The caller's message, entries, and serialized configuration payloads
remain unchanged.

The preprocessing pass returns:

```text
rewritten owned entries
candidate pending-configuration index
```

The live `PendingConfigurationIndex` changes only after the full proposal is
accepted by `AppendLeaderEntries`. Parse errors, index overflow, and quota
rejection leave both the log and pending index unchanged.

### Decoding

For each proposed entry:

```text
EntryConfChange
    => parse Data as ConfChange, then convert with AsV2

EntryConfChangeV2
    => parse Data as ConfChangeV2

other entry type
    => no membership validation
```

Malformed protobuf data propagates `InvalidProtocolBufferException`. It is a
caller/protocol error and must not be converted into an ordinary no-op.

The original entry encoding remains unchanged when accepted. V1 entries stay
V1 and V2 entries stay V2.

### Pending-change check

For each decoded configuration entry, evaluated in proposal order:

```text
alreadyPending =
    candidatePendingConfigurationIndex > Log.Applied

alreadyJoint =
    Tracker.Config.Voters.Outgoing.Count > 0

wantsLeaveJoint =
    decodedV2.Changes.Count == 0
```

`wantsLeaveJoint` intentionally follows the pinned proposal guard rather than
`ConfChangeV2.IsLeaveJoint`. Proposal validation only distinguishes empty from
nonempty change lists. The D10 classifier remains authoritative when the
committed change is actually applied.

The proposal is incompatible when:

1. another configuration entry is still unapplied;
2. the core is joint and the new change list is nonempty; or
3. the core is not joint and the new change list is empty.

When validation is enabled, an incompatible configuration entry becomes:

```text
Type = EntryNormal
Data = empty
```

All other caller-supplied entry fields are discarded before the leader assigns
the current term and final index. The normal no-op remains in the log so the
proposal retains its position and ordering.

When validation is disabled, every decoded configuration entry is accepted
regardless of these checks.

### Pending index update

Each accepted configuration entry updates the candidate pending index to its
future log index:

```text
Log.LastIndex + entry offset + 1
```

With validation enabled, the first accepted entry in a proposal makes later
configuration entries in the same proposal pending and therefore rewrites
them to normal no-ops. With validation disabled, multiple configuration
entries may be accepted and the last one becomes the pending index.

Applying an entry does not clear the field. The protection naturally expires
when:

```text
Log.Applied >= PendingConfigurationIndex
```

Leader activation keeps the existing conservative behavior of setting the
pending index to the pre-no-op last index.

## Applying a committed configuration change

`ApplyConfigurationChange(ConfChangeV2)` creates a
`ConfigurationChanger` with:

```text
Tracker   = live tracker
LastIndex = Log.LastIndex
```

It selects exactly one D10 operation:

1. `IsLeaveJoint()` -> `LeaveJoint()`;
2. `TryGetJointTransition(out autoLeave)` ->
   `EnterJoint(autoLeave, Changes)`; or
3. otherwise -> `Simple(Changes)`.

D10 validates and computes a fresh `TrackerConfig` and `ProgressMap` without
mutating the live tracker. Any classification or D10
`ConfigurationChangeException` before installation leaves the live tracker,
learner state, role, progress, and outbound queues unchanged.

After a successful result, the core installs configuration and progress
together. That installation is the application commit point. Leader-only
follow-up work such as commitment recomputation, append broadcasting, and
probing occurs after the membership operation is already applied. A storage,
sizing, or replication failure from that follow-up work propagates without
rolling membership back, and the caller must not retry the configuration
application. This matches the pinned non-transactional post-install behavior
and avoids pretending that commit state, message queues, and progress side
effects can be rolled back safely.

On success, the method returns a newly allocated `ConfState`.

## Switching live configuration

After installing the D10 result, the core:

1. recomputes `IsLearner`, using `false` when the local ID is absent;
2. handles local leader removal or demotion;
3. if still leader, re-evaluates commitment using the new quorum;
4. broadcasts append state if commitment advanced;
5. otherwise attempts a nonempty append to each remote progress so newly
   added replicas are probed immediately; and
6. clears `LeaderTransferee` if that node is no longer a voter.

Existing progress objects preserve their match, next, activity, snapshot, and
inflight state through D10 cloning. A newly added node uses D10's pinned
initialization:

```text
Match        = 0
Next         = max(Log.LastIndex, 1)
State        = Probe
RecentActive = true
```

The immediate probe uses:

```text
MaybeSendAppend(peer, sendIfEmpty: false)
```

No empty append is emitted solely because membership changed.

### Commitment under the new quorum

Changing membership may lower the quorum required for an entry that is already
replicated. A leader therefore calls `MaybeCommit` immediately after installing
the new tracker.

If commitment advances, `BroadcastAppend` communicates the new commit index to
the updated configuration. The removed local leader is not counted because its
progress no longer belongs to the tracker.

### Local leader removal or demotion

When the local node is absent from progress or is now a learner:

```text
IsLearner = progress exists && progress.IsLearner
```

If it is currently leader:

- `StepDownOnRemoval == false`
  - remain leader;
  - return without probing or recomputing commitment in this method;
  - if removed, future proposals are rejected because local progress is
    absent;
  - if demoted to learner, local progress remains and proposals continue to be
    accepted, matching the pinned implementation; and
  - either state is unpromotable after leadership is later lost.
- `StepDownOnRemoval == true`
  - become follower in the same term with no known leader;
  - return without further leader-only work.

This preserves the pinned optional behavior. D20 may later add an optimistic
handover before stepping down.

During joint demotion, an outgoing voter remains in `LearnersNext` and its
progress is not yet marked as learner. The local leader therefore remains a
voter and does not react as demoted until the joint-exit change promotes it to
the active learner set.

Followers and candidates simply install the configuration and update
`IsLearner`.

## Automatic joint exit

An implicit or auto multi-change transition installs:

```text
Tracker.Config.AutoLeave = true
```

Automatic exit is driven by application, not commitment. After `AppliedTo`
advances the log cursor, the core checks:

```text
Tracker.Config.AutoLeave
&& Log.Applied >= PendingConfigurationIndex
&& Role == Leader
```

When true, the core proposes exactly one empty V2 configuration change:

```text
Entry.Type = EntryConfChangeV2
Entry.Data = serialized empty ConfChangeV2
```

The empty payload is owned by the core and has zero configuration context. The
ordinary proposal path validates it as a joint exit and updates
`PendingConfigurationIndex` to the new entry. That new pending index prevents
subsequent `AppliedTo` calls from proposing duplicate exits.

The proposal has zero protobuf payload and therefore cannot be rejected by the
uncommitted-payload quota. In D17 there is no leadership-transfer rejection
path. D20 will preserve the reference retry behavior when transfer is active.

A follower never auto-proposes. If leadership later changes while the joint
configuration remains active, applying a later entry as the new leader
re-evaluates the condition and proposes the exit.

The host must call `ApplyConfigurationChange` for a committed configuration
entry before acknowledging application through `AppliedTo` at or beyond that
entry. D21 will enforce and document this ordering through `Ready`/`Advance`;
D17 tests exercise the internal sequence directly.

## Ownership and failure atomicity

- Proposal preprocessing mutates only owned entry clones.
- Accepted configuration entries preserve their serialized bytes.
- Rejected configuration entries become newly allocated normal entries.
- Applying a change never mutates the caller's `ConfChange` or `ConfChangeV2`.
- D10 computes configuration and progress candidates transactionally.
- The live tracker is installed only after successful D10 validation.
- Proposal parse, index, or quota failure leaves
  `PendingConfigurationIndex` unchanged.
- A classification or D10 validation failure before tracker installation
  leaves role, learner state, tracker, progress, commit state, and message
  queues unchanged.
- Tracker installation is the application commit point. Failures from
  subsequent commit recomputation or replication are surfaced without
  rollback and must not cause the membership operation to be retried.

## Interaction with snapshots

D16 snapshot restore already installs configuration and progress atomically.
D17 does not route snapshot state through `ApplyConfigurationChange`.

A snapshot may restore `AutoLeave == true`. Snapshot application normally
occurs on a follower, so it does not auto-propose. If that node later becomes
leader, the next ordered application advancement triggers the same automatic
exit check.

## Test plan

### Proposal protection

- accepted V1 and V2 entries preserve type and serialized bytes;
- the caller's proposal remains unchanged;
- a second unapplied configuration entry becomes an empty normal entry;
- only the first configuration entry in one proposal is accepted;
- a nonempty change while joint becomes a normal entry;
- an empty V1 entry outside joint is accepted because `AsV2` contains one
  zero-ID change, while the same entry in joint state becomes an empty normal
  entry without changing the pending index;
- empty V2 `Auto`, `JointImplicit`, and `JointExplicit` entries are tested in
  both simple and joint configurations, proving proposal validation uses only
  `Changes.Count`;
- accepted explicit-empty V2 entries in a joint configuration fail
  atomically during D10 application because they classify as joint entry, not
  joint exit;
- validation-disabled mode accepts overlapping and otherwise incompatible
  entries;
- applying through the pending index permits the next configuration proposal;
- malformed V1 and V2 payloads fail without changing log or pending state;
- malformed V1 and V2 payloads still fail when proposal validation is
  disabled;
- quota and index failure do not publish a candidate pending index.

### Configuration application

- V1 add voter;
- V2 simple add learner and learner promotion;
- local learner demotion and promotion;
- remove voter and learner;
- explicit joint entry and manual exit;
- implicit joint entry with `LearnersNext`;
- invalid changes preserve all live state;
- newly added progress is probed immediately;
- a reduced quorum can commit an already replicated entry;
- removing a transfer target clears `LeaderTransferee`.

### Leader removal

- default mode leaves a removed leader active but unable to accept proposals;
- default mode leaves a demoted leader active and still accepting proposals
  while local learner progress remains, matching the pinned behavior;
- `StepDownOnRemoval` makes a removed leader a follower in the same term;
- `StepDownOnRemoval` makes an actively demoted leader a follower in the same
  term;
- `LearnersNext` staging does not demote or step down the leader until joint
  exit;
- removed or learner nodes are not promotable and cannot campaign.

### Automatic exit

- applying an implicit joint entry proposes one empty V2 exit;
- the exit bytes decode as an empty `ConfChangeV2`;
- the automatic proposal updates the pending index;
- when normal entries already follow the joint entry, the automatic exit is
  appended after that tail and becomes the exact pending index;
- advancing application through each intervening normal entry does not append
  duplicate exits;
- followers do not auto-propose;
- a later leader triggers exit after a subsequent application advance.
- restoring an `AutoLeave == true` snapshot as a follower emits no exit, but a
  later leader no-op application emits exactly one;
- restoring an otherwise equivalent `AutoLeave == false` snapshot never emits
  an automatic exit.

### Transfer-target eligibility

- removing `LeaderTransferee` clears it;
- staging the transferee in `LearnersNext` keeps it while it remains an
  outgoing voter; and
- leaving joint consensus clears it when the transferee becomes an active
  learner.

### Interaction scenarios

- a V1 single-voter addition converges and immediately starts replication;
- a V2 multi-change enters joint consensus, auto-proposes exit, and reaches the
  final voter/learner configuration;
- removing the leader uses the updated quorum and never counts removed local
  progress toward later commitment.

## Acceptance criteria

D17 is complete when:

1. V1 and V2 proposal guards match the pinned behavior;
2. D10 results install atomically through the core;
3. learner, joint, and leader-removal transitions preserve tracker invariants;
4. automatic joint exit is application-driven and emitted exactly once;
5. focused membership and interaction tests pass;
6. the full solution and formatting checks pass; and
7. an independent GPT-5.6 Sol review finds no significant correctness issue.
