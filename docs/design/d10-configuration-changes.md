# D10 Configuration Changes Design

- **Status:** Accepted
- **Date:** 2026-09-30
- **DAG node:** D10
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D10 implements membership transitions over D09 tracker state:

- simple one-voter changes;
- entry into and exit from joint consensus;
- voter promotion and learner demotion;
- staged learners that remain outgoing voters;
- reconstruction from a persisted protobuf `ConfState`; and
- protocol classification of V1 and V2 configuration changes.

Every operation is transactional. It returns a validated candidate
configuration and progress map without mutating the live tracker. The Raft core
installs the pair only after the operation succeeds.

## Goals

- Preserve the pinned `confchange` behavior and safety checks.
- Keep voter and learner roles consistent with D08 progress flags.
- Preserve existing progress, inflight, activity, and replication state across
  membership changes.
- Initialize new nodes with the pinned optimistic next-index rule.
- Support joint-consensus demotion through `LearnersNext`.
- Restore all valid simple and joint `ConfState` combinations.
- Reject invalid changes with explicit typed errors.
- Prove simple and joint paths converge through deterministic property tests.
- Preserve failure atomicity for configuration and progress state.

## Non-goals

- Appending or applying configuration-change log entries; D14 and D17 own the
  Raft-core integration.
- Deciding when a V2 change should be proposed or auto-left; later core logic
  uses the protocol classification supplied here.
- Updating transport peers or application state.
- Public proposal APIs or configuration-change serialization.
- Concurrent mutation. One serialized Raft event loop owns the live tracker.

## Namespace and result types

```csharp
namespace DotnetRaft.ConfChange;

internal sealed class ConfigurationChangeException
    : InvalidOperationException;

internal sealed record ConfigurationChangeResult(
    TrackerConfig Config,
    ProgressMap Progress);
```

`ConfigurationChangeException` represents an invalid requested transition or
an inconsistent tracker/configuration pair. It is not used for programmer
argument errors such as null inputs.

The result owns both candidate objects. Callers must install `Config` and
`Progress` together.

## Progress cloning

D10 adds:

```csharp
internal Progress Progress.Clone();
internal ProgressMap ProgressMap.Clone();
```

`Progress.Clone` copies:

- `Match`, `Next`, `LastSentCommit`, and `State`;
- pending snapshot and append-flow pause state;
- recent activity and learner role; and
- an independent D07 inflight window.

`ProgressMap.Clone` clones every progress under the same node ID.

The pinned Go changer copies progress structs while sharing their inflight
pointer because it mutates only `IsLearner`. C# progress objects are reference
types, so sharing them would let a failed candidate mutate live learner flags.
A complete deep clone is simpler, makes the transactional boundary explicit,
and preserves all observable state.

## Changer API

```csharp
internal sealed class ConfigurationChanger
{
    internal ConfigurationChanger(
        ProgressTracker tracker,
        ulong lastIndex);

    internal ConfigurationChangeResult Simple(
        IEnumerable<ConfChangeSingle> changes);

    internal ConfigurationChangeResult EnterJoint(
        bool autoLeave,
        IEnumerable<ConfChangeSingle> changes);

    internal ConfigurationChangeResult LeaveJoint();
}
```

The changer references the current tracker but never mutates it. `lastIndex`
is the leader log index used only when a change creates a new progress.

Each operation:

1. validates the current live `TrackerConfig`/`ProgressMap` pair;
2. independently clones both;
3. applies changes to the clones;
4. validates the candidate pair; and
5. returns it.

Any failure throws before a candidate can be installed. The live tracker,
including every progress and inflight window, remains unchanged.

Change enumerables and their elements must be non-null. They are materialized
once before processing so caller-side collection mutation cannot affect an
in-progress operation.

## Configuration invariants

Validation permits the empty bootstrap configuration but otherwise enforces:

1. every incoming voter, outgoing voter, learner, and staged learner has a
   progress object;
2. every `LearnersNext` ID belongs to the outgoing voter majority;
3. a staged learner progress is not yet marked `IsLearner`;
4. active learners intersect neither voter majority;
5. every active learner progress is marked `IsLearner`;
6. every incoming or outgoing voter progress is not marked `IsLearner`; and
7. when the outgoing majority is empty, `LearnersNext` is empty and
   `AutoLeave` is false.

Extra progress entries are allowed. This matches the pinned validator and
lets a transaction temporarily retain nodes while roles are being adjusted.

Validation errors identify the offending node or invalid state through
`ConfigurationChangeException`.

## Applying individual changes

Changes are processed in order.

### Node ID zero

`NodeId == 0` is ignored for every change type. Downstream systems use zero to
represent a configuration entry that should not take effect.

### Add voter

For an existing progress:

- clear `IsLearner`;
- remove the ID from `Learners` and `LearnersNext`; and
- add it to the incoming voter majority.

For a new progress, initialize it as a voter.

### Add learner

For a new progress, initialize it directly as an active learner.

For an existing active learner, do nothing.

For an existing voter:

1. remove it from the incoming voter majority and learner sets;
2. retain its existing progress state;
3. if it remains in the outgoing voter majority, add it to `LearnersNext`
   without setting `IsLearner`; otherwise
4. add it to `Learners` and set `IsLearner = true`.

### Remove node

Remove the ID from the incoming voters, active learners, and staged learners.

If it remains in the outgoing voter majority, retain its progress because it
still participates in joint consensus. Otherwise remove its progress.

Removing an absent node is idempotent.

### Update node

`ConfChangeUpdateNode` is a membership no-op. External systems may use it to
carry context handled outside the Raft library.

### Unknown type

An unknown `ConfChangeType` throws `ConfigurationChangeException`.

## New progress initialization

A new node receives:

```text
Match = 0
Next = max(lastIndex, 1)
State = Probe
RecentActive = true
IsLearner = requested role
Inflights = empty tracker-configured D07 window
```

The `Next` rule intentionally matches the pinned implementation rather than
using `lastIndex + 1`. The first append probes the supplied last index.

Existing progress objects are cloned and retained across promotion, demotion,
joint entry, and joint exit. Membership operations do not reset replication
state.

## Simple changes

`Simple` is valid only outside joint consensus.

After applying all requested changes:

- the incoming voter majority must be non-empty; and
- its symmetric difference from the original incoming majority must contain at
  most one voter.

Learner-only changes do not count toward this voter difference. A batch may
contain multiple operations on the same voter when their net voter-set change
is at most one.

Errors:

```text
can't apply simple config change in joint config
removed all voters
more than one voter changed without entering joint config
```

## Entering joint consensus

`EnterJoint` requires:

- no existing outgoing voter majority; and
- at least one incoming voter before the requested changes.

It copies the current incoming voter majority into outgoing, applies every
change to incoming roles, and sets `AutoLeave` to the requested value.

The incoming voter majority must remain non-empty after the changes.

Entering with no changes is valid and produces:

```text
incoming == outgoing
```

Errors:

```text
config is already joint
can't make a zero-voter config joint
removed all voters
```

## Leaving joint consensus

`LeaveJoint` requires a non-empty outgoing voter majority.

It:

1. promotes every `LearnersNext` ID to `Learners`;
2. marks each corresponding progress as a learner;
3. clears `LearnersNext`;
4. removes progress objects that exist only in outgoing and are neither
   incoming voters nor active learners;
5. clears outgoing voters; and
6. clears `AutoLeave`.

Existing incoming voters and learners retain their complete progress state.

Calling this operation outside joint consensus throws
`ConfigurationChangeException` with:

```text
can't leave a non-joint config
```

## Protocol classification

D10 adds internal helpers in `DotnetRaft.Protocol`:

```csharp
internal static ConfChangeV2 AsV2(this ConfChange change);

internal static bool IsLeaveJoint(this ConfChangeV2 change);

internal static bool TryGetJointTransition(
    this ConfChangeV2 change,
    out bool autoLeave);
```

### V1 conversion

`AsV2` creates one `ConfChangeSingle` with the V1 type and node ID and preserves
the immutable protobuf context.

### Leave classification

A V2 change leaves joint consensus only when:

```text
Transition == Auto
Changes.Count == 0
```

Context is ignored. Explicit implicit/explicit joint transitions with no
changes do not leave joint consensus.

### Enter classification

Joint consensus is used when:

```text
Transition != Auto
    || Changes.Count > 1
```

Auto-leave is:

| Transition | Auto-leave |
|---|---|
| `Auto` with multiple changes | `true` |
| `JointImplicit` | `true` |
| `JointExplicit` | `false` |

An auto transition with zero or one change is simple and returns `false` from
`TryGetJointTransition`. Unknown enum values throw
`ConfigurationChangeException`.

The core checks `IsLeaveJoint` before `TryGetJointTransition`, because an empty
auto change is classified as leaving rather than as a simple no-op.

## Restoring `ConfState`

```csharp
internal static class ConfigurationRestore
{
    internal static ConfigurationChangeResult Restore(
        ConfigurationChanger changer,
        ConfState state);
}
```

Restore requires the changer's tracker to have empty configuration and progress
state. It clones and normalizes the input, including absent proto2
`AutoLeave == false`, without mutating the caller's protobuf.

Restore creates a private scratch `ProgressTracker` with the same inflight
limits. Each intermediate result is installed only into that scratch tracker,
and the next changer reads from the scratch state. The caller-supplied tracker
therefore remains empty on success and on any late failure. Even an empty
restore returns fresh configuration and progress objects rather than aliasing
the supplied tracker.

The state is translated into ordered single changes.

### Non-joint state

When `VotersOutgoing` is empty:

1. add each incoming voter;
2. add each active learner; and
3. add each staged learner.

Each change is applied through a separate `Simple` transaction so adding
multiple voters never violates the one-voter simple-change rule.

Valid non-joint states have no staged learners; invariant validation rejects
otherwise.

### Joint state

When outgoing voters exist:

1. add each outgoing voter through separate `Simple` transactions;
2. enter joint consensus once;
3. during joint entry, remove all outgoing voters from incoming;
4. add the requested incoming voters;
5. add active learners; and
6. add staged learners, which remain outgoing voters until joint exit.

The joint operation uses the persisted `AutoLeave` flag.

An empty `ConfState` restores to the allowed empty bootstrap tracker.

Restore returns the same normalized membership as the input for every valid
state. New progress objects use the changer's `lastIndex` and tracker inflight
limits.

Before returning, restore compares the scratch result's `ToConfState()` with
the cloned, presence-normalized input through
`ProtocolDefaults.IsEquivalentTo`. A mismatch throws
`ConfigurationChangeException` instead of silently normalizing corrupted
persisted membership.

This rejects malformed source states including:

- `LearnersNext` or `AutoLeave` without outgoing voters;
- voter/learner role overlap;
- staged learners absent from outgoing voters;
- duplicate IDs;
- zero IDs; and
- any other source state whose ordered operations do not reproduce the same
  normalized `ConfState`.

## Failure and ownership guarantees

- No operation mutates the input protobuf changes.
- No operation mutates the live tracker on success or failure.
- Returned configuration, progress objects, and inflight windows share no
  mutable state with the live tracker.
- Installing a successful result transfers ownership to the Raft core.
- Invalid current tracker state fails before requested changes are applied.
- Invalid candidate state fails before it can be installed.

## Test plan

### Protocol helpers

1. V1-to-V2 conversion preserves type, node ID, and context.
2. The full pinned leave-joint table covers absent/explicit auto transition,
   context, non-empty changes, explicit joint transitions, and multiple
   changes.
3. Enter-joint classification covers simple, automatic multi-change,
   implicit, explicit, and unknown transitions.

### Data-driven behavior

Port the complete pinned `confchange/testdata` corpus:

- `simple_safety.txt`
- `joint_safety.txt`
- `joint_autoleave.txt`
- `update.txt`
- `joint_learners_next.txt`
- `simple_promote_demote.txt`
- `simple_idempotency.txt`
- `joint_idempotency.txt`
- `zero.txt`

A small test-only parser executes the same `simple`, `enter-joint`, and
`leave-joint` commands, increments `lastIndex` after every command, and compares
the exact configuration/error and sorted progress-map output.

### Transaction and invariants

4. failed operations leave the live config, progress flags, progress cursors,
   and inflights unchanged;
5. successful results are independent of live tracker state;
6. promotion and demotion preserve all progress state;
7. outgoing-only progress survives joint entry and is removed on exit;
8. staged learners become active learners only on joint exit;
9. every listed configuration/progress invariant has a rejecting test,
   including incoming and outgoing voters incorrectly marked as learners;
10. null collections/elements and unknown change types fail explicitly.

### Restore and properties

11. restore the upstream empty, simple-voter, voter-plus-learner, and joint
    unit cases;
12. restore does not mutate its input and emits sorted, presence-normalized
    equivalent `ConfState`;
13. restore uses isolated scratch state: the supplied tracker remains empty
    after success and after a late failure, and a non-empty starting tracker is
    rejected;
14. malformed source states covering non-joint `LearnersNext`/`AutoLeave`,
    role overlap, duplicates, zero IDs, and staged learners absent from
    outgoing voters are rejected rather than normalized;
15. deterministic random valid `ConfState` generation restores 1,000 states;
16. deterministic random change sequences compare sequential simple changes
    with enter-joint-plus-leave for 1,000 cases;
17. auto-leave changes only the flag and not the candidate membership or
    progress state.

## Development sequence

D10 uses strict test-driven development:

1. review and accept this design;
2. add protocol, data-driven, transaction, restore, and property tests before
   production code;
3. run the focused target and record the expected compile failure;
4. implement the minimum transactional change engine that passes;
5. refactor while preserving the complete matrix;
6. run formatting and the full suite before code review.

## Completion criteria

D10 is complete when:

- this design is reviewed and accepted;
- simple, joint, learner, restore, and protocol-classification behavior is
  implemented;
- the complete translated data-driven corpus passes;
- deterministic property and restore tests pass;
- formatting, build, and full tests pass;
- code review has no unresolved substantiated findings;
- the reviewed changes are committed before D11 starts.
