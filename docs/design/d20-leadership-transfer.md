# D20 Leadership Transfer Design

- **Status:** Accepted
- **Date:** 2026-10-02
- **DAG node:** D20
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D20 completes the core Raft semantics by adding deliberate leadership
transfer. A leader can select an eligible transferee, catch it up to the
leader's last log index, send `MsgTimeoutNow`, and let the transferee run the
forced real-election campaign introduced by D19.

The transfer remains advisory and bounded. It can be replaced, cancelled by
membership or role changes, or abandoned after one election timeout. Once a
transferee starts its higher-term forced campaign, the old leader can step
down on the vote request even if the campaign is later rejected and no new
leader wins.

## Goals

- Forward transfer requests from followers to their known leader.
- Ignore nonexistent and learner targets.
- Transfer immediately to an up-to-date voter.
- Catch up a slow voter before sending `MsgTimeoutNow`.
- Resume transfer after snapshot-based catch-up.
- Bypass `CheckQuorum` lease suppression with the pinned transfer context.
- Bound one transfer attempt by one election timeout.
- Reject proposals while transfer is pending.
- Preserve automatic joint-exit retry behavior during transfer.
- Cancel transfer on role reset, target removal, or completed demotion.
- Support replacing or cancelling an existing transfer request.

## Non-goals

- A public `RawNode.TransferLeader` method; D21 exposes the core message.
- Transport retries, delivery guarantees, or transfer completion callbacks.
- Guaranteeing that the transferee wins after `MsgTimeoutNow`.
- Selecting a transferee automatically.
- Moving application state or storage outside normal Raft replication.

## Transfer request shape

The pinned protocol encodes the desired transferee in:

```text
Type = MsgTransferLeader
From = transferee ID
```

`From` therefore identifies the requested future leader, not necessarily the
physical sender of the message. D21 will construct this shape for local API
calls.

The caller-owned message is never mutated. Forwarding and outbound messages
use owned clones through the existing `Send` path.

## Role dispatch

Normal `Step` term processing precedes the role-specific rules below. A
higher-term message can therefore reset a leader, candidate, or pre-candidate
to follower before the same message is dispatched. Lower-term messages remain
subject to the existing stale-message rules.

### Follower

When `LeaderId == 0`, a follower drops `MsgTransferLeader`.

Otherwise it forwards an owned copy to `LeaderId`, preserving the transferee
in `From`. `Send` attaches the follower's current term. This matches proposal
and read-index forwarding while retaining the transfer target.

### Candidate and pre-candidate

Candidates and pre-candidates ignore same-term or local
`MsgTransferLeader` and `MsgTimeoutNow`.

A higher-term message first makes the recipient a follower. A higher-term
`MsgTimeoutNow` is then handled by that follower and may start a transfer
campaign. A higher-term `MsgTransferLeader` similarly resets the recipient,
but the new follower has no known leader and drops the request.

### Leader

A leader handles `MsgTransferLeader`. It ignores a same-term or local
`MsgTimeoutNow`. A higher-term `MsgTimeoutNow` first resets it to follower and
can then start a transfer campaign.

### TimeoutNow on a follower

A follower receiving `MsgTimeoutNow` runs the same campaign-eligibility gates
as local `MsgHup`:

```text
Role is not leader
Promotable
no committed unapplied configuration change
```

When eligible, it starts:

```csharp
Campaign(CampaignType.Transfer)
```

Transfer campaigns always use real votes, even when `PreVote` is configured.
Remote vote requests carry the exact `CampaignTransfer` context, so D19's
leader-lease filter cannot suppress them.

Learners, removed nodes, nodes restoring an unstable snapshot, and nodes with
an unapplied committed configuration change ignore `MsgTimeoutNow`.

## Starting a transfer

The leader first resolves progress for `message.From`.

The request is ignored when:

```text
no progress exists for the transferee
progress.IsLearner
```

A node in `LearnersNext` remains an outgoing voter with non-learner progress
and is therefore still eligible until the joint configuration is left.

### Existing transfer

Let:

```text
requested = message.From
current   = LeaderTransferee
```

If `current == requested`, the duplicate request is ignored. In particular it
does not reset `ElectionElapsed`, so repeated requests cannot extend the
timeout indefinitely.

If `current != 0 && current != requested`, the current transfer is aborted
before considering the new request.

If `requested == Id`, the request is then treated as a no-op. This means a
request to transfer back to the current leader cancels any pending transfer.

Unknown or learner requests are rejected before this replacement logic, so
they do not cancel a valid pending transfer.

### Installing the target

For a new eligible remote target:

```text
ElectionElapsed   = 0
LeaderTransferee  = requested
```

No term, role, vote, heartbeat clock, progress, or log state changes.

## Up-to-date target

When:

```text
targetProgress.Match == Log.LastIndex
```

the leader sends:

```text
Type = MsgTimeoutNow
To   = LeaderTransferee
Term = current leader term
```

The leader remains leader with the transfer pending until a role transition or
timeout clears it. Delivery of `MsgTimeoutNow` does not itself change the old
leader, but the transferee's ensuing higher-term vote request normally does.

## Catching up a target

When the target is behind, the leader invokes the normal append path with:

```text
MaybeSendAppend(
    target,
    targetProgress,
    sendIfEmpty: true)
```

This preserves D15/D16 flow control, rejection hints, inflight limits, and
snapshot fallback. No transfer-specific replication format is introduced.

After an accepted `MsgAppResp` advances the transferee and, at that decision
point:

```text
message.From == LeaderTransferee
&& targetProgress.Match == Log.LastIndex
```

the leader sends `MsgTimeoutNow`.

This check runs after normal progress transitions, commitment calculation,
append draining, and snapshot recovery. A follower that catches up through a
snapshot therefore receives `MsgTimeoutNow` only after its post-snapshot
append acknowledgement advances progress to the leader's retained last index.

Heartbeat responses continue to drive ordinary append retries. They do not
send `MsgTimeoutNow` directly; the eventual append acknowledgement does.

This is an emission-time check, not a delivery-time guarantee. Once queued,
`MsgTimeoutNow` is not revalidated against a later `Log.LastIndex`. The leader
may time out the transfer and append new entries before delayed delivery,
leaving the old transferee behind when it receives the message. Its forced
campaign still remains subject to ordinary log-freshness voting.

## Transfer timeout

D19 established the leader tick order:

1. process an election-timeout `MsgCheckQuorum` when enabled;
2. expire leadership transfer if the node remains leader;
3. re-read role; and
4. emit a due heartbeat only while still leader.

D20 fills step 2:

```text
ElectionDue
&& Role == Leader
&& LeaderTransferee != 0
    => LeaderTransferee = 0
```

Transfer initiation resets `ElectionElapsed`, so the attempt receives one full
fixed `ElectionTick` window of local transfer state and proposal suppression.
Heartbeat ticks do not extend it.

If `CheckQuorum` steps the node down on the same tick, the normal role reset
already clears the transferee. No additional transfer timeout action runs.

After timeout, the node remains leader and may accept proposals or start a new
transfer.

Clearing `LeaderTransferee` cannot retract a `MsgTimeoutNow` that was already
queued or handed to transport. Delayed delivery may therefore start a target
election after self-cancellation, replacement, timeout, membership
cancellation, or role reset. This is normal asynchronous-message behavior and
matches the pinned implementation.

## Proposal handling

After the existing nonempty-entry and local-progress checks, while:

```text
Role == Leader
&& LeaderTransferee != 0
```

a valid proposal throws `ProposalDroppedException` before:

- configuration-entry parsing or rewriting;
- uncommitted-size accounting;
- pending-configuration index changes;
- log append; or
- replication output.

Follower forwarding remains unchanged. The leader is the component that drops
the forwarded proposal.

An empty proposal still raises the existing invariant failure. A leader that
has no local progress still raises the existing proposal-drop reason before
the transfer check.

## Automatic joint exit

D17's automatic joint-exit proposal also passes through `HandleProposal`.
During transfer it is intentionally dropped.

`MaybeAutoLeave` catches only `ProposalDroppedException` from this internal
proposal and leaves membership, pending index, log, and queues unchanged. The
application cursor remains advanced.

Every later host `AppliedTo` acknowledgement re-evaluates auto-leave through
the pinned core function. Normal D21 `Ready`/`Advance` use supplies that
fallback.

ADR 0009 adds one narrow liveness deviation: when the transfer reaches its
election timeout and the node remains leader, `TickLeader` clears the transfer
target and immediately re-evaluates auto-leave. It uses the same proposal path
and pending-index guard. Therefore:

- if transfer succeeds, the new leader can propose the exit; or
- if transfer times out, the current leader retries in that timeout tick
  without waiting for unrelated application work.

Malformed protobuf, storage, and invariant failures are not swallowed.

## Cancellation

`LeaderTransferee` is cleared by:

- any normal `Reset`, including higher-term step-down;
- election-timeout transfer expiration;
- a valid request to transfer back to the current leader;
- replacement by a different eligible remote target;
- D17 membership installation when the target is no longer in either voter
  majority; and
- a transferee campaign indirectly, because its higher-term vote request
  resets the old leader before log-freshness evaluation or election outcome.

The final path does not imply successful transfer. The old leader may step
down, reject the stale transferee's vote, and leave the cluster temporarily
without a leader.

Entering joint demotion keeps a target that remains an outgoing voter in
`LearnersNext`. Leaving joint consensus clears it when the target becomes a
real learner.

The pinned configuration-switch order has one exception. When a configuration
simultaneously leaves the current leader removed or demoted and
`StepDownOnRemoval == false`, configuration handling returns before checking
the transferee. If the same change also removes or fully demotes the target,
the stale `LeaderTransferee` remains until timeout, normal reset, or a valid
remote replacement. A request to transfer back to the retained current leader
cannot cancel it because target progress eligibility is checked first and the
leader no longer has eligible progress. D20 preserves and tests this parity
behavior rather than silently reordering D17.

## CheckQuorum interaction

The transferee's `CampaignType.Transfer` vote requests carry
`CampaignTransfer`, bypassing D19's active-leader lease suppression.

This exception is safe because the current leader explicitly selected the
transferee and sent `MsgTimeoutNow`. Transfer campaigns do not use pre-vote,
avoiding an unnecessary round trip.

The ordinary candidate log-freshness and voter-quorum rules still apply.
`MsgTimeoutNow` does not guarantee election if the target's configuration is
stale or its campaign cannot reach quorum.

## Errors and logging

- Unknown and learner targets are ignored without mutating transfer state.
- Duplicate same-target requests are ignored.
- Valid nonempty proposals from a configured leader during transfer throw
  `ProposalDroppedException`.
- Auto-leave catches only that typed proposal drop.
- Invalid roles and impossible progress states retain explicit invariant
  failures.

Transfer initiation, replacement, timeout, and ignored targets may be logged,
but logging does not affect protocol behavior.

## Test plan

### Initiation and forwarding

- an up-to-date voter receives `MsgTimeoutNow` immediately;
- a follower forwards the request to its known leader without mutating the
  caller;
- forwarding through node 2 to leader 1 while targeting node 3 preserves
  `From == 3`, sets `To == 1`, attaches node 2's term, and remains enabled when
  `DisableProposalForwarding` is true;
- a follower without a leader drops the request;
- candidates and pre-candidates ignore requests;
- self-transfer is a no-op;
- self-transfer cancels a pending remote transfer;
- nonexistent and learner targets are ignored without cancelling an existing
  transfer;
- a `LearnersNext` outgoing voter remains eligible.

### Catch-up and completion

- a slow voter receives append traffic before `MsgTimeoutNow`;
- no timeout message is sent until its `Match` reaches `Log.LastIndex`;
- the decisive append acknowledgement sends `MsgTimeoutNow`;
- after an emitted timeout message is delayed, the transfer can expire and the
  leader can append a newer proposal; delivering the old message performs no
  freshness revalidation, the target's higher-term request steps down the old
  leader, and ordinary votes reject the stale target without electing it;
- snapshot catch-up resumes through the ordinary D16 path and then transfers;
- withholding the durable post-snapshot `MsgAppResp` emits no `MsgTimeoutNow`;
- heartbeat-driven probe recovery can continue the catch-up;
- an up-to-date transferee can win while `CheckQuorum` leases are active.

### Timeout and replacement

- transfer remains pending before `ElectionTick`;
- it clears exactly at the election timeout;
- heartbeat ticks do not extend it;
- a same-target duplicate does not reset elapsed time;
- a different eligible target replaces the old target and resets the window;
- a transfer-back-to-self request cancels the old target;
- quorum-loss step-down on the same tick clears transfer through reset;
- with quorum active and election and heartbeat clocks due together, transfer
  expires before the due heartbeat is emitted;

### Target campaign

- `MsgTimeoutNow` starts a real transfer campaign even with `PreVote`;
- higher-term `MsgTimeoutNow` first resets a leader, candidate, or
  pre-candidate and is then processed as a follower transfer campaign;
- higher-term `MsgTransferLeader` first resets those roles and is then dropped
  by the follower with no known leader;
- forced vote requests contain `CampaignTransfer`;
- candidate and pre-candidate recipients ignore `MsgTimeoutNow`;
- learners, removed nodes, and snapshot-restoring nodes ignore it;
- a committed unapplied configuration change blocks the target campaign;
- after application catches up, a later transfer request can succeed.

### Proposal and cancellation behavior

- ordinary and forwarded proposals are dropped while transfer is pending;
- rejection is atomic with respect to log, quota, pending configuration, and
  queues;
- an empty proposal still raises `RaftInvariantException` before transfer
  rejection;
- a retained removed leader without local progress takes the existing
  missing-progress proposal-drop path before transfer rejection;
- malformed V1 and V2 configuration bytes during transfer are dropped without
  protobuf decoding or mutation;
- proposals succeed again after timeout or cancellation;
- a higher-term message clears the target through reset;
- removing or fully demoting the target clears it;
- staged joint demotion preserves it until joint exit;
- removing or demoting both a retained leader and its target preserves the
  pinned early-return exception;
- a valid remote target can replace that stale target, while transfer-to-self
  cannot cancel it after local progress is removed or becomes learner
  progress;
- cancellation or timeout does not retract an already emitted
  `MsgTimeoutNow`;
- transfer timeout immediately retries a previously dropped automatic joint
  exit exactly once, without a later committed entry;
- repeated ticks and duplicate current-index application do not append another
  exit.

### Interaction scenarios

- transfer succeeds to an already caught-up voter;
- transfer succeeds after ordinary log catch-up;
- transfer succeeds after snapshot catch-up;
- leadership can transfer back to the former leader;
- a second transfer can replace a pending first transfer;
- a transfer request sent to a follower reaches the current leader.

## Acceptance criteria

D20 is complete when:

1. only eligible non-learner progress can become a transfer target;
2. the leader queues `MsgTimeoutNow` only when its recorded target `Match`
   equals `Log.LastIndex` at that decision point, without delivery-time
   revalidation;
3. the forced real election bypasses leader-lease suppression without
   bypassing quorum or log freshness;
4. transfer replacement, self-cancellation, reset, and membership cancellation
   match the pinned behavior, while timeout adds the ADR 0009 auto-leave retry;
5. proposals and auto-leave behave atomically while transfer is pending;
6. direct, slow-log, snapshot, forwarding, and `CheckQuorum` interaction tests
   pass;
7. all existing tests remain green; and
8. an independent review finds no unresolved correctness issue.
