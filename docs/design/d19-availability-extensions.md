# D19 Availability Extensions Design

- **Status:** Accepted
- **Date:** 2026-10-01
- **DAG node:** D19
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D19 adds the availability-oriented Raft extensions that depend on the
completed election, progress-tracking, and read-index paths:

- `CheckQuorum` makes an isolated leader step down after one election-timeout
  activity window;
- leader-lease vote suppression avoids disruptive elections while a follower
  has heard from a leader recently;
- `PreVote` probes whether an election can win before incrementing the term;
- `MsgForgetLeader` lets a safe-read follower intentionally stop honoring the
  recent-leader condition; and
- lease-based `ReadIndex` answers without a heartbeat round trip when its
  timing assumptions hold.

These features improve availability but do not change the library boundary.
Ticks and messages remain deterministic inputs, and the host still owns
transport, persistence, and application of committed entries.

## Goals

- Step down a leader that cannot prove recent voter-quorum activity.
- Preserve healthy leaders by resetting remote activity once per timeout
  window.
- Suppress unforced higher-term vote and pre-vote requests during an active
  leader lease.
- Let lower-term leaders discover a peer's higher term through append
  responses.
- Run pre-election and real-election phases with the correct term and
  durability ordering.
- Keep pre-vote requests from mutating persistent term or vote.
- Preserve simple and joint quorum election behavior.
- Allow safe-read followers to forget a leader explicitly.
- Prevent leader forgetting from invalidating lease-based read assumptions.
- Complete lease-based reads after the existing current-term commit gate.

## Non-goals

- Leadership-transfer initiation, timeout, and `MsgTimeoutNow`; D20 owns them.
- Public `RawNode` or `Node` methods for forgetting a leader; D21 and D24
  expose the core operation.
- Wall-clock leases or clock synchronization. The host supplies logical ticks
  and must satisfy the documented bounded-drift assumption.
- Persisting or restoring volatile leader-lease state.
- Changing the default `ReadOnlyOption.Safe` behavior implemented by D18.
- Automatically retrying failed campaigns or reads.

## Safety assumptions

`ReadOnlyOption.LeaseBased` is safe only when:

```text
CheckQuorum == true
logical ticks advance at the configured cadence
clock drift and pauses are bounded by the deployment's lease assumptions
```

Configuration validation already rejects lease reads without `CheckQuorum`.
Unlike safe reads, lease reads do not exchange a fresh quorum heartbeat for
each request.

## Leader quorum checking

### Tick ordering

`TickLeader` uses the existing `LeaderClockTick` result.

When `ElectionDue` is true:

1. if `CheckQuorum` is enabled, step a local `MsgCheckQuorum`;
2. D20 will later abort an expired leadership transfer here; and
3. if quorum checking changed the role, stop processing the leader tick.

When the node remains leader and `HeartbeatDue` is true, step the existing
local `MsgBeat`.

The quorum check therefore runs before a heartbeat due on the same tick. A
leader that steps down does not emit a new heartbeat after losing quorum.

### Activity predicate

`MsgCheckQuorum` is meaningful only in leader state.

The leader asks:

```csharp
Tracker.QuorumActive()
```

The D09 tracker evaluates `RecentActive` through the current simple or joint
voter configuration. Learners do not count. The local leader remains recently
active while it is leader.

When the voter quorum is inactive, the leader becomes a follower in the same
term with no known leader.

After the check, every remote progress record is marked inactive. This happens
whether the check succeeded or caused a step-down; `BecomeFollower` already
resets progress when stepping down. The next election-timeout window must
observe fresh append or heartbeat responses.

The existing response paths continue to mark progress active:

- accepted or rejected `MsgAppResp`;
- `MsgHeartbeatResp`; and
- any future response path that explicitly proves peer activity.

Unreachable reports and missing responses do not mark activity.

Newly added progress remains initially active through D10/D17. This prevents a
configuration change from causing an immediate leader step-down before the new
member has had a full activity window.

## Recent-leader vote suppression

Before applying ordinary higher-term handling, a `MsgVote` or `MsgPreVote`
request is ignored when:

```text
CheckQuorum
&& LeaderId != 0
&& ElectionElapsed < ElectionTick
&& request is not a forced leadership-transfer campaign
```

Ignoring means:

- do not update term;
- do not change role, vote, leader, or timers; and
- do not send a vote response.

The check applies in every role that still has a known leader. Ordinary
candidate and pre-candidate transitions clear `LeaderId`, so peers that have
explicitly begun campaigns can still vote for each other.

D19 introduces the pinned campaign kinds:

```csharp
internal enum CampaignType
{
    PreElection,
    Election,
    Transfer,
}
```

The transfer kind performs a real election and attaches the exact UTF-8 byte
sequence `CampaignTransfer` to every remote `MsgVote`. D19 recognizes that
context in the term filter now. D20 can therefore invoke the transfer kind
without reopening campaign construction or lease suppression.

`ElectionElapsed == ElectionTick` is outside the lease. A higher-term real vote
then follows normal term advancement, while a future-term pre-vote remains a
non-mutating pre-vote.

## Lower-term recovery messages

When an incoming message has a lower nonzero term:

### Lower-term append or heartbeat

If either `CheckQuorum` or `PreVote` is enabled, a lower-term `MsgApp` or
`MsgHeartbeat` produces:

```text
Type = MsgAppResp
To   = sender
Term = local current term
```

The original append or heartbeat payload remains ignored. The response lets a
stale leader learn the higher term and step down, freeing a candidate that
could not otherwise rejoin its old majority.

The response follows the existing after-append queue policy for
`MsgAppResp`.

### Lower-term pre-vote

A lower-term `MsgPreVote` always receives:

```text
Type   = MsgPreVoteResp
To     = sender
Term   = local current term
Reject = true
```

This compatibility response is emitted even when the local node has `PreVote`
disabled. It avoids migration deadlock when enabling pre-vote in an existing
cluster.

All other lower-term messages remain ignored.

## Pre-vote campaigns

### Campaign selection

An eligible local `MsgHup` starts:

```text
PreVote == false -> real election
PreVote == true  -> pre-election
```

The existing promotion and unapplied-configuration gates run before either
campaign.

The existing internal parameterless `Campaign()` remains an ordinary-election
wrapper for existing tests:

```csharp
Campaign() => Campaign(CampaignType.Election)
```

The campaign-kind overload owns pre-election, ordinary-election, and
transfer-election message construction. D20 must call
`Campaign(CampaignType.Transfer)` rather than the parameterless wrapper.

### Pre-election state

Starting a pre-election:

```text
Role     = PreCandidate
Term     = unchanged
Vote     = unchanged
LeaderId = 0
votes    = cleared
```

The requested campaign term is:

```text
Term + 1
```

Term overflow throws `RaftInvariantException`.

Each configured voter receives a `MsgPreVote` with:

```text
Term    = requested next term
Index   = local last log index
LogTerm = local last log term
Context = empty
```

The local voter is represented by a self-addressed
`MsgPreVoteResp(requested next term)`. It uses the existing after-append queue,
matching the pinned ordering and the generic vote-response durability model,
even though pre-vote itself changes no hard state.

### Receiving vote requests

`MsgVote` and `MsgPreVote` share log-freshness comparison. A request may be
granted when the candidate log is up to date and one of these holds:

```text
Vote == request.From
Vote == 0 && LeaderId == 0
request.Type == MsgPreVote && request.Term > Term
```

The future-term pre-vote clause lets a node probe without changing local hard
state.

On grant:

```text
response.Type = matching vote response type
response.Term = request.Term
```

For a real vote only:

```text
ElectionElapsed = 0
Vote = request.From
```

A granted pre-vote changes neither term, vote, role, leader, nor elapsed
timers.

On rejection:

```text
response.Type   = matching vote response type
response.Term   = local Term
response.Reject = true
```

All vote and pre-vote responses remain in the after-append queue.

### Message-term rules

Every vote and pre-vote request or response must carry a nonzero term.

For a message above the local term:

- `MsgPreVote` never advances local term;
- granted `MsgPreVoteResp` never advances local term;
- rejected `MsgPreVoteResp` advances to the response term and becomes a
  follower; and
- real vote messages use ordinary higher-term handling after the lease check.

For a message below the local term, the recovery rules above run before role
dispatch.

### Pre-vote responses

Only a pre-candidate processes `MsgPreVoteResp`. Only a candidate processes
`MsgVoteResp`.

Both use the existing D09 vote tracker:

- `Pending`: remain in the current campaign phase;
- `Lost`: become follower in the current local term with no leader;
- pre-vote `Won`: start an ordinary real election;
- real-vote `Won`: become leader and broadcast append state.

Winning pre-vote does not directly become leader. The subsequent
`BecomeCandidate` increments the term and records the self-vote before real
vote messages are sent.

Delayed pre-vote responses received after entering candidate state are
ignored.

## Forgetting a leader

`MsgForgetLeader` is a termless local operation.

### Follower in safe-read mode

When `ReadOnlyOption.Safe` is configured, a follower clears:

```text
LeaderId = 0
```

It preserves term, vote, role, election elapsed, randomized timeout, log,
progress, read states, and outbound queues.

Clearing `LeaderId` disables the recent-leader vote suppression immediately.
The follower can grant an otherwise valid pre-vote even when its election
timer is still inside the previous lease window.

Log freshness remains mandatory, so forgetting a leader does not allow a
stale candidate to win.

### Lease-based mode

When `ReadOnlyOption.LeaseBased` is configured, the follower logs an error and
does not clear `LeaderId`. Forgetting the leader would invalidate the same
lease assumption used to serve reads.

The error is emitted even when `LeaderId` is already zero, matching the pinned
option-level rejection.

### Other roles

A leader treats `MsgForgetLeader` as a no-op. Candidates and pre-candidates
ignore it.

## Lease-based ReadIndex

The D18 request shape, follower forwarding, current-term commit gate, output
ownership, and local-versus-remote response construction remain unchanged.

After the gate opens:

```text
ReadOnlyOption.Safe
    -> D18 heartbeat quorum confirmation

ReadOnlyOption.LeaseBased
    -> immediate response at Log.Committed
```

Lease eligibility is explicit:

```text
ReadOnlyOption == LeaseBased
&& local progress exists
&& local progress is not learner progress
&& Id belongs to incoming or outgoing voters
```

Only an eligible local voter answers immediately without D11, heartbeat
context, or per-request acknowledgement.

All other leaders use the safe D11 path even when the configured option is
`LeaseBased`. Progress existence or `IsLearner == false` alone is not
sufficient because D13 deliberately permits a tracked but unconfigured
non-learner edge state.

The D18 safety hardening for retained non-voter leaders therefore remains in
force. A local node in the outgoing side of a joint configuration, including a
node staged in `LearnersNext`, remains lease-eligible until leaving joint
consensus makes it a real learner or removes it.

Gated lease requests are cloned and released in FIFO order after the first
current-term commit. Each response uses the commit index current when that
request is released.

## Ownership and reset behavior

- Vote, pre-vote, quorum-check, and forget-leader handlers do not mutate
  caller-owned messages.
- Campaign messages are newly allocated.
- Pre-vote does not reset hard state.
- Transitioning from pre-candidate to candidate performs the ordinary reset,
  clearing pre-vote tallies.
- CheckQuorum step-down performs the normal role reset, including discarding
  active D11 confirmation state.
- ForgetLeader alone performs no reset.
- Lease reads reuse D18's immutable `ReadState` and cloned remote response
  ownership.

## Errors and logging

- Zero-term vote or pre-vote messages throw `RaftInvariantException`.
- Pre-election term overflow throws `RaftInvariantException`.
- Lease-suppressed vote requests are informationally logged and otherwise
  ignored.
- Quorum-loss step-down is logged at warning level.
- Lease-mode `MsgForgetLeader` is logged at error level.
- Unknown local roles or vote results retain explicit invariant failures.

## Test plan

### CheckQuorum

- a three-voter leader steps down after an inactive election-timeout window;
- a recent response from one remote voter preserves a three-voter leader;
- remote activity is cleared after each successful check;
- rejected append responses count as activity;
- learners do not satisfy quorum;
- simple and joint voter quorums are evaluated correctly;
- a local singleton remains leader;
- newly added progress receives one full activity window;
- quorum loss on a tick suppresses a heartbeat due on that same tick.

### Leader lease and lower-term recovery

- followers and leaders with a recent known leader ignore higher-term vote and
  pre-vote requests;
- ignored requests do not change term, vote, role, leader, elapsed timers, or
  queues;
- requests are processed after the lease expires;
- `Campaign(CampaignType.Transfer)` emits real votes with the exact
  `CampaignTransfer` context and bypasses suppression;
- lower-term append and heartbeat messages elicit current-term append
  responses when `CheckQuorum` or `PreVote` is enabled;
- the same lower-term messages remain silent when both options are disabled;
- lower-term pre-votes are rejected with the local term.

### PreVote

- a pending pre-election with no quorum response remains pre-candidate with
  unchanged term and vote;
- a recorded quorum loss returns to follower in the same term with no leader
  and preserves the same-term persisted vote;
- quorum pre-vote success starts a real election and increments term exactly
  once;
- singleton pre-vote requires separate durable self-response cycles for
  pre-vote, real vote, and leader no-op;
- pre-vote grant never changes responder hard state or role;
- stale-log pre-vote is rejected;
- learners may grant pre-votes but cannot campaign;
- simple and joint pre-vote quorums match D09;
- rejected higher-term pre-vote response advances local term;
- granted future pre-vote response does not advance term before quorum;
- delayed pre-vote responses are ignored after entering candidate state;
- split-vote campaigns can succeed on a later round;
- enabling pre-vote does not let a stale isolated follower disrupt an active
  leader;
- two pre-candidates that have cleared their leaders can form a quorum while
  the active leader still suppresses both vote types inside its lease;
- mixed-version migration rejects lower-term pre-votes with the responder's
  current term and can still complete a later election;
- a stuck pre-candidate is freed by the lower-term append/heartbeat recovery
  response and can subsequently win.

### ForgetLeader

- a safe follower clears only `LeaderId`;
- clearing the leader allows a valid recent pre-vote;
- stale candidates remain rejected after forgetting;
- a lease-based follower logs an error and preserves its leader;
- leader, candidate, and pre-candidate handling is a no-op.

### Lease reads

- local and forwarded lease reads answer immediately after the current-term
  gate;
- no D11 pending request or heartbeat context is created;
- a new leader queues lease reads until committing its no-op;
- queued lease reads use release-time commit indexes and FIFO order;
- retained removed or demoted leaders continue to use safe confirmation;
- a tracked but unconfigured non-learner uses safe confirmation;
- an outgoing-only voter and a voter staged in `LearnersNext` remain eligible
  for the lease fast path;
- default safe reads retain their D18 quorum behavior.

## Acceptance criteria

D19 is complete when:

1. an inactive leader steps down only after the configured activity window,
   while a voter quorum of recent responses preserves leadership;
2. pre-vote prevents unsuccessful campaigns from incrementing term and
   transitions to a real election only after quorum approval;
3. leader-lease suppression and lower-term recovery messages match the pinned
   term rules;
4. forgetting a leader is safe-mode only and preserves unrelated state;
5. lease reads complete without per-request quorum messages only under the
   validated `CheckQuorum` configuration;
6. simple, joint, learner, reset, migration, and ownership tests pass;
7. all existing tests remain green; and
8. an independent review finds no unresolved correctness issue.
