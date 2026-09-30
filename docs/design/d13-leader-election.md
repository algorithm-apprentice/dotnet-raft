# D13 Leader Election Design

- **Status:** Accepted
- **Date:** 2026-09-30
- **DAG node:** D13
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D13 adds ordinary Raft leader election to the D12 core shell. A promotable
peer can time out, become a candidate, persist its self-vote, request votes
from the configured voter union, evaluate majority or joint-majority results,
and become leader or return to follower.

D13 also establishes the shared message-term filter and the election-related
parts of role dispatch. D14-D20 extend the same `Step` method with replication,
snapshots, membership, reads, availability extensions, and transfer behavior.

## Goals

- Trigger campaigns from deterministic election ticks and local `MsgHup`.
- Prevent learners, removed peers, and snapshot-restoring peers from
  campaigning.
- Block campaigns while a committed configuration change remains unapplied.
- Send vote requests with the candidate's last log term and index.
- Delay accounting of the candidate's self-vote until durable state completes.
- Grant at most one real vote per term, except repeat requests by the same
  candidate.
- Require candidate logs to be at least as up to date as the voter log.
- Let learners cast valid votes even though they cannot campaign.
- Ignore stale-term messages and step down on relevant higher-term messages.
- Resolve candidate vote responses through D09 simple or joint quorum math.
- Keep inbound protobuf messages read-only.

## Non-goals

- Pre-vote campaigns and responses; D19 owns `PreVote`.
- Leader-lease suppression of disruptive higher-term vote requests; D19 owns
  `CheckQuorum` election extensions.
- Handling append, heartbeat, or snapshot payloads; D14 and D16 own them.
- Appending the leader no-op or broadcasting initial append messages; D14
  completes leader activation.
- Proposal forwarding or dropping; D14 owns proposal behavior.
- Applying live configuration changes; D17 owns that integration.
- Leadership-transfer campaigns and forced vote context; D20 owns them.
- Public `RawNode.Step` and persistence completion APIs; D21 owns them.

## Core API additions

```csharp
internal sealed class RaftCore
{
    internal void Step(Message message);
    internal void TickElection();
    internal void Campaign();
}
```

`Step` reads but never mutates the caller's message. It throws
`ArgumentNullException` for null.

`TickElection` is valid only outside leader state. It consumes D12's
`TickElectionClock`; when that clock signals a promotable timeout, it steps a
local `MsgHup`.

`Campaign` performs an ordinary real-vote campaign. D19 later chooses between
ordinary campaign and pre-vote based on `RaftConfig.PreVote`.

D13 tests may use a test-only helper that captures
`TakeMessagesAfterAppend`, steps self-addressed messages, and records remote
messages. That helper is not a production persistence-completion API.

D21 must capture exactly the after-append messages accepted in one `Ready`
batch. Remote messages from that captured batch are exposed for transport, and
only captured self-addressed messages are stepped after persistence. Messages
created while stepping that batch belong to a later persistence cycle and
must not be drained recursively.

## Campaign eligibility

A local `MsgHup` is ignored when:

```text
Role == Leader
Promotable == false
there is a committed but unapplied configuration-change entry
```

`Promotable` is the D12 predicate:

```text
local progress exists
local progress is not learner
no unstable snapshot is pending or in progress
```

### Unapplied configuration changes

The core scans:

```text
[Log.Applied + 1, Log.Committed]
```

in pages bounded by `MaxCommittedSizePerReady`. A pending
`EntryConfChange` or `EntryConfChangeV2` blocks the campaign. The scan stops at
the first match. Uncommitted configuration entries do not block election.

This gate prevents a peer from campaigning with membership state older than a
configuration already committed in its log.

## Starting a campaign

An eligible campaign:

1. calls `BecomeCandidate`;
2. uses the resulting current term as the election term;
3. obtains the sorted union of incoming and outgoing voter IDs;
4. reads `Log.LastEntryId`; and
5. emits one election message per configured voter.

For the local ID, the core sends a self-addressed `MsgVoteResp` into the D12
after-append queue. The candidate does not record its self-vote yet. The vote
is counted only when local persistence completes and the response is stepped
back through the test-only persistence helper.

For every other voter, the core sends:

```text
Type    = MsgVote
To      = voter ID
Term    = candidate term
Index   = local last index
LogTerm = local last term
```

Vote requests enter the immediate queue in stable voter-ID order.

If the local ID is not part of the configured voter union, no synthetic
self-response is created. `Promotable` intentionally follows the pinned
progress-based check, so a tracked but unconfigured non-learner can enter
candidate state but cannot win through a nonexistent configured self-vote.

## Message-term filter

`Step` handles term before message type.

### Local term zero

`Term == 0` denotes a local message and does not affect persistent term.
`MsgHup` is the D13 local message.

A real `MsgVote` or `MsgVoteResp` with term zero is malformed and throws
`RaftInvariantException`.

### Higher term

For a message with `message.Term > Term`:

- `MsgApp`, `MsgHeartbeat`, and `MsgSnap` become follower at the message term
  and record `message.From` as leader;
- every other non-pre-vote message becomes follower at the message term with
  no known leader, even when its payload handler belongs to a later DAG node.

The full reset clears the old vote before vote-request evaluation.

For example, a higher-term `MsgAppResp` immediately advances term and steps a
leader down in D13, but its rejection, progress, and replication fields remain
unprocessed until D14.

D19 adds the two intentional exceptions:

- higher-term `MsgPreVote` and granted `MsgPreVoteResp` do not advance term;
- an active `CheckQuorum` leader lease may ignore an unforced vote request.

D13 preserves higher-term `MsgPreVote` and granted `MsgPreVoteResp` for D19 by
ignoring them rather than applying ordinary real-vote term changes. A rejected
higher-term `MsgPreVoteResp` follows the ordinary higher-term transition,
because the rejection communicates that the receiver has already advanced.

### Lower term

Messages with `message.Term < Term` are ignored before role dispatch and do
not change queues, votes, timers, or role.

D19 later adds the lower-term append/heartbeat response used with
`CheckQuorum` or `PreVote`, and the lower-term pre-vote rejection. D25 adds the
term-independent snapshot completion exception for local storage responses.

## Recognizing a current-term leader

Before D14 handles payloads, D13 owns leader recognition:

- a candidate or pre-candidate receiving current-term `MsgApp`,
  `MsgHeartbeat`, or `MsgSnap` becomes follower with `From` as leader;
- a follower receiving one of those messages resets `ElectionElapsed` and
  records `From` as leader;
- a leader does not step down for an equal-term message.

No append, commit, heartbeat response, or snapshot restoration is performed in
D13.

## Vote requests

D13 handles real `MsgVote` from every role, including learners.

A peer may vote when:

```text
Vote == candidate ID
    OR
(Vote == 0 AND LeaderId == 0)
```

and:

```text
Log.IsUpToDate(
    EntryId(
        Term: message.LogTerm,
        Index: message.Index))
```

The log comparison uses term before index, as implemented by D06.

### Grant

On a grant:

```text
ElectionElapsed = 0
Vote = candidate ID
```

The core sends:

```text
Type = MsgVoteResp
To   = candidate ID
Term = request term
```

The response enters the after-append queue, so the host must persist the new
term/vote before publishing the grant.

A repeat request from the already selected candidate may be granted again.

### Reject

When either voting eligibility or log freshness fails, the core sends:

```text
Type   = MsgVoteResp
To     = candidate ID
Term   = current term
Reject = true
```

Rejections also use the after-append queue, matching the conservative pinned
durability rule. Persistent vote and election elapsed remain unchanged.

## Vote responses

Only a candidate processes current-term `MsgVoteResp`.

The core records:

```text
Tracker.RecordVote(message.From, !message.Reject)
```

D09 first-vote-wins semantics make duplicate or contradictory later
responses from one ID harmless.

`Tracker.TallyVotes` determines:

- `Won`: call `BecomeLeader`;
- `Lost`: call `BecomeFollower(Term, 0)`;
- `Pending`: remain candidate.

Simple configurations require one incoming majority. Joint configurations
require both incoming and outgoing majorities.

D13 stops after structural `BecomeLeader`. D14 appends the no-op and
broadcasts replication.

Vote responses received by a follower, leader, or pre-candidate are ignored.
This includes a delayed durable self-vote after the candidate already stepped
down.

## Deterministic timeout behavior

`TickElection` delegates randomization to D12. The timeout that starts a
campaign remains in:

```text
[ElectionTick, 2 * ElectionTick)
```

When the clock expires, D12 resets `ElectionElapsed` before local `MsgHup`
processing. If campaign eligibility rejects the `MsgHup`, a new full timeout
must elapse before another attempt.

A candidate timeout starts a new election, increments term again, clears old
vote responses, and sends a fresh request set.

## Durability ordering

```text
candidate sets Term and Vote
        |
        +-- remote MsgVote requests -> immediate queue
        |
        +-- self MsgVoteResp -> after-append queue

follower grants vote
        |
        +-- Term/Vote visible in HardState
        |
        +-- MsgVoteResp -> after-append queue
```

In synchronous `Ready` processing, all outbound messages are sent after
durable state. In the core model, the separate self response guarantees that
the self-vote is not counted before its durability completion.

Role resets do not discard either queue. A delayed response retains its
original term and is filtered when eventually stepped or sent.

D13 never recursively drains a live after-append queue. That behavior exists
only in the pinned test harness. Production acceptance must preserve batch
boundaries so a newly generated self acknowledgement cannot be mistaken for
state persisted in the preceding batch.

## Ownership

- `Step` treats the inbound message as immutable.
- Campaign messages are newly allocated and then cloned by D12 `Send`.
- Vote responses are newly allocated and core-owned.
- The test persistence helper captures one after-append batch before stepping
  its self-addressed messages.

## Invariants

```text
Term never decreases
one real candidate ID is persisted per term
granted vote responses wait for durable Term/Vote
self-vote is not tallied before durability completion
candidate vote requests contain LastEntryId
stale-term messages cannot alter election state
candidate becomes leader only after configured quorum
joint election requires both majority results
learners may vote but may not campaign
current-term leader traffic makes candidates followers
```

## Test plan

### Campaign and timeout

1. local `MsgHup` starts a candidate campaign and increments term;
2. follower and candidate timeouts start elections at deterministic endpoints;
3. candidate timeout starts a new term and replaces prior vote state;
4. leader, learner, removed local ID, and unstable-snapshot peer do not
   campaign;
5. committed unapplied V1 and V2 configuration entries block campaign;
6. uncommitted or already applied configuration entries do not block;
7. a configuration change located after at least one size-bounded scan page
   still blocks campaign, and scanning stops after the matching page;
8. campaign requests target the sorted simple or joint voter union;
9. vote requests contain the exact last log term and index;
10. self-vote remains untallied until after-append completion;
11. a singleton becomes leader only after its durable self-response;
12. role transition before self-response delivery leaves the peer follower and
    does not record the stale vote.

### Term and leader handling

13. higher-term append, heartbeat, and snapshot messages step every role down
    with the sender recorded as leader;
14. higher-term messages whose payload handlers belong to later nodes,
    including `MsgAppResp`, still advance term and step leaders down;
15. a higher-term vote request clears prior vote and is then evaluated;
16. higher-term vote responses step candidates down without tallying;
17. lower-term vote requests, responses, and leader messages are ignored
    without side effects;
18. current-term leader messages reset follower election elapsed;
19. current-term leader messages make candidates followers;
20. leader messages carrying entries, commit indexes, or snapshot metadata
    change only term, role, leader identity, and election timing in D13;
21. an equal-term leader remains leader and does not process those payloads;
22. malformed term-zero real vote messages fail explicitly;
23. higher-term `MsgPreVote`, granted `MsgPreVoteResp`, and rejected
    `MsgPreVoteResp` preserve the D19 term distinctions;
24. D19 reserves lower-term pre-vote and leader-lease term exceptions.

### Voting and quorum

25. first real vote in a term is granted when the candidate log is current;
26. repeat requests from the same candidate are granted;
27. another candidate in the same term is rejected;
28. a known current-term leader prevents an otherwise uncast vote;
29. vote grant resets election elapsed while rejection does not;
30. the complete term/index freshness matrix matches D06 ordering;
31. a learner grants a valid vote but cannot start its own campaign;
32. vote grants and rejections both enter the after-append queue;
33. pending, won, and lost simple-majority outcomes match the paper matrix;
34. joint elections do not win with only one constituent majority;
35. unknown, duplicate, contradictory, stale, and wrong-role responses cannot
    create a false quorum;
36. candidate loss returns to follower at the same term and preserves the
    persisted self-vote;
37. becoming leader in D13 does not append a no-op or emit append messages.

### Durability batch boundaries

38. a test helper captures one after-append batch before stepping self messages;
39. remote responses remain part of that captured accepted batch in order;
40. self responses are stepped locally and never emitted as network traffic;
41. messages generated by stepping a captured self response remain pending for
    a later persistence cycle rather than joining the accepted batch;
42. delayed messages preserve their original term across later role resets.

## Design review resolution

The GPT-5.6 Sol design review found two correctness blockers and two test
hardening gaps:

1. higher-term filtering was too narrowly tied to D13-supported payloads;
2. a recursive test drain was incorrectly proposed as a reusable D21 API;
3. pagination was not forced by the campaign test matrix; and
4. the D14/D16 payload boundary was not directly observable.

All four findings are accepted. The design now applies higher-term transitions
globally, removes the recursive production API, requires a later-page
configuration test with early termination, and verifies that leader-message
payloads remain untouched in D13.

## Development sequence

D13 uses strict test-driven development:

1. review and accept this design;
2. add term, campaign, vote, quorum, and durability tests;
3. run the focused target and record the expected compile failure;
4. implement election behavior without replication payload handling;
5. refactor while preserving the D14/D19 boundaries;
6. run formatting and the full suite;
7. resolve substantiated code-review findings before commit.

## Completion criteria

D13 is complete when:

- this design is reviewed and accepted;
- deterministic ordinary elections work for simple and joint configurations;
- vote durability, one-vote-per-term, and log freshness are verified;
- stale and higher-term transitions match the pinned behavior in D13 scope;
- learners vote but never campaign;
- no pre-vote, replication, or leader no-op behavior is implemented early;
- formatting, build, and the full suite pass;
- code review has no unresolved substantiated findings;
- the reviewed changes are committed before D14 starts.
