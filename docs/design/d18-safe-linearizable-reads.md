# D18 Safe Linearizable Reads Design

- **Status:** Accepted
- **Date:** 2026-10-01
- **DAG node:** D18
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D18 integrates the D11 read-only tracker with the deterministic Raft core.
Safe `ReadIndex` requests wait until the leader has committed an entry in its
current term, then use heartbeat acknowledgements from the active voter
configuration to prove that the leader still holds authority. Confirmed local
requests become read states, while confirmed forwarded requests become
`MsgReadIndexResp` messages.

The host remains responsible for applying committed entries through the
returned read index before serving the application read. D21 will expose this
core behavior through `RawNode.ReadIndex` and `Ready.ReadStates`.

## Goals

- Implement the safe `ReadIndex` path for local and forwarded requests.
- Preserve the single-voter fast path from the reference implementation.
- Delay multi-voter reads until the leader commits an entry in its own term.
- Carry cumulative D11 read positions in heartbeat contexts.
- Count acknowledgements through simple and joint voter quorums.
- Keep learners and unknown senders from satisfying voter quorum.
- Return confirmed reads locally or to the original remote requester.
- Forward follower requests without attaching a stale term.
- Preserve request and response ownership across asynchronous host handling.
- Release tracker memory after reads become confirmed.

## Non-goals

- Lease-based reads; D19 implements them together with `CheckQuorum`.
- `RawNode.ReadIndex`, `Ready`, `HasReady`, or `Advance`; D21 owns the public
  integration surface.
- Application-state-machine reads or waiting for application to reach the read
  index.
- Request timeouts, cancellation, retries, or deduplication.
- Leadership transfer; D20 owns transfer behavior.

## Core output

D18 adds the immutable output model:

```csharp
public sealed record ReadState(
    ulong Index,
    ByteString RequestContext);
```

`Index` is the commit index captured when the leader accepts the request.
`RequestContext` is `Entries[0].Data` from that request. `ByteString` is
immutable, so the output does not expose mutable core state.

The core stores produced read states until:

```csharp
internal ReadState[] TakeReadStates();
```

`TakeReadStates` returns the states in confirmation order and clears only the
core's output collection. It returns a detached array, using the same
snapshot-and-clear ownership pattern as the existing outbound message queues.
D21 will consume this collection when constructing a `Ready`.

## Message shape and ownership

A `MsgReadIndex` request must contain exactly one entry. The entry data is an
opaque request context selected by the host. Empty context data is valid, but
zero or multiple entries are invalid and cause `RaftInvariantException` when a
leader attempts to process the request.

This explicit invariant replaces the pinned Go implementation's implicit
`Entries[0]` precondition and prevents a remote response that the receiver
would later reject.

The core never mutates a caller-owned message:

- a request waiting for a current-term commit is cloned;
- D11 clones a request when it enters safe-read tracking;
- follower forwarding uses an owned outbound clone;
- remote responses contain cloned request entries; and
- local read states retain only immutable context bytes; and
- draining read states returns a detached array that later core mutations
  cannot change.

A follower accepts a `MsgReadIndexResp` only when it contains exactly one
entry. A malformed response is logged at error level and ignored, matching the
reference's non-fatal response handling.

## Role dispatch

### Leader

The leader processes `MsgReadIndex`. `MsgReadIndexResp` has no leader-side
effect.

### Follower

For `MsgReadIndex`:

- when `LeaderId == 0`, the request is dropped because there is no forwarding
  target;
- otherwise, the follower forwards an owned copy to `LeaderId`;
- a nonzero `From` and the request entry are preserved;
- `From == 0` is normalized by `Send` to the forwarding follower's ID; and
- no term is attached to the forwarded request.

The existing outbound `Send` rule for `MsgReadIndex` enforces the termless
wire shape. This allows an old leader to forward a request again after
learning a newer leader.

For a valid `MsgReadIndexResp`, the follower appends:

```text
ReadState(
    Index          = message.Index,
    RequestContext = message.Entries[0].Data)
```

Candidates and pre-candidates ignore both read-index message types, matching
the pinned role handlers.

## Leader admission

### Single-voter fast path

The leader answers immediately at `Log.Committed` only when the tracker is a
non-joint single-voter configuration whose sole incoming voter is the local
node and whose local progress is voting progress.

This check occurs before the current-term commit gate and before the configured
read-only option. It matches the ordinary reference fast path while tightening
the retained-leader edge created by D17. The pinned implementation checks only
the number of voters, but D17 allows a removed or demoted node to remain leader
temporarily. Such a node must not answer immediately when the sole voter is a
different node.

A retained non-voter leader follows safe quorum confirmation. If the sole
remote voter still recognizes it, that voter can confirm the read. If the
remote voter has entered a newer term, normal term handling steps the retained
leader down instead.

### Current-term commit gate

Every non-singleton request must wait until:

```text
term(Log.Committed) == Term
```

Compacted or unavailable committed-term lookup is treated as term zero, so it
cannot satisfy the gate accidentally.

Requests arriving before the gate opens are cloned into a FIFO
current-term-waiting queue. They do not enter D11 tracking and do not emit
heartbeats yet.

Whenever a leader advances commitment, the core checks the gate and releases
the entire waiting queue in arrival order. Each released request follows the
full leader-admission path independently, including a fresh local-singleton
check. The read index is captured at release time, when the request enters D11
or produces a singleton result, not when the gated request first arrived. This
ensures that the returned index includes the current-term entry whose
commitment established the leader's authority.

Running the release check at every successful core `MaybeCommit` also covers
commitment caused by an applied membership change.

The current-term-waiting queue survives role reset, as in the pinned
implementation. If the node later becomes leader again, the requests can only
be released after a commit in that newer leadership term. In contrast,
requests already being confirmed by heartbeats are tied to the old leadership
term and are discarded when reset replaces the D11 tracker.

## Safe-read confirmation

For each admitted request that does not use the local-singleton fast path, the
leader:

1. records the owned request and current `Log.Committed` in D11;
2. obtains the latest eight-byte cumulative heartbeat context;
3. records the local node's acknowledgement for that context; and
4. immediately asks D11 to advance against the current voter set;
5. produces any prefix confirmed by the local acknowledgement; and
6. when requests remain pending, broadcasts heartbeats carrying the latest
   context.

Immediate advancement is normally a no-op for a multi-node quorum. It is
required for a valid local-only joint configuration such as
`Incoming = { local }, Outgoing = { local }`, where the local acknowledgement
already satisfies both majorities and no remote node exists to send a
heartbeat response.

The captured index is not changed if later proposals commit while quorum
confirmation is in flight.

D18 does not implement the multi-voter lease shortcut. Until D19 adds its
`CheckQuorum`-dependent lease rules, `ReadOnlyOption.LeaseBased` conservatively
uses this same safe heartbeat-confirmation path. Requests are never silently
discarded merely because the future option is configured. The local-singleton
fast path remains available in either mode.

## Heartbeat contexts

Every leader heartbeat broadcast, including periodic heartbeats, uses:

```csharp
ReadOnly.GetHeartbeatContext()
```

The result is empty when no safe reads are pending. Otherwise it is the D11
eight-byte little-endian position of the newest unconfirmed request. A follower
already echoes `MsgHeartbeat.Context` in `MsgHeartbeatResp`, so no
follower-side special case is required.

On a leader receiving `MsgHeartbeatResp`:

1. the sender must already have a progress record;
2. normal heartbeat-response progress handling occurs;
3. a nonempty context is recorded in D11;
4. D11 computes the confirmed prefix against the current
   `Tracker.Config.Voters`; and
5. every newly confirmed request is answered in FIFO order.

Unknown senders are ignored before reaching D11. Learner acknowledgements may
be recorded because they have progress records, but the quorum calculation
consults only incoming and outgoing voters. Joint configurations therefore
require both majorities.

Malformed, zero, or future D11 positions retain D11's explicit
`RaftInvariantException` behavior for known senders.

### Configuration changes

After installing a new leader configuration, the core immediately asks D11 to
advance against the new voter set. This can confirm an already active read
without another heartbeat response, for example when removing a voter leaves
the local node as the sole voter.

If reads remain active after re-evaluation, the leader broadcasts the latest
heartbeat context to the updated progress set. Gated requests released by a
membership-induced commit re-enter the full admission path and can therefore
take the newly valid local-singleton fast path.

## Producing a result

The destination is determined from the original request:

```text
local  = request.From == 0 || request.From == Id
remote = otherwise
```

For a local request, the core appends a `ReadState` using the captured commit
index and request context.

For a remote request, the core sends:

```text
Type    = MsgReadIndexResp
To      = request.From
Index   = captured commit index
Entries = owned copy of the original request entry
```

The normal `Send` path attaches the leader's current term to the response.

Duplicate request contexts are not deduplicated. Each request receives its own
internal D11 position, confirmation, and result. The host must use the opaque
context to correlate responses if it requires unique matching.

## Reset and memory behavior

Role reset:

- replaces the D11 tracker, discarding heartbeat-confirmation state from the
  previous leadership;
- preserves the current-term-waiting queue, matching the pinned behavior; and
- preserves already produced read states until the host drains them.

D11 removes confirmed requests from its FIFO. Once results are produced, the
tracker retains only the cumulative confirmed count and acknowledgement
positions, not the request messages or contexts.

## Errors and logging

- Invalid leader request shape throws `RaftInvariantException`.
- Invalid known-sender heartbeat context keeps D11's
  `RaftInvariantException`.
- A malformed follower response is logged at error level and ignored.
- A follower without a known leader drops the request without manufacturing a
  success response.
- Storage errors other than compacted or unavailable committed-term lookup
  propagate.

## Test plan

### Admission and outputs

- local and remote single-voter requests answer immediately;
- the single-voter path works before a current-term commit;
- a retained removed or demoted leader does not use the local-singleton fast
  path when the sole voter is remote;
- a local-only joint configuration confirms through the immediate local
  acknowledgement without waiting for a nonexistent remote response;
- local safe reads emit `ReadState`;
- remote safe reads emit `MsgReadIndexResp`;
- active safe reads preserve the commit index captured when they enter D11;
- invalid request shape fails explicitly;
- malformed responses are logged and ignored;
- draining read states preserves FIFO order and caller ownership.

### Current-term gating

- a new multi-voter leader queues reads before committing its no-op;
- no heartbeat is emitted while a request is gated;
- a commit in the leader's term releases all queued requests in order;
- a gated request returns the release-time commit index, including the
  current-term entry that opened the gate;
- a membership-induced commit also releases the queue;
- a membership transition to a local singleton releases a gated request
  through the singleton fast path;
- role reset discards active heartbeat confirmation but preserves gated
  requests;
- compacted or unavailable term lookup does not open the gate.

### Heartbeat and quorum behavior

- request heartbeats carry the latest D11 context;
- periodic heartbeats carry that same context;
- followers echo the context;
- the local acknowledgement participates automatically;
- unknown senders cannot affect D11;
- learners cannot satisfy voter quorum;
- simple-majority confirmation releases the read;
- joint confirmation requires incoming and outgoing majorities;
- applying a smaller voter configuration immediately re-evaluates and can
  release an active read;
- a multi-voter `LeaseBased` configuration with `CheckQuorum` enabled uses the
  safe path before D19: it produces no immediate result, carries a heartbeat
  context, and returns the captured index only after voter-quorum
  confirmation;
- malformed known-sender contexts fail explicitly;
- confirmed requests are removed from D11 memory.

### Forwarding and request identity

- followers forward to the known leader without a term;
- zero-origin follower requests are normalized to the forwarding node;
- followers with no leader drop requests;
- an old leader can forward the same request to a newer leader;
- candidates and pre-candidates ignore read-index messages;
- duplicate user contexts remain distinct requests;
- delayed acknowledgements cannot confirm a later request in another term;
- caller mutations after `Step` cannot alter queued requests, responses, or
  read states.

## Acceptance criteria

D18 is complete when:

1. safe multi-voter read indexes are never released before a current-term
   commit and voter-quorum heartbeat confirmation;
2. simple and joint quorum behavior matches D09 and D11;
3. local and forwarded requests produce the correct owned outputs;
4. follower forwarding and response validation match the documented role
   behavior;
5. reset, duplicate-context, delayed-response, and memory-release tests pass;
6. all existing tests remain green; and
7. an independent review finds no unresolved correctness issue.
