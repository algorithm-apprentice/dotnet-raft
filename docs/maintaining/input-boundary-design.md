# Parse Raft input once, at the boundary that owns it

Status: membership lookup, ReadIndex acknowledgement, and Append parsing
are implemented.
[ADR 0008](../adr/0008-protocol-input-hardening.md)
owns the existing rejection and fault semantics; the design must preserve
them rather than move errors from one layer to another.

## Problem and goals

Peer messages are mutable generated protobufs. A node can check a field,
discard the result, and hand the original bytes to another layer to check
and decode again. That was the path for ReadIndex heartbeat acknowledgements
and for Append batch geometry. Separately, `RaftNode` used to materialize a
sorted progress snapshot to answer whether one ID was present. Neither a
second check of identical data nor an unused representation adds evidence.

**Parse, not validate** means a boundary produces usable data that carries
the invariant it established. It does not mean accepting unchecked bytes
or substituting a type for a decision about current consensus state.
Preserve the public API, protobuf presence and bytes, diagnostics, SQLite
schema, and synchronous and asynchronous Ready contracts. Do not add a
second representation of every wire message or a second consensus path.

## Ownership and lifetime

| Boundary | What it establishes | What it does not establish |
|---|---|---|
| Host transport | Peer identity and permitted routing; protobuf decoding establishes wire syntax. | Membership, quorum authority, or application completion. |
| `RaftNode` caller to owner | A queued call owns its input; cancellation and dispatch rules govern whether it executes. | Whether a term, role, or membership observed before queueing will still apply. |
| `RawNode` to `RaftCore` | Peer input is detached, and fields consumed by the transition are parsed before core mutation. | That the message wins a vote, matches the local log, or should be counted. |
| Core transition | Current term/role, quorum, log matching, and effect ordering. | Network delivery, disk publication, or physical application. |
| Backend and application | Values read from their own durable or serialized records are checked and parsed at that boundary. | That an in-memory parse result remains valid across replay or a later transition. |

Do not expose mutable protobufs as proof that a check ran. For a scalar,
prefer a value type; for a batch, own the data and do not expose a mutable
alias. No `await`, queueing, or retained state-dependent object between
preparing a network input and dispatching it. Local completions do not
travel on the wire and retain their own origin and ordering rules.

## Slice 1: look up membership, do not build diagnostics

Add an internal `RawNode.HasPeerProgress(id)` query on the serialized
owner. It reads `ProgressTracker.Contains`, also used by `RawNode` when
filtering peer responses. Use it for `RaftNode`'s response filtering and
local progress observation. Keep the existing observation points and
the distinction between never observed, currently present, and removed.
A learner has progress; neither a voter-only test nor a cached peer set
substitutes for the lookup. `VisitProgress` must continue to return
detached, sorted diagnostics and must not serve the scheduler.

**Evidence:** the [facade tests](../../tests/DotnetRaft.Tests/Node/RaftNodeCommandTests.cs)
cover ignored senders, learners, removal, and re-addition; the
[status tests](../../tests/DotnetRaft.Tests/RawNode/RawNodeStatusTests.cs)
cover sorted, detached observations. Compare allocation per completed
`StepAsync` for a known response at three and 31 voters with memory storage.
The cost of the lookup must not grow by materializing a snapshot of the
voters. Do not treat the microbenchmark as an end-to-end throughput result.

## Slice 2: consume a parsed ReadIndex acknowledgement

The heartbeat context echoed to the leader is not the application's
request context or a log index. Empty context means ordinary liveness
feedback. A read acknowledgement is a positive, eight-byte little-endian
position no later than the last registered read position in this leader's
active tracker. A position already confirmed is still a valid earlier
position; recording it does not regress progress or release a read again.

1. Keep the existing term and role gate. For `MsgHeartbeatResp` about to
   be used by a leader (current term or the existing term-zero path), parse
   its context on the serialized owner and check it against that tracker.
   A lower-term response, a response ignored by a follower, or a
   higher-term response that steps the node down must not parse an unused
   context. Unknown responses keep the existing facade-specific behavior.
2. Pass `ReadAcknowledgement?` into dispatch: null means no read evidence,
   not a zero position to record. Downstream heartbeat handling accepts the
   parsed position, not the original context. The original protobuf stays
   available for dispatch and diagnostics; it is not the handler's source
   of read evidence. Capture the position when adding a local read so the
   leader's own acknowledgement never needs a byte round trip.
3. Preserve recovery and fault boundaries. Malformed context on an active
   peer route must be rejected with `ArgumentException` (parameter
   `context`) before altering peer activity, flow control, replication
   output, or read progress. The direct internal core entry, used by
   deterministic tests, uses the same parser but reports a core invariant
   error. One private dispatcher handles effects; the peer entry prepares
   and dispatches in the same call. `RawNode` activates only after a
   successful preflight; a rejected input must not consume bootstrap
   eligibility. Do not expose a method that lets callers pair an arbitrary
   message with a supposedly prepared position.
4. Do not reorder successful effects. A valid heartbeat response still
   updates peer activity, may send a catch-up append, and then records its
   read evidence for the current voter configuration. Quorum math, local
   application barriers, and the outgoing context encoding stay unchanged.

**Evidence:** the [network regressions](../../tests/DotnetRaft.Tests/RawNode/RawNodeNetworkValidationTests.cs)
must cover invalid size, zero, and a future position without progress or
output changes; exercise the accepted term-zero route, and higher- and
lower-term responses with unused malformed context. Prove that an earlier
valid position does not confirm a later read. The
[read quorum tests](../../tests/DotnetRaft.Tests/Core/RaftCoreReadIndexQuorumTests.cs)
must still distinguish voters, learners, and joint quorums. Run the exact
[dispatch transcript](../../tests/DotnetRaft.Tests/Core/RaftCoreDispatchCharacterizationTests.cs),
both Ready modes, public API and wire approvals, and the durable KV smoke.

## Slice 3: parse an owned Append batch

The peer boundary produces a `LogSlice`, not a success flag. It captures
the preceding tuple, an ordered sequence from the detached message, and the
last tuple. Every real index follows its predecessor and has a representable
successor; terms never decrease or exceed the sender term. Peer parsing also
keeps the existing entry-kind and configuration-payload checks. An empty
Append retains its preceding tuple without applying the real-entry index
limit to that tuple.

Pass that slice through dispatch to the follower. Neither the log nor
the unstable suffix needs to check this batch's geometry again. The log
still checks the local prefix and prohibits replacement of committed
entries; `UnstableLog` still checks the suffix against its current offsets
and snapshot. `RawNode` owns the detached peer input for that serialized
call; `LogSlice` borrows it only for that call and exposes only tuple values
and detached copies. For a conflicting suffix, it hands off a fresh owned
list of just that suffix to `UnstableLog`. Matching and ignored batches never
populate the unstable log or allocate a list of detached output entries.
Locally generated entries and reads from `IStorage` keep their own boundaries.

**Review decisions:** malformed peer Append structure is rejected before
activation even on a route that would ignore a well-formed Append. This
is the existing behavior, unlike the state-dependent fields on a heartbeat
response or snapshot. Do not set the term or leader hint before reporting a
peer error. The internal core entry used by the dispatch characterization can
still receive synthetic Append inputs; parse them when the existing path
would inspect the batch, so ignored payloads and the transcript remain
unchanged. Preserve the order of conflict, commit, and after-append trace
and output events. Do not change the stable read-side types or `IStorage`
to implement this write-side handoff.

**Evidence:** check empty, maximum-index, malformed and conflicting batches,
detachment of the retained entries and Ready outputs, and the same
after-persistence reply protocol in both storage modes. The
[dispatch transcript](../../tests/DotnetRaft.Tests/Core/RaftCoreDispatchCharacterizationTests.cs)
and [log regressions](../../tests/DotnetRaft.Tests/Core/RaftLogTests.cs)
must continue to pass without changing compatibility approvals.

## Follow-up decisions, not part of these slices

| Candidate | Design direction and gate |
|---|---|
| Configuration proposals | Retain the parsed configuration alongside the exact original entry bytes for admission and tracing in the same transition. Do not reserialize forwarded or accepted entries. Applying committed entries and replaying after restart are separate boundaries; future membership cannot be decided at proposal parsing time. |
| Incoming snapshots | Prepare an install decision and the configuration to install only when the term/role path uses a new snapshot. Already-matching prefixes must not install or reject unused configuration. Check that preflight did not mutate the current tracker and that older delivered application work retains its configuration. |
| SQLite `GetTerm` | A term lookup currently also validates the payload. A narrower query must not silently stop detecting corruption the current contract catches. Measure large entries, then review integrity at open, on entry reads, and at transaction publication before choosing any cache or query change. |
| Ready copies and KV throughput | First measure allocations, owner queue time, per-peer send waits, and application transactions. Preserve detached outputs, batch identity, durability, and post-application success. Do not combine a performance change with this input-boundary refactor. |

## Pre-implementation review

- **Rejected eager parsing.** A follower must echo a leader's heartbeat
  context without interpreting it. An old reply can be ignored, while a
  higher-term reply still has a term effect even when its context is
  irrelevant. One predicate must identify the route that can consume the
  context; membership is checked before decoding on either entry path.
- **Rejected parsing inside the mutating handler.** Peer progress and
  catch-up output are already changed there. Moving the decode to that
  handler would turn a bad input into a partial transition. Preflight
  and dispatch must share the same owner turn, and the prepared result
  must be consumed there rather than cached.
- **Rejected parsing on the enqueueing thread.** While an input waits,
  the owner may register more reads or replace the tracker after a term
  change. The concurrent facade must not read it directly from a caller.
- **Rejected a whole-message rewrite.** The generated message remains
  the wire and diagnostics model. A private prepared scalar is enough to
  make the first change auditable. No new public API, approval changes,
  or change to the worker protocols is justified.

The original two changes can be tested independently. Strengthen the existing
read-boundary tests to capture peer progress and output, not just final
status. The proposed performance check is a sequential public `StepAsync`
loop with warm-up (known response to a follower, no outgoing work), not a
concurrent-client or SQLite performance claim.

## Implementation evidence

### Membership lookup and ReadIndex

The format check passed. Debug and Release each passed 1,099 tests,
including the unchanged dispatch transcript and compatibility approvals.
The real-process KV smoke and refactor benchmark smoke passed. The new
regressions check bootstrap eligibility after rejection, unused context on step-down,
term-zero replies under both Ready protocols, unchanged peer progress on
parse failure, and catch-up output preceding a forwarded read result.

The temporary Release allocation probe compared the `7cc59c0` baseline
with both slices on macOS arm64, .NET SDK 10.0.100. It measured five
rounds of 25,000 sequential `StepAsync` calls after 2,000 warm-up calls,
using a term-zero `MsgAppResp` from a known voter to a follower.
The value is the median process-wide allocation per completed call;
the follower's role and term were unchanged.

| Progress entries | Before (bytes/op) | After (bytes/op) |
|---|---:|---:|
| 3 | 3,408 | 1,424 |
| 31 | 9,328 | 1,424 |

The comparison confirms that the measured call no longer allocates in
proportion to the size of a diagnostic progress snapshot. It does not
measure transport, disk I/O, or peak throughput.

### Append batches

The final format check, 1,109 tests in each configuration, all Counter runs,
the real-process KV smoke, and the refactor benchmark smoke passed. The
dispatch transcript and compatibility approvals were unchanged. New cases
cover optional and unknown protobuf fields, both storage protocols, empty
Append, and malformed structure rejected before a term or role change.

A separate temporary Release probe measured a follower's `Step`, Ready,
in-memory persistence, and `Advance` cycle. The comparison uses the
pre-Append implementation on the same Mac and SDK. It uses 512 warm-up
cycles, then the median of five 1,000-cycle samples. Matched batches reuse
one already-stable suffix; replacement batches alternate between two
terms for the same uncommitted range. Entry-count checksums agree.

| Entries | Matched before → after (bytes/op) | Replacement before → after (bytes/op) |
|---:|---:|---:|
| 1 | 3,096 → 2,952 | 4,088 → 3,776 |
| 64 | 7,577 → 6,929 | 26,810 → 24,938 |
| 256 | 21,401 → 19,217 | 96,086 → 89,502 |

These are allocation observations for an in-memory workload; elapsed
time varied across runs. The stable read-side and SQLite paths are unchanged.
