# Parse Raft input once, at the boundary that owns it

Status: all seven slices are implemented. The evidence below separates
hot-path allocations, durable transactions, and end-to-end behavior.
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
2. Pass `ReadAcknowledgement` into dispatch: its default means no read evidence,
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

## Slice 4: use the parsed configuration proposal

Keep the original protobuf entry for forwarding and append. Keep the parsed
change for admission and diagnostics in the same transition. The legacy
format contributes one operation even when its node ID is zero. An empty
V2 list is an admission signal; this is not the same as deciding how to
apply a committed transition. If policy replaces an entry with a no-op,
do not trace that configuration as proposed. Do not emit a proposal trace
after quota rejection. A local typed API should not have to decode what it
just encoded; the automatic joint exit also starts with a typed value.

**Gate:** a queued caller cannot mutate the retained configuration, malformed
peer proposals still fail before dispatch, accepted and forwarded entries
retain their exact bytes, and existing admission and trace ordering tests
continue to pass. Application and replay are separate boundaries.

## Slice 5: prepare the incoming snapshot decision once

Only the term/role route that can process a snapshot prepares it. The
decision is ignore, advance commit through a matching tuple, or install a
new snapshot with a detached `ConfigurationChangeResult`. Building that
result cannot mutate the live tracker. Do not decode or install unused
configuration for a matching tuple or an obsolete snapshot. Wrap errors
from a necessary configuration build as peer input errors before activation.

Consume the decision in the same owner turn. Stepping down or setting a
leader hint doesn't change the committed index or the logical log prefix;
valid host compaction cannot delete an index above the current commit.
Installation must still retain the old configuration for previously
delivered application work. Snapshot completion is a different, later
transition: retain its exact-index check and re-evaluate the live tracker.

**Gate:** exercise stale, matching, rejected and installed snapshots; prove
matching does not inspect unused membership or repeat the term lookup;
retain configuration-before-commit trace order and the two-mode regressions
for older delivered work and stale acknowledgements.

## Slice 6: project SQLite entry identity without materializing command data

`GetTerm` must still reject a row whose protobuf is malformed or whose term
or index disagrees with its SQLite columns. Selecting only `entry_term`, or
caching a previously checked term without re-reading the record, would
weaken that contract. Keep all integrity checks at open, reads, and
publication. Project the two scalar fields with the protobuf reader and skip
opaque command data without constructing an `Entry` or `ByteString`. Unusual
tags fall back to the generated parser rather than relying on its generic
skip behavior for unrecognized wire types. The `WITHOUT ROWID` table still
requires the driver to materialize the BLOB; `GetTerm` is not a metadata-only
query. Keep full decoding for an entry read that returns the command.

**Gate:** compare with the generated parser on absent and repeated fields,
unknown fields, truncated and invalid encodings, and unsigned boundaries.
Test corrupted interior entries and reopen/rollback on real SQLite files.
Measure small and large records against the same baseline. Keep this
optimization only if the narrower parse has equivalent integrity and a
worthwhile allocation gain.

## Slice 7: measure Ready and the KV host separately

Measure allocations with the same deterministic Ready workload. Find a
handoff where objects already detached from the core can move to `Ready`
without an additional clone. Do not alias the hard-state baseline, the
synchronous acknowledgement, mirrored async fields, or worker responses.
Do not clear outputs before the existing publication point or change any
trace failure semantics.

Measure the node queue, per-peer sends, and SQLite application writes
separately in the KV host. Do not change the host's single-consumer ordering,
durability mode, or success-after-application contract to win a microbenchmark.
The local three-process profile found millisecond application commits and
microsecond proposal queue waits. Batch only consecutive ordinary entries
already delivered in one Ready, capped at 64 entries per transaction; do
not wait for more work. A configuration entry, snapshot, or end of Ready
is a boundary. Preserve log order and the first durable request result;
publish the last physical index with all effects and resolve waiters only
after commit. A failed transaction exposes no results or cursor advance
from its batch. Keep network delivery and Raft persistence unchanged.

**Gate:** compare one and several concurrent clients with the same
three-node workload, and verify duplicate/conflicting IDs within a batch,
rollback and reopen, and the existing ReadIndex and restart behavior.

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
time varied across runs. This slice did not change SQLite or stable reads.

### Configuration, snapshots, and Ready

The peer configuration tests cover both encodings, exact forwarding,
unknown fields, and traces for accepted entries only. The snapshot test
counts one local term lookup in both the matching and installation paths;
when matching, it supplies configuration that would be rejected on install.
Ready tests mutate mirrored persistence and application entries and verify
that neither worker requests, completions, nor Raft's own cursors change.

Compared with `ea18c99`, allocation per operation in the Release refactor
workloads fell from 6,050 to 5,410 bytes for a proposal cycle, from 2,528 to
2,096 for follower heartbeats, and from 5,400 to 4,752 for safe reads.
Checksums matched. Timing varied materially, including on repeated runs
of the same binary; the comparison is evidence about allocation.

### SQLite term projection

On the same Mac and SDK, the warm `GetTerm` probe used five samples after
256 warm-up calls. Checksums matched. This removes a managed payload
copy, not the SQLite BLOB read, and is not a claim of lower latency for
small entries.

| Command size | Before (bytes/op) | After (bytes/op) |
|---:|---:|---:|
| 32 B | 1,776 | 1,544 |
| 64 KiB | 132,784 | 67,048 |
| 1 MiB | 2,101,320 | 1,050,136 |

Interior corruption tests cover mismatched term/index, a truncated field,
and invalid tags and groups. Reopen and transaction-publication checks
still use the same identity rule. Alternate encodings are compared with
the generated parser on actual stored rows.

### Durable KV application batches

In the local three-process loopback probe, requests were 32-byte values
on fresh files, with one and eight concurrent writers and 320 measured
commands per phase. The instrumented baseline had an application
transaction per command. The instrumented candidate completed the
eight-writer phase with 80 application transactions for 320 commands;
both made 320 transactions at one writer. Leader proposal queue, per-peer
send, Raft persistence, and application timings were sampled separately.
In the baseline the median queue wait was tens of microseconds, whereas
the median application transaction took about six to seven milliseconds.

Two alternating **uninstrumented** comparisons yielded about 27–28 ops/s
at one writer and 47 ops/s on the baseline versus 42–84 on the candidate
at eight writers. The variation does not justify a general throughput
claim. The reason to retain batching is the directly observed reduction
in transactions without extra waiting or a change to durability. The
integration tests prove that a malformed command rolls back its batch
without releasing waiters, and that the size cap and configuration
entries both end a transaction. No transport or scheduler change was made.

The final verification passed 1,131 tests in each of Debug and Release,
formatting, all four Counter runs, the real-process durable KV smoke, and
both benchmark smokes. The dispatch transcript, public API, protobuf wire,
and SQLite schema approvals were unchanged.
