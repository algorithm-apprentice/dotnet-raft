# Readability roadmap

The goal is for a C# developer new to this repository to follow one real
write from proposal through storage, quorum commitment, and application, and
then find the code and tests that own each rule. A shorter file or a green
test run alone does not establish that the path is easier to read.

## Scope and design

- **One consensus transition owner.** `RaftCore.Step` handles the term
  and shared messages before routing by the resulting role. Replace the
  stateless strategy objects with private routing methods. Use partial
  files to put related transitions next to one another, not to create
  independent state machines. Cross-component sequences such as commit
  then ReadIndex release and snapshot installation remain explicit.
- **Two ways to drive the same node.** Keep `RawNode` for hosts that
  already serialize calls and `RaftNode` for concurrent callers. A caller
  can await a queued operation without tying up a thread; a synchronous
  `IStorage` read can still hold up that node's owner loop. Asynchronous
  storage writes are a separate host completion protocol.
- **No compatibility migration.** Preserve the public API, protobuf wire
  format, SQLite schema, and host ordering. Do not introduce asynchronous
  `IStorage` reads, a new scheduler, or new dispatcher queues. If production
  measurements later show a storage bottleneck, evaluate that separately.

## Sequence and gates

| Stage | Deliverable | Acceptance gate |
|---|---|---|
| 1. Baseline and plan review | Record the source revision, Debug test result, and a same-machine Release refactor benchmark. Review the mutation and observation boundaries below before editing implementation. | Existing [dispatch transcript](../../tests/DotnetRaft.Tests/Core/RaftCoreDispatchCharacterizationTests.cs) and tests are green. Keep the baseline binary for comparison. |
| 2. Direct dispatch | Keep term handling and shared messages in `Step`. Route to private leader, follower, and campaigning handlers; do not conflate the vote responses accepted by candidate and pre-candidate. | Dispatch transcript unchanged, including output queues, traces, exceptions, and role/term state. Run the focused core tests. Remove tests that assert only the identity of the deleted strategy objects. |
| 3. Source layout and test paths | Keep construction, shared dispatch, and reset in `RaftCore.cs`. Organize election/leadership, replication, membership/snapshot application, ReadIndex, and diagnostics into named files. Remove test-only log helpers for append-plus-commit and restore-plus-commit; exercise the production log primitives and use real message paths to verify commitment. | One mutable owner and no new cross-module callbacks or state copies. Existing follower, snapshot, membership, ReadIndex, Ready, and async-storage regressions still pass. |
| 4. Learning and integration | Add an opt-in single-write Counter trace for both modes using existing core events and observations at host operations. Put the cursor and storage contracts next to the source declarations. Link the Counter trace to the same phases of the durable KV write. Demonstrate with a deterministic test that a stable read holds up a queued command in either storage mode. | A reader can tell when an acknowledgement was queued, when storage was updated, which received acknowledgement advanced commit, and when the application ran and was acknowledged. Default Counter output stays unchanged. The storage test uses an explicit gate and releases it on every exit. |
| 5. Final verification | Run formatting, Debug and Release tests, the four Counter runs (both modes with and without trace), and the real-process KV smoke. Compare the Release refactor workloads with stage 1. | No compatibility approval changes. Report allocations, checksums, and timings as measured, not as a throughput promise for a SQLite host. The [test map](testing.md) and [benchmark guide](benchmarks.md) describe the checks. |

## Review the boundaries before implementation

| What could go wrong | What to review or verify |
|---|---|
| Routing uses the old role after a higher-term transition, or accepts the wrong vote response in a campaign. | Compare every role/message case before and after. Keep the term and shared-message branches ahead of role routing. The dispatch transcript records both state and effects. |
| Moving replication or membership hides the order of transitions. | Keep the existing bodies and call order during file moves. In particular, do not separate commit from releasing ReadIndex requests, or overwrite the configuration of application work already delivered before an incoming snapshot. |
| Removing a test helper accidentally removes the only assertion of a rule. | Match the old assertion to a test of the real transition before removing the helper. Follower append tests must still check the commit clamp, and log tests must still check rejection of committed-prefix conflicts. |
| A trace treats `MessageSent` as transport or persistence evidence, or treats `Applied` as the application's recovery point. | Trace core queuing and receipt separately from host storage and physical application. Use the core's acknowledgement cursor only for Raft accounting; read the recovered position from the actual application. Counter storage is memory, not a durability proof. |
| The storage test proves only that a timed wait happened. | Enter a known stable read before submitting the second operation. Assert the queued operation is still pending while the read is gated, then release the read and observe completion. Reuse the existing blocked-proposal test for the contrasting case. |

The plan review confirmed three limits: the dispatch transcript covers one
transition, not a crash or worker schedule; the core's trace callback observes
queueing and receipt, not persistence; and changing where code lives does not
justify changing its call order. This is why the work separates routing,
physical file moves, and host observations, and keeps the later integration
gates in addition to the dispatch check.

### Baseline and results

- Baseline: `62da565`, .NET SDK `10.0.100`, macOS arm64; Debug passed
  1,056 core/host tests and 37 SQLite tests, including the dispatch transcript.
- Final verification: format check, 1,093 tests each in Debug and Release,
  all four Counter runs, and the real-process durable KV smoke passed. The
  default Counter output and the dispatch, public API, wire, and schema
  approvals are unchanged. The trace runs also confirmed that storage precedes
  the counted append replies and that physical apply precedes its acknowledgement.

For the [Release refactor workloads](benchmarks.md#run-the-benchmarks), run
the saved baseline and candidate three times each on the same machine in
alternating order. The table shows the median of the three five-iteration
run medians. Timing ranges overlapped on each workload; allocations shown
below and checksums were unchanged. These are in-memory core workloads,
not evidence about SQLite or end-to-end KV throughput.

| Workload | Before (ns/op) | After (ns/op) | Bytes/op (both) | Checksum (both) |
|---|---:|---:|---:|---:|
| Sync proposal cycle | 4,751 | 4,703 | 6,050.24 | `9240325797370691861` |
| Follower heartbeat dispatch | 298.35 | 300.95 | 2,592.01 | `10936930208570869669` |
| Safe read completion | 1,643 | 1,638 | 5,560.02 | `3884428198604542453` |

## Group state by its owner

`RaftCore` is not a single flat state object today: `RaftRoleState`,
`RaftClock`, `RaftLog`, the tracker, proposal admission, and the read
coordinator already own state. The remaining problem is where the same
policy is surfaced as unrelated properties, or a lifecycle is represented
by independently writable fields. Review the same pattern across the
facades, log, storage packages, and example host instead of only rearranging
the top of `RaftCore.cs`.

| Boundary | Invariant and intervention | Evidence |
|---|---|---|
| Consensus configuration and log | Use named validated election and proposal policies. Let `ProposalAdmission` own its quota and policy. Move the committed-page bound to the `RaftLog` application delivery state, which owns the applying/applied cursors, outstanding bytes, and pause. Retain the distinct per-append size bound. The election scan uses the same limit as before. | Configuration, voting, proposal, log, and differential tests. |
| `RawNode` Ready | Track the last delivered soft/hard state as a pair, and keep the exact outstanding synchronous batch and its detached acknowledgement in one optional slot. Keep Ready creation, exceptions, and the order of effects in `RawNode`. Async mode selects the host protocol, not consensus; retain it on `RawNode`, not as another core property. | Ready identity and frozen acknowledgements; synchronous and typed async completion regressions. |
| `RaftNode` owner | A pending Ready waiter and an outstanding synchronous batch are mutually exclusive. Represent them in one slot without changing claim/cancel locking, dispatch, or scheduler order. Membership observation is unobserved, present, or removed, rather than two independently writable booleans. | Ready contention, cancellation, stop/fault, and removal/readdition tests. |
| Durable KV application | One applied prefix carries the physical index and its membership. Reuse one commit-and-publish step for commands, no-ops, configuration, and snapshot restore. The same lock protects it, and SQLite commits before its in-memory replacement. | Atomic rollback, reopen, snapshot restore, request retries, and real process smoke. |

The audit leaves the `SqliteStorage` metadata record, per-peer `Progress`,
inflight window, clock, read coordinator, request registries, and test/trace
observations with their existing owners. Field count alone does not justify
splitting them further. `RaftConfig`, `Ready`, and status/wire projections
remain compatible. Narrow read-only views are useful when they avoid exposing
mutable subobjects to the facades or tests. Do not merge the earlier
application configuration with live membership, the application's physical
prefix with the log's acknowledged cursor, or the pre-persistence and
post-persistence output queues.

Pre-implementation review: the grouped values may simplify representation,
but must not change when a budget pauses, when a Ready claim wins, or when a
transaction becomes visible. Verify that cancellation of a different queued
Ready request cannot clear a batch already awaiting `Advance`. Use in-place
or value state, not per-transition containers. Keep `RaftCore` and the
facades as orchestrators at their existing boundaries. Verify unchanged
approvals, format, both full suites, pinned Go comparison, the real-process
smoke, and a same-environment refactor benchmark against `5748c3c` before
calling this a behavior-preserving cleanup.

### State ownership verification

- The new Ready cancellation test fails if cancellation clears the whole
  slot: the original published batch can no longer be advanced. It passes
  when cancellation removes only the matching waiter.
- An initial layout made the full acknowledgement an inline value. It
  allocated less but slowed the heartbeat workload in repeated comparisons.
  The selected layout binds batch and acknowledgement with a small pair and
  keeps the existing acknowledgement record. It updates the delivered-state
  pair only when there is a state change.
- Compared with `5748c3c`, six alternating pairs of Release benchmark runs
  on the same macOS arm64 host kept all four checksums and bytes per operation
  unchanged. Timing ranges overlapped; this is not a throughput guarantee.
- The final layout passed 1,253 tests each in Debug and Release, formatting,
  all 13 pinned Go comparison checkpoints, and the durable KV process smoke.
  No public API, wire, schema, or golden approvals changed.
