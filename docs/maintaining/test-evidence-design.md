# Make the Raft tests distinguish failure paths

Status: reviewed and implemented. This changes tests and the example host's
observability, not Raft, either Ready protocol, public package APIs,
protobuf, or the SQLite schemas. The baseline is `2b8f92d`: 1,131 passing
Release tests.
The [test map](testing.md) remains the guide to where each invariant belongs.

## Evidence and acceptance gates

| Invariant | Observe at | A deliberately wrong path must fail because… |
|---|---|---|
| A restarted lagging process actually catches up by snapshot | Stop the lagger and read its durable last index. Before restarting it, observe durable compaction on both surviving peers past that index and through the large write. After restart observe incoming snapshot restore, verify the full value's hash through a linearizable read, and retry a request first applied during the outage. | Raising the threshold beyond the workload now fails at the compaction precondition. Ordinary log catch-up is not an allowed substitute, and a restore must preserve the original result index as well as the value. |
| Restart delivers only the missing application suffix | Observe each delivered entry before the mock application skips a covered index, independently of entries it actually applies. Compare the first recovered delivery with the durable physical prefix. | Restarting one index behind exposes a duplicate delivery even if the application has no duplicate effects. A distinct snapshot-overlap test must allow a previously delivered worker request to be superseded, but still return its completion. |
| SQLite publishes an entire durable generation | Seed a nonempty generation. After abrupt process exit compare the full hard state, snapshot, marker, and actual log entries with the expected generation. Exercise rollback before an insert and after the metadata write inside the real transaction. | Dropping vote, commit, or payload can no longer pass the crash test. The late injection is proven to execute, not inferred from any storage exception. |
| A confirmed read still waits for the local application | Drive the real follower host with a scripted peer. Deliver a read response before its committed entries, then observe an outbound marker from a later Ready. Then apply the prefix and check value and index. | A ready result, fault, or stalled Ready consumer before application fails. Posting a peer message or sleeping is not proof the Ready consumer reached the barrier. |
| An isolated former leader cannot confirm a new read | In the deterministic cluster, confirm a healthy read, partition the leader, elect in the majority, and apply a new value there. Submit a fresh read to the isolated leader while it still believes it leads. | Using a local commit index as authority produces an observable `ReadState` on the isolated node. A new read after healing must work; do not make a claim about whether the interrupted attempt eventually completes. |
| All replicas agree on the winner of a conflicting request ID | Identify the command that actually succeeded. Obtain a read barrier after both attempts finish, then inspect each replica once it has applied through that barrier. | A different winner, a wrong value, or a request result pointing at a different index fails even when each replica holds exactly one of the two keys. |

The smoke may add structured information logs to the example host, after
successful compaction or incoming snapshot reconciliation. These are useful
operational events, not test switches or public endpoints. Do not query a
live exclusively owned database from the smoke, change its durability
settings, or count an outgoing snapshot as a restore. Inspect only files
in the smoke's own stopped node directory.

The SQLite crash harness must terminate and reap its child on timeout and
retain both streams in failure messages. The fixture flushes its phase marker
and forcibly kills its own process while the SQLite scopes are still live.
Use `Process.Kill`, not `Environment.FailFast`: Windows Error Reporting and
dump generation can delay exit beyond the deadline. The launcher rejects
unexpected termination status (`-1` on Windows, `128 + SIGKILL` on Unix) and
the fixture's top-level managed unwind marker, so graceful disposal and
rollback cannot substitute for abrupt termination. Ordinary stderr diagnostics
remain allowed. `AbruptProcessTerminationRecoversAtomically` exercises both
transaction modes with and without `DOTNET_HOST_TRACE`, proves the requested
diagnostics were emitted, then reopens and compares the full durable generation.
It still tests process exit, not device power loss. For the late in-process
injection, keep the trigger and connection access in the tests; do not make SQL
fault injection part of the storage API.

## Make the schedules reliable and reproducible

- Keep bounded random exploration for safety, not a liveness assertion.
  Pair seeds between the Ready protocols, observe `BecameLeader` even
  inside a drain, and capture the executed actions (targets, dropped or
  duplicated messages, and skipped actions). Give a separate seeded recovery
  scenario a controlled write, crash with a real missing suffix, majority
  write, and a new write after recovery. Keep the existing async case where
  application is ahead of durable commit. Do not require every exploratory
  seed to make progress.
- In host tests give an asynchronous predicate the same deadline as the
  wait, bound the predicate wait itself, and report which phase failed and
  each node's last observable state. Release test gates in `finally` and
  stop all nodes even if one cleanup fails. Do not dispose databases while
  a live host might still be using them. In owner-loop tests, replace
  `Task.Yield` assumptions with completed commands spanning a scheduling
  round; retain the explicit claim/cancel barriers already in use.
- Discover each golden interaction file as its own test. Verify the copied
  corpus matches the checkout, and keep the pinned manifest and exact
  approvals. Verification must be read-only even if the old rewrite
  environment variable is set. An explicit command in the test assembly
  can render proposed output for one fixture to stdout, but never edits
  the fixture. A bad fixture should identify itself without hiding other
  results. Keep that command outside the package and production paths.

## Review before implementation

- **Compaction must be a precondition.** Wait for both survivors, not just
  the elected leader, so a change of leader cannot let the lagger catch
  up by replay. A stopped node's `last_index` includes entries ahead of
  its physical application; using only its applied index would prove less.
  The retried request is small and precedes the large write, so it was
  missing on the lagger and is necessarily covered by the restored prefix.
- **No special treatment in the actual node for restart tests.** Observe
  delivery from sync Ready or at dequeue of async apply work. Snapshot
  supersession is intentional, so assert the pre-skip boundary for a
  fresh restart, not for every worker action after a snapshot.
- **A SQLite trigger can fail too early.** The late trigger fires only
  after a metadata update, with the new marker, boundary and suffix
  visible inside that transaction. Verify the trigger error string and
  then reopen. The abrupt fixture keeps its simple SQL and does not add
  a power-loss claim.
- **Follower reply markers bracket consumption, not just dispatch.**
  Gate one heartbeat reply, deliver the read response while that Ready is
  outstanding, then enqueue a second heartbeat. Release the first marker,
  wait until the second enters the transport, and release it too. A third
  marker proves the previous Ready completed before asserting the read is
  still registered; otherwise a fast assertion can race `CompleteRead`.
  Inspect pending ownership through the registry's duplicate registration
  check. Even after an early release, the client task's asynchronous
  continuation might not yet have run.
- **A finite wait is not a license to dispose a running host.** Cancel the
  work, keep the separate cleanup deadline, and report incomplete stop as
  a failure while still cleaning up other nodes. Tests use known phase
  markers rather than a blanket global timeout.
- **Reject baseline rewriting during verification.** The explicit preview
  command uses the same script runner on a single fixture, stdout only.
  Verify fixture names against the checkout so stale copied files or a
  missing fixture do not go unnoticed.

Use the older negative controls in isolated copies, then verify every
normal case on the real paths. Don't make approvals or timeouts more
permissive to pass a negative control. Keep the existing deterministic
simulator, queue ownership, and separate durability tests rather than
building a second general-purpose harness.

## Verification

Run the full Debug and Release suites, formatting, the teaching examples,
and the three-process smoke. The following fault variants were exercised
only in temporary copies or with a temporary smoke configuration:

| Fault variant | Observed rejection |
|---|---|
| Raise the snapshot threshold from 4 to 1,000,000 | The smoke fails waiting for a survivor to compact through the large write, before restarting the lagging follower. |
| Start the restarted node one application index behind | The observer sees an extra delivery in each of the three recovery cases, even when the mock would have skipped execution. |
| Store an incorrect vote and commit, then separately store a wrong command | The crash test fails the full `HardState` comparison in the first case and the replayed entry comparison in the second. Both fixtures reach the committed-process marker. |
| Release a follower's confirmed read before local application | The registration has already been removed at the post-Ready fence, so registering the same context does not throw. |
| Let a leader answer using only its local commit | The isolated former leader emits a `ReadState`; both Ready modes fail the empty-read-result check. |
| Change the approved term in `01-singleton.txt`, with the old rewrite flag set | Only that golden fixture fails, reporting the case and line. The file is not rewritten. |
| Make a status probe ignore cancellation, or make the crash child hang | The status wait fails at its own deadline and reports all nodes. The crash launcher times out, captures both streams, and reaps the child. |
