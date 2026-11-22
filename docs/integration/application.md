# Apply entries and finish client requests

Part of [host integration](README.md). Raft orders commands and provides read
barriers; only the application knows when the effects and results exist.

## Call completion and client results

| Event | What it establishes |
|---|---|
| `Propose` or `ProposeAsync` returns | Submission was processed, possibly by forwarding. It does not prove commit or application. A dropped proposal can raise `ProposalDroppedException`. |
| A committed entry is delivered | The entry is selected for ordered application. In synchronous mode it is in `Ready.CommittedEntries`; in async mode it is in `MsgStorageApply.Entries`. |
| Your state machine finishes applying the entry | The result exists locally. Acknowledge the matching client only when the application's durability and retry policy permits it. |
| `ReadIndex` or `ReadIndexAsync` returns | Submission was processed. The index, if confirmed, comes later in `Ready.ReadStates`. |

An API cancellation token cannot retract a dispatched proposal or read.
Correlate attempts and results at the host. For safely retried writes,
persist the request identity alongside its outcome; the
[durable KV host](../internals/kv-host.md#mutation-application-and-deduplication)
shows how to avoid applying the same mutation twice across a restart.

## Application progress

`BasicStatus.Applied` is the **Raft acknowledgement cursor**. The host's
`physicalApplied` is the last position of the contiguous prefix actually
represented by application state. A newer restored snapshot advances that prefix;
an applied no-op also advances it even though it does not change data.
Persist a durable application's cursor and its `ConfState` consistently
with the effects at that position.

| Storage protocol | What the acknowledgement cursor can tell you |
|---|---|
| Synchronous | `Advance` may acknowledge irrevocable ordered admission, so Raft can be ahead of physical work. It can also lag work done before `Advance`. |
| Asynchronous | The node advances on a worker response after physical application or restore. It can still lag completed physical work. |

Use physical progress for client read barriers, [snapshots and compaction](recovery.md#snapshots-and-compaction),
and [restart](recovery.md#restart). Do not seed restart from a cached status.

## Configuration entries

For each committed `EntryConfChange` or `EntryConfChangeV2`, make the same
decision on every replica at that log position, based only on replicated
state, never local health, timing, or transport observations:

- **Accept:** call `ApplyConfChange` or await `ApplyConfChangeAsync` before
  acknowledging application. Record the returned `ConfState` with the
  application index.
- **Reject:** treat the entry as an application no-op. Advance the physical
  index when processed, but do not call the configuration apply method.

Bootstrap entries are initialization records and cannot be rejected.
`Start` preinstalls the complete initial voter set, so during a partial
bootstrap the configuration method can report all peers even though the
application has processed only an initial subset. Persist membership for
that exact bootstrap prefix, or recover bootstrap as a whole; do not tag a
partial snapshot with the full set. See the
[fixed-membership example](../internals/kv-host.md#configuration-and-no-op-entries).

A failure after dispatching configuration application faults the instance.
Use [fault recovery](recovery.md#fault-handling) instead of retrying the
change on possibly partially mutated state.

## Read index

1. Register a fresh, immutable context **for each attempt**, before calling
   `ReadIndex` or `ReadIndexAsync`. For example, encode a new `Guid` as bytes.
   Never reuse the context after a timeout, completion, or restart, because
   an old response could then match a different request.
2. Correlate `ReadState.RequestContext` from `Ready.ReadStates` to the
   pending attempt. Wait for any required snapshot restoration and for
   `physicalApplied >= ReadState.Index`. Defer this wait away from the Ready
   consumer or worker that has to advance application to the barrier.
3. Check the application index and read the value under the same application
   synchronization or transaction boundary.

A follower with no known leader can ignore a submission. Leadership changes
can interrupt confirmation. Bound the wait, remove expired attempts, and do
not substitute a stale local value for a timed-out linearizable read.

Every leader first commits an entry in its own term, even if it is the only
voter. A sole local voter can then confirm without a heartbeat round. A safe
read with multiple voters confirms against the applicable quorums; a lease
read also requires `CheckQuorum` and a host timing model with bounded clock
drift. If you cannot justify the lease assumptions, use the default safe
mode. Neither a lease nor a single voter skips the physical read barrier.
The [read walkthrough](../learning/04-membership-reads-and-liveness.md#derive-readindex-from-two-separate-conditions)
shows why the commit gate and authority check solve different problems.
