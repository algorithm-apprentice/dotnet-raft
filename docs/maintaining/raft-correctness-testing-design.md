# Check Raft semantics and KV histories across faults

Status: reviewed and implemented. This adds evidence at two boundaries in the
[test map](testing.md). It does not change consensus, the package API,
Protobuf, SQLite formats, or the approved interaction corpus.

## Why two checks

The pinned interaction files have the same inputs as the referenced Go
implementation, but their approved output describes our own execution.
They detect changes from our baseline, not disagreements with upstream.
The healthy KV history checks completed operations through the real
application, but they cannot interpret a retried write whose outcome was
unknown. Neither limitation is addressed by adding more copies of a test.

## Compare observable Raft semantics with pinned Go

Use the `etcd-io/raft` revision in the interaction manifest
(`1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`) as a test-only Go module.
Pin its module and checksums and apply the same checked-in JSON schedules
to each implementation in both Ready modes where meaningful. The C# driver uses the
existing `InteractionEnvironment`, including typed work and production
receipts. The Go driver is a small serialized host for upstream `RawNode`:
publish Ready work in memory, route messages, apply entries, and return
the upstream acknowledgement. It does not implement any consensus rule.

The first schedules use explicit campaigns and heartbeat ticks, so they
do not compare randomized election timeouts. Their actions stay at the
host boundary (no injected terms, fabricated acknowledgements, or
implementation-specific hooks). At named checkpoints compare:

- term, leader, vote, committed index, physical application index, and
  the configuration at that application prefix;
- every entry newly applied on each node: index, term, entry type, and
  payload bytes, with per-node order preserved;
- the full application prefix bytes and the first retained log index;
- completed ReadIndex contexts and their indexes, including their
  requesting node. Normalize independent reads across nodes, not the
  order of entries in a node's applied log.

The first case elects a leader, writes and reads through a follower,
isolates the leader, commits on a new majority, tests an unconfirmed read
on the isolated node, and verifies a fresh read after healing. The second
keeps a prospective learner disconnected during its addition, compacts
through a known write, and then makes it catch up through a snapshot and
apply a suffix. Check observable milestones as well as equality: both
systems could otherwise agree because neither made progress. In the
snapshot case, the lagger's last index before healing is below the
sender's compacted boundary; require its own retained boundary to advance
and its application bytes and membership to match. The learner is never
compacted by the test itself.

A third, async-only case uses the same scheduler to stop while a singleton
append is pending, then while its durable reply is pending. In contrast to
the drained checkpoints, the action explicitly allows pending work there.
The log publication must advance while commit and application stay behind;
only delivery of the durable reply lets the command become visible. This
checks a publication boundary that a final converged state cannot prove.

Compare structured observations, not diagnostic strings, message batching,
timing, or pointer/presence details. Omit scenarios with known intentional
semantic differences rather than silently masking individual mismatches.
On disagreement report schedule, mode, checkpoint, and differing evidence.
A bounded drain protects against non-progress loops. Run the comparison in
CI with explicit Go setup; ordinary `dotnet test` must not require Go or
network access. A supported local runner executes both implementations
without regenerating any approved files.

## Check one durable KV register through faults and retries

Reuse the bounded register checker. Record each attempt's invocation,
return, observed result and, for writes/deletes, a stable request ID. The
model groups attempts for the same ID and command into one logical write:
its window starts at the first invocation and ends no later than the
first successful reply, including a duplicate reply. A timeout is not a
known completion of a write: if no attempt succeeds, the effect may occur
after the timeout or not at all. An unsuccessful read contributes no
observed value. Successful retries for an ID must report the same result
index. Distinct successful request IDs must report distinct result indexes,
and those indexes order the known writes even when their client calls overlap.
An unconfirmed write has no known index and still may take effect at any
position consistent with the observations. The model deliberately covers
identical-command retries on one key; same-ID conflicts remain separate
component/host tests. Its inputs are
client observations, not log/state internals. Unit tests of the checker
must reject stale reads, lost acknowledged writes, and reapplication of a
retried command after a newer write, while accepting legal late or absent
effects from an unconfirmed write.

Drive the real `RaftClusterHost` with SQLite and a controllable in-memory
peer transport, reusing the host integration harness and manual ticks.
Block delivery **to** a follower but allow its forwarded writes to reach
the majority. Start a write and a read through it; both must time out
locally. Obtain a successful quorum read of the write before any retry.
Advance the register and force both survivors to publish snapshots and
compact through the missing prefix. Stop/reopen the lagger on its own
databases, isolate the first leader and elect the other survivor with the
reopened follower. The restart clears the follower's remembered leader;
do not sleep to age a lease. Assert that the lagger really installed a
snapshot (a retained boundary beyond its offline durable log). After a
fresh majority write, a fresh read through the isolated old leader must
not succeed. Retry the first write through the restored follower with the
same ID. It must return the original index as a duplicate and must not
restore the superseded value. Heal and verify new reads/writes. Check the
recorded history end to end, with timeouts and phase markers available on
failure. The existing real-process smoke still checks sockets and abrupt
process exit; this case controls ordering, not power-loss behavior.

## Review and acceptance gates

- Keep expected milestones in the same checked-in schedules, but check them
  independently of the Go comparison. In the snapshot schedule both the
  offline last index and the later retained boundary are observed. In the
  read schedule expected read results include both their contexts and indexes.
- Keep retry grouping separate from the generic register search. The
  search can skip an unconfirmed logical mutation, but must not prune a
  legal ordering whose late effect explains a later observation. Unit-test
  dropped, late, duplicate, and lost-success outcomes independently. Test
  the returned write order separately: overlapping client calls may permit
  either order by time alone, but the original log indexes determine the
  order of successful writes.
- Use one announced transport rule at a time in the host case. No ticks
  advance the old leader while it is isolated. Restarting the lagger
  resets its election view, allowing the other node to campaign without a
  wall-clock guess; a manually delivered heartbeat drives the heal.
- Scope the reference check to shared semantics. Verify fresh read contexts
  and actual application observations; a transport delivery or equal index
  alone is not evidence the effect occurred.
- The lagger must lack the relevant prefix before the fault is lifted. Its
  local snapshot boundary must advance after healing, and no retry may
  precede the majority observation of the original write. Otherwise log
  replay or a newly accepted retry could make an incorrect test pass.
- Observe real timeout responses; a test guard expiring is a failure, not an
  ambiguous client outcome. Preserve the first successful reply as a hard
  upper bound even if later attempts time out. Do not collapse fresh writes
  with distinct request IDs just because their values match.
- As a negative control, compare same-shaped observations with a changed
  committed payload; the comparison must fail at its checkpoint. In the KV
  model, a timeout followed by an observed effect must be accepted; changing
  the later read to a stale value after an acknowledged overwrite must be
  rejected. Rerun the full Debug and Release suites, formatting, both-mode
  comparison, and the existing three-process smoke without approval edits.

## Verification results

- The live comparison matched 13 checkpoints across five runs: both Ready
  modes for failover/read and learner/snapshot catch-up, plus the async
  publication schedule. Changing just one applied payload in a temporary
  Go report failed at `failover-read/async/first-read` with unchanged indexes.
- The KV fault case passed five consecutive focused Release runs. Removing
  the request table from application snapshots in an isolated copy made the
  retry produce a fresh result index rather than a duplicate; the test failed
  at that assertion. The checker controls also reject a stale post-retry
  read and loss of an acknowledged write. Two review controls also showed
  that ignoring successful writes' indexes accepted both a duplicate index
  and an impossible ordering. The checker now rejects both.
- Debug and Release passed 1,252 tests each, including the earlier
  serialization-boundary fixes. Formatting, Go module integrity, and the
  existing real-process smoke passed. No golden, public API, wire, or schema
  approvals changed.
