# 4. Membership, linearizable reads, and liveness

Fixed-membership replication is not the whole problem. This chapter explains
how to change the voters without losing quorum intersection, how to read
without putting each read in the log, and which extensions improve progress
without replacing the safety proof. The runnable KV host deliberately has
fixed membership; dynamic membership is exercised by the library tests.

## Why a direct switch between voter sets is unsafe

Imagine replacing `{1,2,3}` with `{3,4,5}`. Each set has an internal majority
of two. But `{1,2}` is an old majority and `{4,5}` is a new majority, with
no common voter. If different nodes simply changed a local settings file,
two disjoint partitions could each think they could elect a leader or
commit writes. Having at least one node in the intersection of the
**configurations** is not enough: the two **majorities** might not include it.

Joint consensus uses a transition state `C_old + C_new`. While joint, an
operation needing a quorum must win in **both** sets, not just in their
union. For example:

| Responding voters | Old majority? | New majority? | Joint quorum? |
|---|---|---|---|
| `{1,2,4}` | Yes | No | No |
| `{2,3,4}` | Yes | Yes | Yes |
| `{3,4,5}` | No | Yes | No |

Voter 3 can count in both sets, but cannot count twice within either set.
For commit, compute the majority-replicated index of each side and take
the **minimum**, not the maximum. With matches
`{1:10, 2:9, 3:5, 4:12, 5:8}`, old permits 9 and new permits 8, so the
joint candidate commit index is 8. The leader's current-term commit test
still applies. Consult [`JointConfig`](../../src/DotnetRaft/Quorum/JointConfig.cs)
and [`MajorityConfig`](../../src/DotnetRaft/Quorum/MajorityConfig.cs). When
there is no outgoing set, its empty quorum is neutral: it does not lower
the commit result or veto an election.

### Walk the membership entry through the implementation

1. A membership proposal is a **log entry**, not an administrative edit to
   the tracker's in-memory dictionary. The leader admits at most one
   unapplied change under the normal validation rules. Track the pending
   index in [`ProposalAdmission`](../../src/DotnetRaft/Core/ProposalAdmission.cs).
2. The node replicates the change and learns that it is committed. A host
   allowed to accept or reject a change makes that decision deterministically
   at the same application position on every replica. Applying an accepted
   change updates the tracker; seeing it merely in the unstable log does not.
3. [`ConfigurationChanger`](../../src/DotnetRaft/ConfChange/ConfigurationChanger.cs)
   builds and validates the proposed next config and progress map on copies.
   A simple path can change at most one voter; a joint transition preserves
   the old voting set as the outgoing side. The core installs the result and
   reevaluates consequences such as commit, read quorums, transfers, or removal.
4. Leaving joint consensus is itself an ordered configuration entry. In the
   implicit form the leader proposes an automatic exit after the joint entry
   is applied; it does not edit the joint state in place to skip a log step.
   After an explicit exit the application of that entry is what discards the
   outgoing set. A temporarily blocked proposal may have to be retried.
5. Snapshots carry a `ConfState` for their actual application index. On
   restore, [`ConfigurationRestore`](../../src/DotnetRaft/ConfChange/ConfigurationRestore.cs)
   reconstructs tracking state rather than treating the old list of runtime
   connections as the current voters.

Why is a demoted voter sometimes in `LearnersNext`? During joint consensus a
node can still be required as an **outgoing voter**, even though the final
configuration wants it to be a learner. If it became an active learner
immediately, the outgoing quorum could lose a vote before the log established
the transition. The intermediate field represents an obligation at a
different time, not an extra vote. Read `MakeLearner` and `LeaveJoint` in the
changer. An ordinary learner also receives replication and snapshots; being
caught up does not itself make it a voter.

There are now two reasons the snapshot case from
[chapter 3](03-ready-and-recovery.md) matters: recovering membership as of
the application index, and keeping a delivered older membership transition
legal while consensus is already using the newer snapshot. Applying `LeaveJoint`
twice or applying it against the wrong generation is not the same as a
valid log replay from its own preceding configuration.

## Why a leader cannot just read its local state

Suppose A was leader through index 20. A becomes isolated but does not learn
that fact immediately. B and C elect B, commit `color = red` at 21, and B
returns success to the writer. A later client asking A for `color` must not
receive the old value as a *linearizable* read. The account balance example
from chapter 1 has the same problem: the client's read cannot be placed
before a completed write that precedes the read's invocation in real time.

There are several ways to engineer a solution:

- **Log the read** as an ordered operation. Conceptually simple and safe once
  applied, but adds replication and log traffic to reads.
- **ReadIndex**: confirm current leadership and learn a safe commit index,
  then read state that has physically applied through that index. No new log
  entry is required for every read.
- **Lease-based read**: avoid a fresh quorum exchange on some reads only when
  the host can justify the additional clock and timing assumptions.
- **Explicit stale read**: return local state without making a linearizability
  promise. This is a different API, as `/local` in the example demonstrates.

### Derive ReadIndex from two separate conditions

The safe-read path has two consensus questions. The first applies even when
the local leader is the sole voter:

1. **Do I know the committed prefix in this term?** A newly elected leader
   may have inherited entries that were committed before it was elected but
   not yet know the latest commit. It establishes a current-term commitment
   (often the no-op), then releases gated requests.
2. **Am I still allowed to act as leader for this read?** For a safe read,
   a multi-voter leader queues the request at the observed commit index and
   collects current-term heartbeat acknowledgements from its quorum. Without
   this exchange, the isolated old leader could answer the example above.
   A leader that is itself the only voter can skip this exchange once the
   current-term gate opens.

For example, the old leader commits its removal at index 2 and a write at
index 3 before applying the removal. The remaining voter has both entries
but only learns the commit through index 2 before it becomes leader. Its
election does not tell it the write was already committed. Committing the
current-term no-op at index 4 establishes a read barrier that includes the
write. Answering at index 2 could let a read miss a write that already
completed. This is the scenario covered by the
[single-voter regression](../../tests/DotnetRaft.Tests/RawNode/RawNodeReadIndexTests.cs).

Those conditions establish an index, not a value. A third, host-side
condition is `physicalApplied >= ReadState.Index`, after any required
snapshot restore. Even the leader's own application might be behind. For a
read forwarded through a follower, the follower may get the read barrier
before its own apply catches up. The read can wait without asking the
leader to repeat the write. A subsequent read does not need to return any
particular intermediate value if writes overlap it; it needs a value that
admits an order consistent with real time.

### Trace a read through this port

1. The caller supplies an opaque request context to
   [`RawNode.ReadIndex`](../../src/DotnetRaft/RawNode.cs) to correlate one
   request with its later `ReadState`; the KV example uses a GUID. Role
   routing can forward a follower's `MsgReadIndex` to its known leader.
   See the [host contract](../public-api.md#read-index) for timeouts and retries.
2. In [`RaftCore.HandleLeaderReadIndex`](../../src/DotnetRaft/Core/RaftCore.cs),
   follow the current-term gate before the local singleton fast path. Neither
   that fast path nor a lease-based reply bypasses the application's physical
   read barrier.
3. [`ReadIndexCoordinator`](../../src/DotnetRaft/Core/ReadIndexCoordinator.cs)
   and [`ReadOnlyTracker`](../../src/DotnetRaft/Read/ReadOnlyTracker.cs) track
   pending reads. The heartbeat context on this path is an internally
   encoded sequence position, not the client's opaque correlation bytes.
   Each peer's maximum acknowledged position covers all earlier positions;
   one quorum can release a prefix of reads efficiently. Joint config uses
   both majorities, and a learner cannot substitute for a voter.
4. The result is a local `ReadState` or a `MsgReadIndexResp` back to the
   requesting peer, which makes the local state. Follow
   [`RaftClusterHost.ReadAsync`](../../examples/DotnetRaft.KvCluster/RaftClusterHost.cs)
   and `CompleteRead`: the example defers a result until the matching
   application index can be read under the state-machine barrier.

Think about a heartbeat for read position 8 arriving before a heartbeat for
position 7. Acknowledging 8 implies eligibility for 7 from *that peer*;
it does not imply a quorum. This use of cumulative votes is analogous to
append `Match`, but it attests a read exchange, not more durable entries.

## Put availability features in their place

| Mechanism | Failure it addresses | What it does not prove |
|---|---|---|
| Randomized election timeout | Repeated simultaneous campaigns | Permission to elect without a voting majority |
| `PreVote` | An isolated node unnecessarily raising terms and disrupting an active leader on return | That a real vote can be forgotten or that connectivity will stay healthy |
| `CheckQuorum` | A leader continuing to accept work after losing recent quorum activity | That it still has a quorum at every instant, or a read is safe without additional reasoning |
| Lease read mode | Latency of a quorum exchange on every read | Safety under arbitrary clock drift or untimely scheduling |
| Leadership transfer | Coordinated handover to a particular caught-up eligible voter | An out-of-band instruction to treat the target as leader immediately |

`TickLeader` checks activity at election intervals when `CheckQuorum` is on.
`HandleCheckQuorum` must use the current voter configuration; an active
learner does not repair a lost voter majority. A role can still be stale
before the relevant ticks or messages arrive. The lease option requires
`CheckQuorum` and bounded clock behavior from the host; the library cannot
infer that assumption from a `CancellationToken` or nominal tick setting.
Prefer safe reads until those assumptions are explicit.

For transfer, follow `HandleLeaderTransfer`: identify an eligible target,
catch it up, and send `MsgTimeoutNow` when its acknowledged match reaches
the current last index. The transferee still starts a real election. The
leader rejects new proposals during transfer so the log does not keep moving
away from the target. A failed transfer must not disable writes forever;
this port also rechecks a pending automatic joint exit when a transfer times
out. That ordering is documented in
[ADR 0009](../adr/0009-auto-leave-retry-after-transfer-timeout.md).

## Experiments

```bash
dotnet test tests/DotnetRaft.Tests -c Debug --no-restore \
  --filter 'FullyQualifiedName~ConfigurationChangerTests|FullyQualifiedName~RaftCoreReadIndexQuorumTests|FullyQualifiedName~RaftCoreLeadershipTransferMembershipTests'
```

1. Read `JointConfigurationRequiresBothMajorities` in the
   [read-index tests](../../tests/DotnetRaft.Tests/Core/RaftCoreReadIndexQuorumTests.cs).
   Write down the acknowledgement set after each step; say which majority is
   still missing before inspecting the assertions.
2. In the [manual cluster lab](../raft-learning-guide.md#manual-laboratory),
   compare the stale local endpoint with the ReadIndex endpoint after losing
   a quorum. Explain why "cannot verify a fresh value" does not mean the
   local database must be empty or unavailable for inspection.
3. With old voters `{1,2,3}` and new voters `{3,4,5}`, determine the minimum
   set of active nodes needed during the transition. Can two particular node
   failures leave a joint config unable to make progress while *three* nodes
   remain? **Hint:** examine a partition that leaves `{1,2,4}` reachable.

Continue with [concurrency and the test laboratory](05-concurrency-and-testing.md).
