# 5. Concurrency and the Raft test laboratory

This chapter is for studying the implementation, not just running the test
command. Learn to identify which layer is racing, what each test observes,
and what would count as a counterexample. Our lab tests are inspired by
MIT 6.824's approach to election, agreement, unreliable links, and recovery;
they do not run the Go course grader or use its API and performance limits.

## Do not confuse two meanings of concurrency

1. **One node, concurrent callers.** Many .NET tasks call `ProposeAsync`,
   `StepAsync`, `Tick`, `WaitForReadyAsync`, or `StopAsync`. The facade must
   decide which calls are accepted and complete each caller correctly.
2. **Several nodes, asynchronous events.** One simulated thread can exercise
   a whole distributed cluster. Choose which message arrives, which node
   ticks, which disk worker runs, and which host crashes next. These decisions
   create interleavings even though the simulation itself has no task races.

The first is about serializing mutations within a node; the second is about
coordination across nodes that may see different histories. Neither suite
substitutes for the other. The example's threaded three-node integration
tests join those concerns with a real application and durable stores.

## The .NET owner-loop contract

In [`RaftNode`](../../src/DotnetRaft/RaftNode.cs), follow `QueueOperation`, the
owner loop, and `TryClaimForDispatch` before reading core Raft rules again.
The facade owns the command, blocked-proposal, and tick lanes; only the owner
calls `RawNode`. Messages and mutable input buffers are detached before
queueing. Returning from a submission does not mean a client write is
committed or applied.

The crucial race is not "which task was started first" but which event won
the claim:

```text
waiting -- cancellation wins --> no dispatch
waiting -- owner claims -------> executes exactly once
claimed -- caller cancels -----> already dispatched; operation result wins
stop/fault --------------------> unclaimed work must finish with an error
```

Use the [claim and cancellation tests](../../tests/DotnetRaft.Tests/Node/RaftNodeCommandTests.cs),
[Ready tests](../../tests/DotnetRaft.Tests/Node/RaftNodeReadyTests.cs), and
[lifecycle tests](../../tests/DotnetRaft.Tests/Node/RaftNodeLifecycleTests.cs).
The tests pause the owner in a trace callback and explicitly coordinate
entry, release, and exit. They do not infer which transition won by sleeping
for a guessed duration. Timeout guards detect stuck tests, not slow Raft
elections. See [ADR 0005](../adr/0005-deterministic-raftnode-concurrency-verification.md).

Try this experiment: predict whether a queued high-term heartbeat should be
able to change the node's term when cancellation wins before dispatch. Then
run `CancellationBeforeClaimPreventsDispatch`. Contrast its ordered events
with `ClaimWinsOverLaterCancellation`. Both issue cancellation; only one
does so before the linearization point of the local queue claim. This is a
local API guarantee, not a proof of KV linearizability.

## What matches the 6.824 style

| Course-style question | Local executable evidence |
|---|---|
| Does a healthy group elect and agree? | Core election and replication tests plus the pinned interaction scripts. |
| Can a disconnected leader still choose a new entry? | `MinorityCannotCommitAndItsOldLeaderRejoins`: 3 and 5 voters, both storage modes. The old leader retains a minority-only suffix, the majority elects and commits, and healing makes the old leader converge. |
| Does a new process respect what survived the crash? | `CrashAtStorageBoundaryReplaysOnlyUnappliedWorkAndRejoins`: restart from durable consensus and physical application state, exercise replay and the asynchronous commit-only gap, and continue proposing with the surviving quorum. |
| What if RPCs are lost, delivered out of order, or duplicated? | 32 fixed seeds, 200 choices each, in `RandomizedInteractionPropertyTests`. Incoming network delivery, ticks, commands, Ready, local workers, and crash/restart can interleave. |
| Do equal indexes actually apply equal commands? | `AppliedLogOracle` observes term, index, type, and bytes when the mock application applies an entry. It checks contiguous physical application per node and equality at the same index across nodes and restarts. |
| Can an apparently successful concurrent KV history be wrong? | `ConcurrentClientsProduceALinearizableSingleKeyHistory` checks completed reads, puts, and deletes against a register model, respecting real-time precedence. |
| Does recovery also work with real processes and files? | The durable KV smoke kills and restarts loopback gRPC processes, verifies snapshot catch-up and failover, and reopens the entire SQLite-backed cluster. SQLite contract tests also isolate publication and recovery. |

### Read the simulator as a lab grader

Start in [`RaftLabTests.cs`](../../tests/DotnetRaft.Tests/Interaction/RaftLabTests.cs)
and [`InteractionEnvironment.cs`](../../tests/DotnetRaft.Tests/Interaction/InteractionEnvironment.cs).
The environment holds real `RawNode` instances. `ProcessReady`,
`ProcessAppendThread`, and `ProcessApplyThread` drive the standard host work.
The tests use memory as **mock durable** storage: it survives replacing a
node object, but this models publication boundaries, not physical disk or
power loss. A crash discards volatile work and messages for the old instance;
the new instance starts from the stable store and the recovered physical
application prefix. Local worker queues keep their FIFO contract; only
network messages are selected for out-of-order delivery, loss, or duplication.

A split is a link predicate, not an instruction to switch the stopped
leader's `Role`. This matters. The minority case deliberately lets the old
leader accept and persist an uncommitted proposal, then observes that it
cannot **apply** it without a quorum. After the new leader commits and the
network heals, the lab compares application output on every node. In the
five-voter run, the old leader can still replicate to one other node; two
copies are not three. A test that simply asserted the old leader demoted
would miss the most important question.

To understand the crash test, sketch these two schedules first:

1. The entry and commit are persisted, but the application worker has not
   applied the entry. Restart owes the application exactly that suffix.
2. The async apply worker published an already committed stable entry before
   the commit-only append ran. Restart sees a trusted physical prefix ahead
   of durable commit. The mock host repairs that commit boundary from the
   retained entry and does not apply the command twice.

Observe what `AppliedLogOracle` checks in each schedule. A test that compared
only the final log indices could pass even if the new process had applied
different command bytes or executed a command twice. This oracle starts
from a shared snapshot and observes subsequent applied entries; it is not
a general checker for arbitrary snapshot or backup contents.

### Deterministic randomness is a debugging tool

Open [`RandomizedInteractionPropertyTests.cs`](../../tests/DotnetRaft.Tests/Release/RandomizedInteractionPropertyTests.cs).
Each seed drives a fixed pseudorandom sequence of actions against a three-voter
cluster (16 runs per storage mode). The test records the leader ID observed
for each term at action boundaries, checks cursor and replication-progress
invariants, and independently compares commands crossing the application
boundary. Within a draining action, not every intermediate state is sampled.
The separate lab cases assert progress after healing; a randomly unlucky
period without useful delivery does not by itself prove a liveness defect.

When a run fails, keep the storage mode, seed, first failing action number,
and queued message state from the exception. Replay that seed, then reduce
the sequence to the shortest trace that still breaks the invariant. For a
stable regression, encode the reduced sequence in an ordinary test rather
than replacing it with a much larger random loop. The
[pinned corpus](../parity-matrix.md#corpus-integrity) is complementary: its
28 files contain 558 checked commands/input cases with expected output,
not 558 independent random executions.

## From agreement to a client history checker

Agreement on applied entries is necessary but does not check the moment at
which an application answers a read. If a `PUT x=blue` has already returned
before a later `GET x` is called, the read may not return an older value
unless another legal operation accounts for it. If the two calls overlap,
either order can be legal; requiring the latest value in every overlap would
reject correct behavior. Likewise, checking only the final value can miss a
bad intermediate response.

The [register checker](../../tests/DotnetRaft.Tests/Examples/RegisterHistoryTests.cs)
is a bounded teaching implementation. It records an invocation and completion
order for each completed operation. Its sequential model starts with one
absent key: `Put` changes the value, `Delete` returns it to absence, and `Get`
must match the value at that point. The search considers only operations whose
real-time predecessors have already been placed, and memoizes failed search
states by the remaining operations and current value. It accepts if **some**
legal ordering explains all observations. The checker has its own negative
test for a stale non-overlapping read and positive examples for overlapping
reads.

The [KV integration test](../../tests/DotnetRaft.Tests/Examples/KvClusterIntegrationTests.cs)
drives the actual host with SQLite and an in-memory transport. Each round
releases a writer, reader, and deleter through a common gate and records the
actual order; it does not rely on a particular race winner. The checker is
deliberately limited to one key and completed operations with healthy
connectivity. A fault-tolerant history checker would also need to model
ambiguous outcomes: a timed-out write may have taken effect, and skipping
such operations blindly can make a correct execution look impossible or
hide a real error.

## Run and read the results

```bash
# Protocol lab, randomized schedules, and concurrent client histories.
dotnet test tests/DotnetRaft.Tests -c Debug --no-restore \
  --filter 'FullyQualifiedName~RaftLabTests|FullyQualifiedName~RandomizedInteractionPropertyTests|FullyQualifiedName~RegisterHistoryTests|FullyQualifiedName~ConcurrentClientsProduceALinearizableSingleKeyHistory'

# Real owner-loop race cases.
dotnet test tests/DotnetRaft.Tests -c Debug --no-restore \
  --filter 'FullyQualifiedName~DotnetRaft.Tests.Node'

# Durable storage and actual multi-process failover.
dotnet test tests/DotnetRaft.Sqlite.Tests -c Release --no-restore
dotnet build examples/DotnetRaft.KvCluster -c Release --no-restore
./eng/smoke-durable-kv-cluster.sh "$PWD"
```

The smoke needs the local tools and ports listed in the
[example README](../../examples/DotnetRaft.KvCluster/README.md#automated-real-process-smoke).
It is a deliberately bounded scenario, not long-running chaos or a
replacement for replayable protocol tests. The snapshot and membership tests
remain separate from the fixed-membership KV workload.

### What a green test run cannot establish

It does not exhaust all message orders, certify the implementation against
the MIT grader, prove wall-clock availability, validate independent database
backups, or prove that arbitrary client histories across arbitrary real
failures are linearizable. Scope matters: an exact deterministic regression
can be stronger evidence for one race than thousands of uncontrolled runs,
and a process smoke supplies evidence that an in-memory oracle cannot.

Exercises for extending the lab:

1. Add an unreliable schedule that delays a reply until after leader
   replacement. Choose an assertion on physical application rather than a
   `Role` snapshot; explain which stale message should be harmless and why.
2. Extend the oracle to validate a snapshot's application prefix before
   skipping covered entries, then rehearse compaction and lagging follower
   catch-up. Do not equate "received snapshot" with "applied snapshot".
3. If you expand the history checker to faults, record stable request IDs,
   starts, returns, and uncertain results. Decide what a timeout means in
   the model before adding any more randomized actions.

Return to the [main learning guide](../raft-learning-guide.md) for the manual
lab and capstone exercises.
