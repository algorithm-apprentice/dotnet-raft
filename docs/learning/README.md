# Learning Raft with dotnet-raft

Start with one command replicated by three voters. Work out why the same
mechanism still works when messages are lost, a leader changes, or a process
restarts. You do not need to know another implementation. Use the
[host integration contract](../integration/README.md) when adapting the code
to a real service.

## Start here

1. Use the [architecture and source guide](../internals/README.md) to locate
   the consensus, storage, and host boundaries. Run the
   [Counter write trace](../../examples/DotnetRaft.Counter/README.md#trace-one-write)
   and [follow the write in the source](../internals/README.md#follow-a-write).
   Start with Ready/Advance, then repeat with async storage to see why a
   proposal is not a commit and a commit is not application.
2. Work through the [implementation deep dives](#implementation-deep-dives)
   in order. Predict each example on paper before stepping through the code.
3. Run the [manual KV laboratory](#manual-laboratory) and use the
   [capstones](#capstone-exercises) to connect the traces to failure and
   recovery behavior.

<a name="2-prepare-the-repository-for-offline-use"></a>

## Prepare the repository for offline use

Install the .NET 10 SDK selected by [`global.json`](../../global.json). From the
repository root, restore once while online:

```bash
dotnet restore DotnetRaft.sln
dotnet test DotnetRaft.sln -c Debug --no-restore
```

The chapters' tests run without another restore. The example `dotnet run`
commands can also run offline after that restore: add `--no-restore` before
the `--` that separates application arguments.

## Implementation deep dives

Each chapter has worked examples, source links, focused test commands, and
exercises. Follow the sequence to learn the system, or jump to a question.

| Chapter | What to work through |
|---|---|
| [1. Safety and elections](01-safety-and-elections.md) | Derive quorum intersection, durable votes, leader completeness, and the current-term commit rule. |
| [2. Log and replication](02-log-and-replication.md) | Repair a divergent suffix by hand, then follow stable and unstable logs, follower progress, flow control, and snapshots. |
| [3. Ready and recovery](03-ready-and-recovery.md) | Trace persistence before acknowledgement, the separate application cursor, and async ABA protection. |
| [4. Membership, reads, and liveness](04-membership-reads-and-liveness.md) | Calculate joint quorums and follow a read from leader confirmation to physical application. |
| [5. Concurrency and testing](05-concurrency-and-testing.md) | Compare local call races and distributed interleavings, then check a concurrent KV history. |

<a name="8-manual-laboratory"></a>

## Manual laboratory

Use the [durable KV example README](../../examples/DotnetRaft.KvCluster/README.md)
for the startup commands and HTTP requests. Run only your own local demo
processes. To repeat the exercises from a clean state without touching an
existing demo, use a new `Raft:DataDirectory` for each node.

Follow these experiments in order:

1. **Observe the election.** Query `/status` on all three nodes. Wait until
   they agree on a nonzero `leaderId`. Compare term, commit, logical applied,
   and physical applied. These are observations at an instant, not a trace
   of which particular peers formed a quorum.
2. **Write through a follower.** Follow the example's
   [follower-write instructions](../../examples/DotnetRaft.KvCluster/README.md#write-and-read),
   choosing the actual nonleader's port. Save the request ID, exact command,
   returned `appliedIndex`, and the node that handled it. Compare a
   linearizable read with an explicitly local read on a different node.
3. **Verify durable deduplication.** Retry the same command with the same ID.
   Expect `duplicate: true` and the original result index. Reuse the ID for
   different content and expect HTTP 409. For an actual new write, use a
   fresh ID.
4. **Replace the leader.** Check `/status` again, then stop the current leader
   with `Ctrl-C`. Wait for the two surviving nodes to agree on a new leader.
   Write a different value with a fresh ID, restart the old leader with its
   original data directory, and verify that it serves the updated value once
   it catches up.
5. **Lose quorum and compare responses.** Run the example's
   [quorum-loss experiment](../../examples/DotnetRaft.KvCluster/README.md#observe-quorum-loss).
   Explain why a new write and a new `ReadIndex` read need a live quorum but
   a retry of an already applied request can return its stored result.
6. **Restart all three.** Stop the two processes left running by the
   quorum-loss experiment, then launch all three with their original
   arguments and data directories. Verify that both the latest values and
   an earlier request's duplicate result survive the restart; then confirm
   that a fresh write can still complete.

`Ctrl-C` exercises orderly shutdown. To see abrupt termination and snapshot
catch-up, run the example's
[real-process smoke](../../examples/DotnetRaft.KvCluster/README.md#automated-real-process-smoke).

<a name="13-capstone-exercises"></a>

## Capstone exercises

### A. Explain one committed write from evidence

First follow `CurrentTermRuleBlocksOlderEntriesUntilNoOpIsAcknowledged` in
[`RaftCoreReplicationTests`](../../tests/DotnetRaft.Tests/Core/RaftCoreReplicationTests.cs)
and `SingletonSelfAppendWaitsForAdvance` in
[`RawNodeReadyTests`](../../tests/DotnetRaft.Tests/RawNode/RawNodeReadyTests.cs).
Together they show how a quorum advances commitment and how an append
confirmation depends on the host's Ready processing. The singleton test
models persistence with memory; it is not a disk-durability measurement.

For the running KV host, record the term, leader, and commit from `/status`.
To identify the actual quorum and ordering for one write, also capture the
matching `MsgAppResp` messages (including the leader's self-acknowledgement),
the leader's commit advance, and the host's persistence and application
completion. Breakpoints or local instrumentation around `HandleAppendResponse`,
`PersistReady`, and `ApplyCommittedEntriesAsync` make those events visible. A
`MessageSent` trace is a core output event, not a transport delivery or a
disk completion. Do not infer those events from `/status` alone.

### B. Diagnose a stalled operation

For `[commit=20, physicalApplied=17]`, explain why a read barrier at 20 can
still be waiting even with a healthy quorum. Compare it with a leader that
has an uncommitted entry at 21 and only one reachable voter in a three-voter
cluster. Name which layer is stalled and which evidence would distinguish
the two cases.

### C. Design a new host

Draw the boundary between consensus, storage, transport, and application.
Describe who owns ticks and Ready, what must persist before a response,
how a successful client write is correlated to application, and what
recovery must reconstruct. Check that your design can survive a crash
between Raft snapshot persistence and application restoration. Compare it
with the [host integration contract](../integration/README.md) and trace the
same cut in [chapter 3](03-ready-and-recovery.md).

## Further reference

- The [implementation map](../internals/README.md) follows one request across
  code owners.
- The [maintainer test map](../maintaining/testing.md) helps select evidence
  when changing an invariant.
- The [decision index](../adr/README.md) records the trade-offs behind the
  architecture.
