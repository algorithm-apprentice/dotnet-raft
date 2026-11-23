# Replicated counter

This console program runs three Raft voters in one process. It starts a
manually chosen campaign, proposes one `increment` command, and verifies that
all three replicas apply it. It uses only the public `DotnetRaft` API, with
no sockets, timers, database, or threads for the local storage workers.

| The library does this | The example host does this |
|---|---|
| Elects a leader and decides when the log entry is committed by a quorum | Drives each `RawNode` from one loop and carries messages between peers |
| Describes work that is ready to save, send, or apply | Uses a `MemoryStorage` per peer and acknowledges completed work |
| Provides the ordered, committed command bytes | Interprets `increment` and updates each replica's counter |

`Propose` returning is not write success. The program checks that it has
not applied the command immediately after proposing it, and checks the
value and applied index on every replica after processing the work.

## Read the example as a workflow

The lesson is **submission is not commitment, and commitment is not application**.
Start at `Program.Main`: `InitializeCluster`, `ElectLeader`, `SubmitIncrement`,
and `VerifyAndPrintReplicas` name the scenario's phases. The optional
`ObserveCommitBeforeApplication` phase pauses application without pausing
replication. Choosing a node to campaign does not appoint it leader.

| Source | Responsibility | What to look for |
|---|---|---|
| [`Program.cs`](Program.cs) | Arrange one write and check its result | The counter stays zero on submission and becomes one on application. |
| [`CounterCluster.cs`](CounterCluster.cs), `ProcessSynchronousReady` | Execute the host contract | Store the batch, enqueue messages, apply committed entries, acknowledge Ready. |
| `CounterCluster.RunOneRound` | Simulate scheduling | Process each replica's work and then deliver one message. No stage is skipped because an earlier stage made progress. |
| `CounterReplica.Apply` | Interpret committed commands | Bootstrap and no-op entries advance the index; only `increment` changes `Count`. |
| `CounterCluster.DispatchAsyncStorageWork` | Route the alternate storage protocol | Worker requests complete through their responses, never through `Advance`. |
| [`CounterWriteTrace.cs`](CounterWriteTrace.cs) | Observe the write | Trace callbacks do not drive or mutate the node. |

`CounterCluster` is the demonstration host, not a central authority in Raft.
Each replica owns an independent `RawNode`, `MemoryStorage`, and counter.
`Drain` runs until no enabled stage can progress; it does not itself decide
that a proposal is committed. All calls are serialized by this one host loop.

## Ready and Advance

From the repository root, with the .NET 10 SDK:

```bash
dotnet run --project examples/DotnetRaft.Counter
```

Read [`Program.cs`](Program.cs) for the scenario and `ProcessReady` in
[`CounterCluster.cs`](CounterCluster.cs) for the host loop. For each `Ready`,
the host writes its log and hard state to memory before enqueuing outgoing
messages, applies committed entries in order, then advances that exact batch.
Bootstrap configuration entries and the leader's no-op occupy log indexes
but do not change the counter. The run ends with `counter=1` on all three nodes.

## Asynchronous storage writes

```bash
dotnet run --project examples/DotnetRaft.Counter -- --async-storage
```

Only the storage protocol changes; the nodes still use synchronous `RawNode`
calls. Each replica has two FIFO work queues. `ProcessReady` routes
`MsgStorageAppend` and `MsgStorageApply` by `Message.To` instead of processing
the diagnostic persistence/application fields on `Ready`. The append worker
writes the entries and hard state; the apply worker applies committed entries.
Neither path calls `Advance`. Each worker releases the generated responses in
order, and the pump calls `Step` on each response's own destination. A single
append request can release both a local acknowledgement and a reply to a
remote Raft peer. This process simulates both using one delivery queue; with
real networking, only the remote replies go on the wire.

This run deliberately pauses the apply queues after proposing. Append and
network work can still advance the leader's commit index, but the counter
remains zero. Resuming the apply queues gives the same final result as the
first run. The queues are driven by one deterministic event loop so the
difference is visible without thread timing. `AsyncStorageWrites` is a
work/completion protocol, not a claim that `MemoryStorage` performs async I/O.

```text
Mode: asynchronous storage writes
Elected node 1 in term 2.
Proposed increment: commit=4, counter=0.
Committed at index 5: counter=0 (apply workers paused).
Node 1: committed=5, applied=5, counter=1.
Node 2: committed=5, applied=5, counter=1.
Node 3: committed=5, applied=5, counter=1.
```

## Trace one write

Add `--trace` to either mode to follow just the `increment`, not the
bootstrap and election:

```bash
dotnet run --project examples/DotnetRaft.Counter -- --trace
dotnet run --project examples/DotnetRaft.Counter -- --async-storage --trace
```

Read `core` lines as state transitions or message observations, and `host`
lines as work done by the example. These selected synchronous lines show
what to look for (intermediate lines are omitted):

```text
  core node 1: queued MsgAppResp 1->1 for index 5
  host node 1: stored entry 5 (Ready)
  core node 1: received MsgAppResp 1->1 for index 5
  host node 2: stored entry 5 (Ready)
  core node 1: received MsgAppResp 2->1 for index 5
  core node 1: Leader advanced Commit to 5
  host node 1: received application work for index 5 (Ready.CommittedEntries)
  host node 1: stored HardState.Commit=5 (Ready)
  host node 1: applied entry 5: counter=1
  host node 1: acknowledged application (Advance): Raft Applied=5
```

Predict which response will complete the quorum in this schedule, then check
the first leader commit line. The leader's own response only counts when
processed after the storage write. In async mode, look separately for the
local `MsgStorageAppendResp` that marks entries stable, the `MsgAppResp`
that counts toward replication, and the `MsgStorageApplyResp` that
acknowledges application. Find the point where the apply workers are paused:
the host may already have received apply work although none of the counters
has changed. This is a deterministic schedule using memory, not a claim
about disk durability or the order every cluster will exhibit.

Continue with the [source walkthrough](../../docs/internals/README.md#follow-a-write)
to find the methods that produced these lines.

## What to use beyond this demo

The playback uses volatile memory, fixed membership, and no snapshots or
restarts. The write ordering illustrates the protocol, not crash-safe
publication. To integrate real storage, read the [Ready contract](../../docs/integration/ready.md)
and [SQLite package guide](../../src/DotnetRaft.Sqlite/README.md); do not copy
the separate in-memory writes as a durable transaction. The
[durable KV cluster](../DotnetRaft.KvCluster/README.md) is the next example
for a real transport, `RaftNode`, linearizable reads, and recovery.
The [learning path](../../docs/learning/README.md) explains why the commit and
application observations in this run are different.
