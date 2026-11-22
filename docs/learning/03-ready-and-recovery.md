# 3. Ready, persistence, and crash recovery

If [chapter 2](02-log-and-replication.md) explains what the algorithm chooses,
this chapter explains when the outside world is allowed to see the choice.
The [Ready contract](../integration/ready.md) specifies integration ordering.
The examples below explain why those boundaries exist, including why the
host's [physical application cursor](../integration/application.md#application-progress)
must be tracked separately.

## Separate a decision from its effects

The core can choose to grant a vote while executing one in-memory step. It
cannot make that vote survive a process crash. Similarly, it can decide that
entry 41 is committed without executing your key-value operation. The host
has to act on those decisions and acknowledge the effects. That boundary
gives `RawNode` a deterministic execution model and makes it possible to test
the same protocol against memory, SQLite, or a more elaborate storage engine.

Read a [`Ready`](../../src/DotnetRaft/Ready.cs) batch as a set of related
effects, not as a notification that they have all already happened:

| Work | Why the host sees it |
|---|---|
| New term, vote, commit | Record how this node resumes after restart. |
| New entries or incoming snapshot | Publish the next stable prefix. An in-progress write is still unstable from the core's point of view. |
| Messages | Deliver observable effects; a vote grant or append response must not attest data that will disappear after a crash. |
| Committed entries | Apply operations to the state machine in index order; commitment does not itself execute a command. |
| Read states | Identify application barriers, not ready-made values. |

Open [`RawNode.CreateReady`](../../src/DotnetRaft/RawNode.cs). Accepting a
batch drains the corresponding output queues, marks unstable work in
progress, and reserves the committed entries it is handing to the host. A
synchronous batch also keeps detached acknowledgement metadata. That metadata
is why mutating a returned protobuf cannot make `Advance` confirm a different
log entry. In synchronous mode, only one batch can be outstanding, but other
inputs can still change the core while that batch is being processed. Async
storage permits multiple batches' work to be in flight.

### Track the log and application positions on paper

Suppose a leader has appended through 46, knows that it committed through 44,
and has handed entries through 42 to its host. The last acknowledged index
is 40. The application database may have physically applied 41 already:

```text
Raft last index            46     tail may include uncommitted entries
Raft committed             44     chosen prefix
Raft applying              42     entries handed to the host
Raft applied (acknowledged) 40     acknowledgements processed by the core
application physical       41     effects the application can actually read
```

`Applied <= Applying <= Committed <= LastIndex` is a core invariant. The
physical application cursor belongs to the host. With synchronous early
admission, Raft `Applied` can instead be *ahead* of physical state, because
`Advance` can acknowledge irrevocably queued work. In asynchronous mode an
apply response follows physical completion (or snapshot supersession), so
the core can temporarily lag actual physical work, not claim completion
before it. Neither cursor can be guessed from `LastIndex`.

This is also an accounting mechanism. A handed-out batch consumes encoded
bytes; acknowledgements release exactly that batch's budget. Advancing a
cursor without releasing accounting could deadlock the apply pipeline;
releasing accounting without acknowledging an actual batch could allow
unbounded work to accumulate.

## Follow a vote and an append through the durability barrier

In a three-node term-7 election, B's in-memory `Vote` changes to A. B can
accept other inputs before the host finishes a write, but its grant is
queued after append. The host persists `HardState{Term:7, Vote:A}`, and
only then sends the grant. If B crashes before sending, A can retry; B's
durable vote still prevents a contradictory grant. If B crashes after
sending, the outcome on recovery is the same. Without that boundary the
election-safety proof in [chapter 1](01-safety-and-elections.md) does not hold.

Similarly, a follower accepting `(index=41, term=7)` can queue an append
acknowledgement while performing the deterministic in-memory step. The
receiver should not observe it before the follower's stable storage is able
to reproduce entry 41. The leader uses the same principle for its own copy:
`AppendLeaderEntries` queues a self-directed `MsgAppResp`. Appending to the
leader's RAM is not yet a durable vote for `Match`.

Do not confuse atomically publishing a batch with **forcing** it to durable
media. `RawNode.MustSync` is true for new entries or a term/vote change. A
commit-only change does not by itself require a forced flush; neither does
snapshot presence change the generic flag. A snapshot still has to meet its
storage engine's durability contract before effects that depend on it. The
SQLite package deliberately chooses `synchronous=FULL` rather than trying to
relax durability on individual calls.

## Two protocols for driving the same state machine

Synchronous processing has one acknowledgement, `Advance`. The node exposes
the complete Ready, the host handles it, and `Advance` uses the saved
metadata to confirm it. Read `RawNode.AdvanceCore` with the
[synchronous contract](../integration/ready.md#synchronous-ready-loop). `RaftNode`
delegates that work to `RawNode`; its background command loop is not a
substitute for the host's Ready consumer.

With `AsyncStorageWrites`, append and apply are independent FIFO workers.
The host can accept more Ready batches without waiting for an earlier local
write. To see the change, compare `CreateStorageAppendMessage` and
`CreateStorageApplyMessage` in `RawNode`:

```text
Ready.Messages (async)
    immediate protocol messages       -> transport
    MsgStorageAppend                   -> append worker
        nested network replies        -> their remote destination
        nested local append response  -> the same node
    MsgStorageApply                    -> application worker
        nested local apply response   -> the same node
```

The append worker need not wait for the apply worker simply to persist an
ordinary log suffix. A snapshot is different: restoration and physical apply
share a barrier because old application work must not overwrite the state
restored from a newer prefix. The apply worker can receive only committed
entries already acknowledged stable. In async mode there is no `Advance`;
processing the diagnostic entry/snapshot fields as well as the local worker
messages would process the work twice. Use the complete
[append-worker routing example](../integration/ready.md#append-worker) for actual
host code. In particular network replies in `request.Responses` do **not** go
back to the local node just because the outer request was local.

### Why `(index, entryTerm)` is not enough for an async append response

The response carries both a **request generation's Raft term** and the
acknowledged last entry's tuple. Consider this sequence of unstable tail
replacements while earlier append confirmations are delayed:

| After the leader change | Unstable entry at index 3 | Old outstanding confirmation |
|---|---|---|
| Raft term 1 | `(3, entryTerm=1)` | Append response with Raft term 1 will say `(3, 1)`. |
| Raft term 2 | `(3, entryTerm=2)` | Another response will say `(3, 2)`. |
| Raft term 3 | `(3, entryTerm=1)` | A new leader can bring back the earlier entry as part of its log. |

A delayed term-1 response would superficially match the unstable tuple in
term 3, but term 2's queued write might have replaced the durable copy since
the first response was generated. Clearing the unstable suffix on that
response would mistake an older successful write for the new write. This
is an ABA problem: the current value looks equal to an older value after
an intervening change.

`RaftCore.HandleStorageAppendResponse` checks the current response term for
entry stabilization, and `UnstableLog.StableTo` checks the entry tuple. Even
a later hard-state-only append request can provide current-term evidence for
the unstable tail because earlier append requests run in FIFO order.
Snapshot completion is different: it can acknowledge a committed snapshot
across a term change, but must not remove or reinstall a newer unstable
snapshot's configuration. See the
[ABA response tests](../../tests/DotnetRaft.Tests/AsyncStorage/AsyncStorageResponseTests.cs)
to step through the stale-acknowledgement execution in C#.

## Evaluate crashes at boundaries, not at arbitrary source lines

The runnable KV host has two durable stores. The Raft database owns the
stable log, hard state, snapshot, and pending snapshot marker. The application
database owns rows, deduplicated request results, physical applied index,
and membership at that index. They do not form one SQLite transaction. The
table also includes the commit-only gap a host can create with independent
async workers or non-forced writes. The KV example instead persists commit
with FULL synchronization before applying the batch.

| Where the process stops | What the next host can know | Safe next action |
|---|---|---|
| An append is still queued; no complete generation was published | In-memory log changes might be lost | Start from the old durable prefix; allow the leader to retry. |
| Entry and commit are durable, but application has not run | The node can recover entries and see that work remains | Start with the actual physical cursor and replay committed entries after it. |
| A different host applied a committed, stable entry before a commit-only write became durable | Trusted application state shows an applied prefix ahead of the recovered commit | Verify that the retained Raft prefix covers it; repair commit or roll the application back. Do not invent term or vote. |
| An incoming Raft snapshot is durable, but app restore has not finished | The pending marker identifies the exact incomplete cross-store operation | Restore application bytes, index, and membership together and acknowledge the marker. |
| App restore finished, but snapshot acknowledgement did not | A pending marker still exists, even though its effects may already be durable | Restore idempotently and then clear the marker. |
| One store is restored from an unrelated backup | The indexes and configuration may look plausible but the generations need not match | Do not assume crash recovery proves they are a valid pair; restore a coordinated set. |

Trace the concrete example in [`RaftClusterHost.ProcessReadyAsync`](../../examples/DotnetRaft.KvCluster/RaftClusterHost.cs),
[`SqliteStorage.PersistReady`](../../src/DotnetRaft.Sqlite/SqliteStorage.cs),
and [`DurableHostRecovery.Reconcile`](../../examples/DotnetRaft.KvCluster/DurableHostRecovery.cs).
Then compare the [SQLite package recovery guide](../../src/DotnetRaft.Sqlite/README.md#open-and-restart).
The example's process-kill tests check transaction/restart behavior; they do
not emulate a storage device losing power or prove that independent live
copies of two databases are compatible. On restart, handing Raft a newer
`Applied` does not update the membership supplied by storage. Work through
the [restart contract](../integration/recovery.md#restart) if the application has
already applied a configuration entry newer than the retained snapshot.

### A newer snapshot does not make an older Ready retroactively disappear

Suppose the host is applying a delivered configuration entry that *leaves*
joint consensus, and the node receives a newer snapshot that already reflects
that exit. Applying the old entry against the snapshot's configuration would
try to leave a configuration that is no longer joint. It could also produce
an incorrect membership for the older application cursor if it happened to
succeed. This implementation keeps the earlier delivered application's
configuration separately while the live tracker follows the newer snapshot;
both storage modes share the snapshot completion path. An obsolete completion
cannot install its old membership over a newer snapshot. Read the regression
`SnapshotAndEarlierApplicationKeepTheirOwnConfigurations` in
[`RawNodeReadyTests.cs`](../../tests/DotnetRaft.Tests/RawNode/RawNodeReadyTests.cs).

## Experiments

```bash
dotnet test tests/DotnetRaft.Tests -c Debug --no-restore \
  --filter 'FullyQualifiedName~RawNodePersistenceTests|FullyQualifiedName~AsyncStorageResponseTests|FullyQualifiedName~CrashAtStorageBoundaryReplaysOnlyUnappliedWorkAndRejoins'

dotnet test tests/DotnetRaft.Sqlite.Tests -c Debug --no-restore \
  --filter 'FullyQualifiedName~SqliteStorageRawNodeTests|FullyQualifiedName~AbruptProcessTerminationRecoversAtomically'
```

Run the crash-boundary lab in both orders: persistence before application,
and asynchronous application before the commit-only write. Put a breakpoint
at `InteractionEnvironment.RestartNode` and inspect the persisted term,
commit, and recovered physical cursor. Predict whether the first Ready will
contain the command again. You should be able to explain why skipping replay
when it is needed and replaying application effects twice are both wrong.

Continue with [membership, reads, and liveness](04-membership-reads-and-liveness.md).
