# 2. How replication repairs a log

Start with [election safety](01-safety-and-elections.md). Here the question is
what a leader does with the authority it won: establish one matching prefix,
keep slow followers from blocking fast ones, and eventually compact what the
application has already consumed.

## The preceding entry is a proof obligation

An append carries `(previousIndex, previousTerm)`, then a contiguous sequence
of entries starting at `previousIndex + 1`. The sender's current term is a
separate field. A follower checks the preceding entry before accepting the
suffix. An empty `MsgApp` still makes this check.

Why is one tuple enough? Within a term there is only one leader, and that
leader does not write two different entries at the same index. The entry at
that tuple was first created with a specific prefix. A follower can acquire
it only after matching the preceding tuple or receiving a snapshot that
establishes the corresponding prefix. Induct backward: if two logs contain
the same `(index, term)`, they contain the same command at that index and
the same prefix through it. Raft does **not** compare entire logs in every
request, and it does not match entries by payload equality alone.

The follower's successful path is roughly:

```text
check term and previous tuple
for each received entry in index order:
    skip an already matching entry
    at the first differing entry, replace the uncommitted suffix
advance commit no further than entries this request established
acknowledge only after the required durable publication
```

[`LogSlice`](../../src/DotnetRaft/Core/LogSlice.cs) expresses the batch and
preceding tuple. `RaftLog.MaybeAppendEntries` and `FindConflict` do the log
work. `RaftCore.HandleAppendEntries` traces accepted entries, advances commit
to `min(message.Commit, lastNewIndex)`, and queues a `MsgAppResp`. A conflict
at or below the committed index is an invariant failure, not an instruction
to rewrite committed state. If an append's preceding index is already below
the follower's commit, this implementation acknowledges the committed prefix
instead of trying to rewrite the old region.

### Walk a divergence to a shared prefix

Assume the leader is in term 6. The follower's index 6 is its last entry.
Only entries through index 4 are common:

| Index | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Leader entry term | 1 | 1 | 2 | 2 | 4 | 4 | 6 |
| Follower entry term | 1 | 1 | 2 | 2 | 3 | 3 | — |

1. In `Probe` with `Next = 8`, the leader asks about preceding tuple `(7, 6)`.
   The follower cannot match it. Starting at `min(7, 6)`, it can report hint
   `(6, 3)`: index 6 is its last entry and its term is no greater than the
   requested preceding term.
2. The leader uses `FindConflictByTerm(6, 3)` locally. Its terms at indexes 6
   and 5 are both 4, so it can skip them as a run. At index 4 its term is 2.
   For a current probe rejection and `Match <= 4`, the next send begins at
   5. Hints are conservative hints, not proof of a match.
3. This time the preceding tuple `(4, 2)` matches. The follower can replace
   indices 5 onward with the leader's suffix and acknowledge index 7 after
   durable publication. If the leader advertised commit 6, the follower will
   not apply index 7 merely because it was in the same RPC.

The two searches avoid a round trip for every entry in a divergent term.
Compare `HandleAppendEntries`, `HandleAppendResponse`, and
[`Progress.MaybeDecrementTo`](../../src/DotnetRaft/Tracker/Progress.cs). A
delayed rejection for an old probe cannot roll a newer `Next` back arbitrarily;
in replicate mode a genuine rejection returns to probing from `Match + 1`.

## Stable storage and the unstable overlay

The implementation does not require the application to make a SQLite write
every time it calls `Step`. It needs one **logical** log that includes work
not yet confirmed durable. Its two sources are:

| Component | Owns | Does not own |
|---|---|---|
| [`IStorage`](../../src/DotnetRaft/Storage/IStorage.cs) | The stable view of entries, term lookup, bounds, snapshot, and initial hard state/configuration | The protocol decision to commit or overwrite an entry |
| [`UnstableLog`](../../src/DotnetRaft/Core/UnstableLog.cs) | Entries and snapshots awaiting confirmation, plus in-progress boundaries | External disk writes or acknowledgements |
| [`RaftLog`](../../src/DotnetRaft/Core/RaftLog.cs) | Bounds, merged reads, log matching, commit, and application cursors | Connection lifetimes, database transactions, and state-machine code |

For example, stable storage may hold indexes 41 through 60. A valid new
leader replaces the **uncommitted** range starting at 57 with three new
entries. Before the new suffix has been saved, [`RaftLog`](../../src/DotnetRaft/Core/RaftLog.cs)
reads 41–56 from stable storage and 57–59 from the unstable suffix. Logical
last index is 59; the physically retained old index 60 is hidden. Saving the
new suffix eventually truncates the old durable tail too. An in-progress
write is not the same as a confirmed stable write.

For a read across the seam, `RaftLog.Slice` reads the stable part first and
only continues into the unstable part if the stable prefix did not fill the
page. The byte limit is over serialized entries. A nonempty page returns at
least its first entry so a single large entry cannot stall replication
forever; an already nonempty page does not get to add an oversized second
entry just because the next source also promises one. SQLite checks the
stored payload length before reading or decoding BLOBs beyond its page.
`StorageError.Compacted` can mean a snapshot is needed, whereas unexpected
unavailability inside the requested stable range is a consistency problem.

### Read half-open intervals out loud

Suppose a snapshot covers index 40, and 41 is the first entry available to
read. `[41, 44)` means entries 41, 42, and 43. The backend still needs the
term at index **40**: to append starting at 41, the sender asks the follower to
match `(40, snapshotTerm)`. The memory and SQLite stores retain a dummy
boundary entry for this purpose. It is not a new command to apply. Dropping
that term during compaction would make it impossible to check the first
entry after the compacted prefix in an ordinary append.

## Replication has two distinct types of knowledge

The leader maintains [`Progress`](../../src/DotnetRaft/Tracker/Progress.cs)
per follower and learner:

- `Match` is the highest index the leader knows this peer has acknowledged.
  Within that progress record, retries and rejections must not reduce it.
- `Next` is where the next append will start. It may move forward *on send*
  and backward *on rejection*. It is a scheduling prediction, never a vote
  for commitment. `Match < Next` is a useful local invariant.

| State | What the leader knows | Sending policy |
|---|---|---|
| `Probe` | It has not yet established reliable streaming progress | Probe for a matching tuple; a nonempty append pauses the probe until new evidence or heartbeat response. |
| `Replicate` | A probe succeeded | Pipeline batches, updating `Next` optimistically and recording their final indices in the inflight window. |
| `Snapshot` | The peer needs a prefix the leader cannot send as entries | Wait for snapshot transfer feedback or append evidence; do not keep sending an incompatible log suffix. |

[`InflightWindow`](../../src/DotnetRaft/Tracker/InflightWindow.cs) limits
each peer's outstanding batches. With capacity two, sends through 14 and 18
can be in flight at once. A successful response for 18 covers both batches;
an older response for 14 does not roll progress back. The acknowledged prefix
allows the leader to free window space and send more work. Other followers
have their own windows; the slowest one need not gate a healthy majority.

There are several limits worth distinguishing in a performance diagnosis:

| Setting | Which resource it bounds |
|---|---|
| `MaxSizePerMessage` | Encoded-entry budget when selecting an append batch, not exact transport envelope size |
| `MaxInflightMessages` / `MaxInflightBytes` | Per-follower appends and their outstanding payload bytes |
| `MaxUncommittedEntriesSize` | Admission of growing, unapplied proposal payload, with forward-progress exceptions |
| `MaxCommittedSizePerReady` | Cumulative encoded bytes outstanding for application |

If the log is growing on an isolated leader, look at proposal admission. If
one follower is slow but others stay healthy, look at its individual window.
If `Committed` keeps moving but `Applied` does not, examine the host's apply
worker and acknowledgements first.

### A heartbeat is not an append acknowledgement

In this port `MsgHeartbeat` is separate from `MsgApp`. It carries no entries,
so the leader sends that peer at most `min(Match, Committed)` for commit.
The reply can unblock a paused probe, report activity for quorum checking,
and echo a read context. It does **not** prove the peer has copied the latest
suffix. In contrast, a matched append can advance a peer's `Match`, and a
leader can use a quorum of actual matches to try to commit.

In a five-voter example, suppose the `Match` values are
`[100, 94, 92, 81, 0]`. Sort by index and choose the third largest, 92. A
majority has established at least that prefix. The current-term test still
applies before the leader advances `Committed`. A learner at 105 would not
change this calculation, nor would an optimistic `Next` of 120.

## When entries are no longer available: snapshots

A snapshot represents the state-machine prefix at `(index, term)` and carries
the membership valid there. Creating the snapshot and compacting the log are
host policy; the core implements acceptance and transfer, not serialization
of business state. Separate three cases at the receiver:

1. **Obsolete:** the snapshot index is no newer than the committed prefix.
   Installing it would move state backward.
2. **Already matched:** the receiver finds the same tuple in its log. It can
   advance commit through the matched prefix without replacing its whole log.
3. **Needs installation:** the prefix is absent. Accept the snapshot as
   unstable, restore its consensus membership, and wait for persistence and
   application reconciliation before exposing later committed entries.

After the host publishes a snapshot, the sender can start with the first
entry after the snapshot. Distinguish two kinds of feedback: the host's
`ReportSnapshot` reports a transport result to progress tracking; a follower's
protocol acknowledgement reports replication evidence. A successful RPC
transport return is not permission to pretend that the peer has matched
unrelated later entries.

## Experiments

```bash
dotnet test tests/DotnetRaft.Tests -c Debug --no-restore \
  --filter 'FullyQualifiedName~RaftCoreRejectionHintTests|FullyQualifiedName~RaftCoreFlowControlTests|FullyQualifiedName~RaftCoreSnapshotRestoreTests'
```

Read [`02-replication-drop.txt`](../../tests/DotnetRaft.Tests/Interaction/TestData/02-replication-drop.txt)
as a time line: drop one append, allow the other two nodes to stabilize,
then tick the leader. Predict the lagging follower's `Match`, `Next`, and
retained log before and after the retry. Observe how the same analysis works
even when the surviving quorum has already applied the command.

1. Why is it safe to replace a suffix only after matching the preceding
   tuple? Show an incorrect execution if you check only the preceding index.
2. If a follower acknowledges 18 before 14, how many inflight batches are now
   free? What if the later-delivered response for 14 is a rejection instead?
3. What is the difference between a peer needing a snapshot because data was
   compacted and a storage implementation losing an expected retained entry?
4. Sketch a crash after a new leader changes the logical suffix but before
   the host publishes it. Which prefix will restart actually see?

Continue with [Ready, persistence, and restart](03-ready-and-recovery.md).
