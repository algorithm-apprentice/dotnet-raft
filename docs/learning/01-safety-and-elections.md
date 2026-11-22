# 1. Why Raft elections are safe

This chapter derives the rules behind leader election and commitment. Keep
the [learning path](README.md) open as a map and the
[host contract](../integration/README.md) for service requirements.
Examples here are explanatory traces, not a second API contract.

## Start with the problem, not the roles

Suppose three replicas keep an account balance. Two clients race to withdraw
money. Each replica can execute a withdrawal deterministically, but all must
execute the **same commands in the same order**. Receiving messages in arrival
order fails: A can hear the first client before the second while B hears the
reverse. Raft chooses one ordered log. The state machine executes only the
prefix that is safe to keep after a change of leader.

Raft assumes non-Byzantine peers. A process can crash, messages can be lost,
delayed, replayed, or reordered, and different partitions may disagree about
who is alive. Peers do not forge votes or report a write as durable when it is
not. Each node that restarts with the same identity must recover its durable
consensus state. A node ID by itself is not authentication.

Separate two types of claims:

| Claim | What it means | What it does not mean |
|---|---|---|
| Safety | Bad histories cannot occur: two different commands cannot be applied at the same index. | A command will finish on any time schedule. |
| Liveness | When a majority can communicate and processing becomes timely, elections and proposals can eventually finish. | A permanently partitioned minority can make progress, or any finite timeout proves a peer died. |

Safety is not bought by choosing an election timeout that is "long enough."
Heartbeat intervals and randomized timeouts affect when nodes try to make
progress; durable votes, log rules, and quorum intersection affect what they
are allowed to decide.

## Four different facts often called "the term"

When reading a trace, always label which term you mean:

| Value | Meaning | Example |
|---|---|---|
| Local current term | Latest election epoch recognized by this node. Part of `HardState`. | A candidate increments its local term to 7. |
| RPC/message term | Epoch claimed by the sender for the protocol action. | `MsgApp.Term = 7` means a term-7 leader sent the append. |
| Entry term | Epoch of the leader that originally created one log entry. Never rewritten by a later leader. | That append can carry an unchanged term-5 entry. |
| Snapshot term | Entry term at the index covered by the snapshot. | A snapshot through index 40 might carry term 5 even when sent in term 7. |

This is why the fields `Message.Term` and `Message.LogTerm` are not redundant.
For `MsgVote`, `Index`/`LogTerm` describe the candidate's last entry; for
`MsgApp`, they describe the preceding entry. `EntryId` pairs an index and an
entry term independently of the current role. Read the
[`raft.proto`](../../src/DotnetRaft/Protocol/raft.proto) schema and
[`EntryId`](../../src/DotnetRaft/Core/EntryId.cs) together.

## Majority arithmetic, including the edge of a partition

With `N` voters in a fixed configuration, a majority contains
`floor(N / 2) + 1` distinct voters. If two majorities were disjoint, their
union would need more than `N` voters. They therefore intersect in at least
one voter. This remains true however the network splits the voters.

| Voters | Majority | Failures tolerated while making progress |
|---:|---:|---:|
| 1 | 1 | 0 |
| 2 | 2 | 0 |
| 3 | 2 | 1 |
| 4 | 3 | 1 |
| 5 | 3 | 2 |

Count **voters**, not processes. A learner can have a complete copy of the log
and still contribute nothing to an election or commit quorum. Repeated copies
of one vote do not increase the count. `MajorityConfig.VoteResult` distinguishes
three cases: won, still possible given missing votes, and lost even if all
remaining votes were granted. `ProgressTracker` records votes by peer ID, and
`JointConfig` combines the constituent results when membership changes.

### Election Safety: at most one leader in a term

Assume two different candidates won the same term. Each had a majority of
grants. A voter in the intersection must have granted to both. Raft stores
`Term` and `Vote` so the same voter cannot grant to two different candidates
in one term, including across restart. A retransmitted grant to the *same*
candidate does not violate the proof.

Notice the required ordering: a voter must persist its decision before the
grant becomes observable. If B can reply "yes to A in term 7," crash before
remembering that vote, and then reply "yes to C in term 7," the very voter
that made the intersection useful is no longer a witness. This is a
correctness requirement on the host's durability, not an optimization.

### Majority is necessary, but it does not say whom to elect

Giving one vote per term prevents duplicate leaders **within** that term. It
does not stop an elected node from overwriting data that was committed in a
previous term. The election restriction compares the candidate's last
`(term, index)` lexicographically with the voter's:

```text
candidate is up to date if
    candidate.lastTerm > voter.lastTerm
    OR
    candidate.lastTerm == voter.lastTerm
        AND candidate.lastIndex >= voter.lastIndex
```

For example `(term=4, index=8)` beats `(term=3, index=50)`; length alone is not
freshness. Within the same last term, `(4, 9)` beats `(4, 8)`. An election uses
the last entry, not `commitIndex`. A node may know a newer, uncommitted suffix
that is essential to preserving quorum history. Follow `RaftLog.IsUpToDate`
in [`RaftLog.cs`](../../src/DotnetRaft/Core/RaftLog.cs) and
`RaftCore.HandleVoteRequest` in [`RaftCore.cs`](../../src/DotnetRaft/Core/RaftCore.cs).

## Why a leader cannot commit an old-term entry by counting replicas

Here is a simplified version of the Raft paper's Figure 8. All five voters
share index 1. Show only their copies of index 2; `-` means no entry there.
Assume ordinary elections, without leases or pre-vote, and delay other
replication messages.

| Step | A | B | C | D | E | What happened |
|---|---|---|---|---|---|---|
| A is leader in term 2 | `2:x` | `2:x` | `-` | `-` | `-` | `x` has only two replicas. A becomes unavailable. |
| E is leader in term 3 | `2:x` | `2:x` | `-` | `-` | `3:y` | C, D, E could elect E; E appends a different entry locally. E becomes unavailable. |
| A returns and wins term 4 | `2:x` | `2:x` | `2:x` | `-` | `3:y` | A, B, C can now contain `x`. It is still a term-2 entry. |
| A becomes unavailable again | `2:x` | `2:x` | `2:x` | `-` | `3:y` | E can win term 5: its last entry term 3 is newer than term 2. |

If A had told a client that `x` was committed in step three merely because
three nodes held it, E could later replace `x` with `y`. There is an
intersection between the old and new majorities; the voters in it really did
see `x`. The problem is that the freshness rule can still let them accept a
candidate whose last term is greater than 2.

Now change step three: A also appends a **term-4** entry at index 3, and
replicates through it to A, B, and C. E's term-3 log is now too old for B or
C. A can commit index 3 by counting the quorum and thereby commit the prefix
through index 2 as well. This is the reason for the new leader's no-op: it
creates an entry in the new term even if clients supply no writes. A no-op is
still a log entry and occupies an applied index.

This rule is intentionally directional:

- The leader chooses a new commit index using only a quorum-confirmed entry
  whose entry term equals its current term. Earlier entries become committed
  *through the prefix*.
- A follower can advance from the leader's communicated commit index across
  older entries it has matched. It is not independently counting votes for
  those entries.
- An entry already known to be committed is not made uncommitted by a change
  of leader.

### Sketch of Leader Completeness

Let a term-`T` leader commit a term-`T` entry at index `i`. Suppose the first
later leader missing that entry wins in term `U`. Its election quorum shares
a voter with the quorum that stored the entry. That voter took the older
append **before** it voted in term `U`; it would reject an append from term
`T` after recognizing term `U`. A prior leader cannot have removed the entry
from that voter: by choice of `U`, earlier intervening leaders contain it.

To win that vote, the new candidate must look at least as up to date as the
voter. If its last entry is in term `T`, it needs a term-`T` prefix extending
through `i`; those entries came from the unique term-`T` leader. If its last
entry is in a later term, that entry came from an intervening leader that
already contained the prefix. Either way, log matching means the candidate
also contains index `i`, a contradiction. This is a proof sketch under
crash-recovery assumptions; the joint-configuration case needs the
quorum-transition rules in [chapter 4](04-membership-reads-and-liveness.md).

## The actual election path in C#

Read these in order, instead of starting from an HTTP endpoint:

1. [`RawNode.Tick`](../../src/DotnetRaft/RawNode.cs) dispatches logical time to
   the core. [`RaftClock`](../../src/DotnetRaft/Core/RaftClock.cs) increments
   elapsed counters and chooses a fresh timeout in
   `[ElectionTick, 2 * ElectionTick)` on reset. The host chooses how often to
   call `Tick`; the core counts logical ticks, not wall-clock time.
2. [`RaftCore.TickElection`](../../src/DotnetRaft/Core/RaftCore.Election.cs)
   triggers a local campaign for an eligible node.
   A learner or a node with a pending unstable snapshot cannot campaign.
   `HandleHup` also protects the application of committed configuration
   entries during an ordinary campaign.
3. `Campaign` transitions to candidate, sends requests containing `LastEntryId`,
   and generates a response addressed to itself for its own vote. The node
   does not advance its election merely because it queued a self vote: that
   response belongs to the after-append queue in
   [`RaftOutput`](../../src/DotnetRaft/Core/RaftOutput.cs).
4. An incoming message passes the term logic in
   [`RaftCore.Step`](../../src/DotnetRaft/Core/RaftCore.cs) before dispatching
   on the resulting role. Higher-term traffic normally makes the node a follower. A
   higher-term pre-vote request or granted pre-vote response is not a real
   term change. With `CheckQuorum`, an active leader lease can also suppress
   an ordinary higher-term vote or pre-vote request, but not a transfer
   campaign's vote request.
   `HandleVoteRequest` decides whether voting is permitted and the candidate's
   log is sufficiently new. Unknown, stale, and duplicate vote responses
   must not create another quorum.
5. On quorum, `BecomeLeader` initializes follower progress and appends the
   new-term no-op. `MaybeCommit` combines `Tracker.CommittedIndex` with
   `RaftLog.MaybeCommit`, which checks the entry term. A leader's own copy is
   acknowledged through persistence too.

This ordering explains an important observation in a debugger: you can see
the in-memory `Vote` change and find an outbound vote response without the
response having been sent or even delivered to the same node. The `Ready`
boundary separates planned protocol effects from observable durable effects;
see [chapter 3](03-ready-and-recovery.md).

## Work through a split vote

Imagine A and B time out together at term 8 and vote for themselves. C votes
for A. Only A can win: B needs either C or A, and both are already committed
to a vote in that term. If none has a majority (for example with unavailable
links), nothing is decided. A later timeout starts a *new* term. Randomized
timeouts reduce repeated collisions; they do not make a vote valid. A term
increase is not evidence that an earlier term committed anything.

Now isolate B long enough that it would repeatedly start elections. It may
increase terms without progress and disrupt a healthy leader when it rejoins.
**PreVote** asks whether the next election could succeed before committing to
a real term increase. A positive future pre-vote is not a real vote. A
rejected response from a peer already in a newer term can still convey that
newer term. Go through `Campaign(PreElection)`, `IsHigherTermPreVoteException`,
and the pre-candidate strategy; see
[chapter 4](04-membership-reads-and-liveness.md) for the separate quorum and
lease mechanisms.

## Experiments

From the repository root, after the setup in the
[learning guide](README.md#prepare-the-repository-for-offline-use):

```bash
dotnet test tests/DotnetRaft.Tests -c Debug --no-restore \
  --filter 'FullyQualifiedName~RaftCoreElectionTests|FullyQualifiedName~RaftCorePreVoteTests|FullyQualifiedName~CurrentTermRuleBlocksOlderEntriesUntilNoOpIsAcknowledged'
```

Read [the replication regression](../../tests/DotnetRaft.Tests/Core/RaftCoreReplicationTests.cs)
and stop at the two calls that deliver acknowledgements. Predict the commit
index after each before running the test. Then use line breakpoints on
`Campaign`, `HandleVoteRequest`, and `MaybeCommit` to follow the `Term`,
`Vote`, and final log tuple independently.

Questions to answer without the code:

1. What changes if a vote is remembered only in RAM? Identify exactly which
   proof step fails. **Hint:** quorum intersection is still mathematically true.
2. Can two nodes each believe they are leader? **Yes**, during a partition in
   different terms. Explain why the old one cannot use that belief to obtain
   a new quorum.
3. Could a longer log lose to a shorter log? Give concrete last tuples.
4. Why does majority storage of a current-term entry establish a safe prefix
   while majority storage of an older entry in Figure 8 does not?

Continue with [log matching and replication](02-log-and-replication.md).
