# Learning Raft with dotnet-raft

This is a self-contained, offline study guide for learning Raft through this
repository. It explains the algorithm, maps each concept to production code
and executable tests, and ends with a durable three-node KV-store laboratory.

The repository implements the deterministic consensus state machine from the
pinned `etcd-io/raft` behavior. It also contains:

- a synchronous `RawNode` integration API;
- a concurrent `RaftNode` facade;
- durable SQLite Raft storage;
- a durable three-node gRPC KV example; and
- translated unit, interaction, recovery, and process-crash tests.

You do not need the Raft paper or an internet connection while following this
guide. The paper remains useful background, but the explanations and all
required links below are local to the repository.

## 1. Learning goals

After completing the guide, you should be able to:

1. explain why Raft needs terms, randomized elections, log matching, and a
   majority quorum;
2. trace a proposal from a client through replication, commitment, durable
   application, and response;
3. distinguish stable, unstable, committed, applying, logically applied, and
   physically applied state;
4. explain the current-term commit rule and the leader's initial no-op entry;
5. reason about follower progress in probe, replicate, and snapshot states;
6. explain joint consensus, learners, snapshots, `ReadIndex`, `PreVote`,
   `CheckQuorum`, and leadership transfer;
7. integrate `RawNode` safely through `Ready` and `Advance`;
8. explain what the Raft library does not provide and what a production host
   must provide;
9. diagnose loss of quorum, stale reads, slow followers, and crash-recovery
   boundaries; and
10. use this repository's tests as an executable Raft specification.

## 2. Prepare the repository for offline use

The solution targets .NET 10.

Before disconnecting from the network, restore all dependencies once:

```bash
dotnet --version
dotnet restore DotnetRaft.sln
dotnet build DotnetRaft.sln -c Debug --no-restore
```

Run the baseline tests:

```bash
dotnet test DotnetRaft.sln -c Debug --no-restore --verbosity quiet
```

The rest of this guide uses `--no-restore`, so a populated local NuGet cache is
enough for offline study.

Useful test commands:

```bash
# Run one test class.
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Debug --no-restore \
  --filter "FullyQualifiedName~RaftCoreElectionTests"

# List tests when you are unsure of a class or method name.
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Debug --no-restore --list-tests

# Run the durable SQLite storage tests.
dotnet test tests/DotnetRaft.Sqlite.Tests/DotnetRaft.Sqlite.Tests.csproj \
  -c Debug --no-restore
```

## 3. The most important mental model

Raft in this repository is a deterministic state machine, not a server:

```text
current Raft state + Message or Tick
                 |
                 v
          deterministic step
                 |
                 v
Ready {
  state to persist,
  entries to persist,
  messages to send,
  committed entries to apply,
  completed read barriers
}
                 |
                 v
host persists, sends, applies, and acknowledges
```

The library owns consensus decisions. The host owns real-world effects.

### 3.1 Three layers

| Layer | This repository | Responsibility |
|---|---|---|
| Consensus core | `src/DotnetRaft` | Elections, replication, commitment, membership, snapshots, reads |
| Integration | `RawNode`, `RaftNode`, `Ready` | Serialize inputs and expose ordered work to a host |
| Product host | `examples/DotnetRaft.KvCluster` | Network, timers, databases, application state, retries, recovery |

The optional `DotnetRaft.Sqlite` package persists consensus state, but it does
not persist the application state machine.

### 3.2 State categories

| State | Examples | Must survive restart? |
|---|---|---|
| Persistent consensus state | current term, vote, committed index | Yes |
| Stable log | entries already durably stored | Yes |
| Unstable log | new entries/snapshot awaiting persistence | Not yet |
| Volatile role state | follower/candidate/leader, known leader, timers | Reconstructed |
| Leader progress | each follower's `Match`, `Next`, inflight appends | Reconstructed |
| Application state | KV rows, request deduplication, physical cursor | Yes, in a real host |

Read these files first:

- [Reference architecture](reference-architecture.md)
- [Public API and host responsibilities](public-api.md)
- [Protocol schema](../src/DotnetRaft/Protocol/raft.proto)
- [Raft configuration](../src/DotnetRaft/RaftConfig.cs)

## 4. Raft fundamentals before reading code

### 4.1 Terms

A term is Raft's logical leadership epoch.

- Terms increase monotonically.
- A node votes for at most one candidate per real term; repeated requests from
  that same candidate may be granted again.
- Outside the intentional exceptions below, a relevant higher-term message
  advances the local term and makes the node a follower.

The exceptions are important:

- a higher-term `MsgPreVote` or granted `MsgPreVoteResp` does not advance the
  real term; and
- an active `CheckQuorum` leader lease may ignore an ordinary higher-term vote
  or pre-vote request unless leadership-transfer context forces it.

Terms do not measure wall-clock time. They order leadership generations.

### 4.2 Roles

The core roles are:

```text
Follower -> Candidate -> Leader
    ^           |
    +-----------+
```

With `PreVote`, an additional pre-candidate phase checks whether an election is
likely to succeed before increasing the real term.

All nodes begin as followers. An eligible voting follower starts an election
after a randomized logical election timeout. A candidate becomes leader after
winning the current voter quorum.

### 4.3 Majority quorum

For `N` voters:

```text
majority = floor(N / 2) + 1
```

| Voters | Majority | Failures tolerated while remaining available |
|---:|---:|---:|
| 1 | 1 | 0 |
| 2 | 2 | 0 |
| 3 | 2 | 1 |
| 4 | 3 | 1 |
| 5 | 3 | 2 |

A two-node cluster does not continuously "vote forever" merely because it has
two nodes. Randomized timeouts reduce simultaneous campaigns when both nodes
can communicate. However, either node alone has only one vote and cannot form
a majority of two. A partition or one failed node therefore makes a two-node
cluster unavailable for elections and new commits.

Study:

- [MajorityConfig](../src/DotnetRaft/Quorum/MajorityConfig.cs)
- [JointConfig](../src/DotnetRaft/Quorum/JointConfig.cs)
- [Quorum tests](../tests/DotnetRaft.Tests/Quorum/QuorumTests.cs)

The majority commit index is the highest index acknowledged by a majority. In
a three-node cluster with acknowledged indexes `[12, 9, 7]`, the committed
quorum index is `9`.

Joint consensus requires both the incoming and outgoing voter majorities. Its
commit index is:

```text
min(incoming majority index, outgoing majority index)
```

### 4.4 Log entries

Each entry has:

```text
Index: its position in the replicated log
Term:  the leader term that created it
Type:  normal command or configuration change
Data:  application-defined bytes
```

An `(Index, Term)` pair identifies a logical log position. Candidate freshness
compares last-entry terms first and indexes second.

### 4.5 The safety properties to protect

Use these as your checklist whenever you inspect code:

1. **Election safety:** at most one leader can be elected in one term.
2. **Leader append-only:** a leader never overwrites its own entries.
3. **Log matching:** equal index and term imply the same preceding log.
4. **Leader completeness:** committed entries appear in future leaders.
5. **State-machine safety:** no two nodes apply different commands at the same
   index.

This repository turns those statements into local invariants and tests.

## 5. Repository map

The implementation was built in dependency order. The full graph is in the
[implementation DAG](implementation-dag.md).

| Topic | Production code | Design | Executable specification |
|---|---|---|---|
| Protocol messages | `Protocol/raft.proto` | D01-D02 in the DAG | `Protocol/*Tests` |
| Quorum math | `Quorum/*` | Reference architecture | `QuorumTests` |
| Stable storage | `Storage/*` | [D04](design/d04-stable-storage.md) | `MemoryStorageTests` |
| Unstable and unified log | `Core/UnstableLog.cs`, `Core/RaftLog.cs` | [D05](design/d05-unstable-log.md), [D06](design/d06-raft-log.md) | `UnstableLogTests`, `RaftLogTests` |
| Follower replication progress | `Tracker/*` | [D07-D09](implementation-dag.md) | `Tracker/*Tests` |
| Configuration changes | `ConfChange/*` | [D10](design/d10-configuration-changes.md), [D17](design/d17-membership-integration.md) | `ConfChange/*Tests` |
| Core algorithm | `Core/RaftCore.cs` | [D12-D20](implementation-dag.md) | `Core/RaftCore*Tests` |
| Synchronous integration | `RawNode.cs`, `Ready.cs` | [D21](design/d21-rawnode-ready.md) | `RawNode/*Tests` |
| Concurrent integration | `RaftNode.cs` | [D24](design/d24-concurrent-node.md) | `Node/*Tests` |
| Async storage | local storage messages | [D25](design/d25-asynchronous-storage-writes.md) | `AsyncStorage/*Tests` |
| Durable Raft storage | `src/DotnetRaft.Sqlite` | [SQLite design](design/sqlite-durable-storage.md) | `DotnetRaft.Sqlite.Tests` |
| Durable KV product | `examples/DotnetRaft.KvCluster` | [Durable host design](design/durable-grpc-kv-host.md) | `Examples/*Tests`, process smoke |

Do not begin with the 2,000-line `RaftCore` file. Begin with leaf components,
then follow the curriculum below.

## 6. Curriculum

The modules are ordered so each one depends only on concepts already studied.

### Module 0: Protocol vocabulary and quorum algebra

**Goal:** understand what travels through the state machine and how a majority
is calculated.

Read in order:

1. [raft.proto](../src/DotnetRaft/Protocol/raft.proto)
2. [MajorityConfig](../src/DotnetRaft/Quorum/MajorityConfig.cs)
3. [JointConfig](../src/DotnetRaft/Quorum/JointConfig.cs)
4. [QuorumTests](../tests/DotnetRaft.Tests/Quorum/QuorumTests.cs)

Run:

```bash
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Debug --no-restore \
  --filter "FullyQualifiedName~QuorumTests"
```

Questions:

1. Why does an empty majority return a special algebraic result?
2. Why do learners not contribute to commitment?
3. Why is joint commitment the minimum of two quorum indexes?

Exercise:

```text
Voters: 1, 2, 3, 4, 5
Match:  14, 13, 10, 8, 6
```

The majority is three, so the commit candidate is `10`.

### Module 1: Stable storage, unstable state, and the unified log

**Goal:** understand how Raft presents one logical log while some entries are
durable and others are not.

Read:

1. [IStorage](../src/DotnetRaft/Storage/IStorage.cs)
2. [MemoryStorage](../src/DotnetRaft/Storage/MemoryStorage.cs)
3. [UnstableLog](../src/DotnetRaft/Core/UnstableLog.cs)
4. [RaftLog](../src/DotnetRaft/Core/RaftLog.cs)
5. [D06 design](design/d06-raft-log.md)

The central cursor invariant is:

```text
Applied <= Applying <= Committed <= LastIndex
```

Important distinctions:

- `Committed` means a quorum has made the entry safe to execute.
- `Applying` means the entry has been handed to the application pipeline.
- `Applied` is the library's acknowledged logical cursor.
- A durable host may also need a separate `physicalApplied` cursor.

Stable storage keeps a dummy entry at the compacted boundary so the term at
`FirstIndex - 1` remains available for log matching.

Run:

```bash
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Debug --no-restore \
  --filter "FullyQualifiedName~MemoryStorageTests|FullyQualifiedName~UnstableLogTests|FullyQualifiedName~RaftLogTests"
```

Trace these operations:

```text
FindConflict
MaybeAppend
CommitTo
MaybeCommit
AcceptApplying
AppliedTo
Restore
```

Questions:

1. Why may an uncommitted conflicting suffix be overwritten?
2. Why must a committed entry never be overwritten?
3. Why is compaction different from deleting arbitrary old entries?

### Module 2: Election and term transitions

**Goal:** explain exactly how a follower becomes leader and why stale leaders
cannot continue safely.

Read:

1. [RaftClock](../src/DotnetRaft/Core/RaftClock.cs)
2. [Raft role state](../src/DotnetRaft/Core/RaftRoleState.cs)
3. [Role strategies](../src/DotnetRaft/Core/RaftRoleStrategies.cs)
4. [Election design](design/d13-leader-election.md)
5. `BecomeFollower`, `BecomeCandidate`, `BecomePreCandidate`,
   `BecomeLeader`, `Step`, and `HandleVoteRequest` in
   [RaftCore](../src/DotnetRaft/Core/RaftCore.cs)
6. [Election tests](../tests/DotnetRaft.Tests/Core/RaftCoreElectionTests.cs)

Election flow:

```text
randomized timeout
    -> local MsgHup
    -> candidate increments term and votes for itself
    -> MsgVote requests contain last log index and term
    -> voters check term, prior vote, and log freshness
    -> candidate wins a majority
    -> leader appends one current-term no-op entry
```

The self-vote and vote grants use the after-persistence path. A vote must not
be published before the new term and vote are durable.

Run:

```bash
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Debug --no-restore \
  --filter "FullyQualifiedName~RaftCoreElectionTests"
```

Focus on these tests:

- `DurableSelfVoteElectsSingletonAndNoOpNeedsNextBatch`
- `CandidateTimeoutStartsFreshTermAndClearsPriorVotes`
- `JointElectionRequiresBothConstituentMajorities`
- `ElectionLossPreservesTermAndPersistentSelfVote`
- `LearnerCanVoteButCannotCampaign`

Questions:

1. Why is the election timeout randomized?
2. Why does log freshness compare term before index?
3. Why does an isolated pre-candidate not immediately increase the real term?

### Module 3: Proposals, replication, and commitment

**Goal:** trace one command from proposal to quorum commitment.

Read:

1. [Basic replication design](design/d14-basic-log-replication.md)
2. `HandleLeaderProposal`, `AppendLeaderEntries`, `MaybeSendAppend`,
   `HandleAppendEntries`, `HandleAppendResponse`, and `MaybeCommit` in
   [RaftCore](../src/DotnetRaft/Core/RaftCore.cs)
3. [Replication tests](../tests/DotnetRaft.Tests/Core/RaftCoreReplicationTests.cs)

Proposal behavior by role:

| Role | Behavior |
|---|---|
| Leader | Append locally, then replicate |
| Follower with known leader | Forward `MsgProp` unless `DisableProposalForwarding` is enabled |
| Follower without leader | Drop explicitly |
| Candidate/pre-candidate | Drop explicitly |

A new leader appends an empty current-term entry. This no-op has two jobs:

1. establish a committed entry in the leader's own term; and
2. indirectly commit safe entries inherited from older terms.

The leader does not commit an older-term entry merely because a majority
stores it. It advances commit through quorum acknowledgement only when the
entry at the candidate commit index belongs to the current term.

Run:

```bash
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Debug --no-restore \
  --filter "FullyQualifiedName~RaftCoreReplicationTests"
```

Focus on:

- `FollowerForwardsProposalWithoutMutatingCaller`
- `AcceptedResponseAdvancesProgressCommitsAndBroadcasts`
- `CurrentTermRuleBlocksOlderEntriesUntilNoOpIsAcknowledged`
- `JointCommitRequiresBothConstituentMajorities`

Write-path checkpoint:

```text
client command
  -> follower forwards or leader accepts
  -> leader assigns current term and next index
  -> entry becomes unstable
  -> host persists it
  -> leader receives acknowledgements
  -> quorum-derived commit advances
  -> Ready exposes committed entry
  -> host applies it in order
  -> client receives a result
```

### Module 4: Follower progress and flow control

**Goal:** understand how a leader catches up fast and slow followers without
unbounded memory or network use.

Read:

1. [InflightWindow](../src/DotnetRaft/Tracker/InflightWindow.cs)
2. [Progress](../src/DotnetRaft/Tracker/Progress.cs)
3. [ProgressTracker](../src/DotnetRaft/Tracker/ProgressTracker.cs)
4. [Follower progress design](design/d08-follower-progress.md)
5. [Replication flow-control design](design/d15-replication-flow-control.md)

Progress states:

| State | Meaning |
|---|---|
| Probe | Exact follower position is uncertain; send one effective probe |
| Replicate | Optimistically stream entries through an inflight window |
| Snapshot | Required log prefix is compacted; pause appends and send snapshot |

Key fields:

```text
Match = highest index known replicated on the follower
Next  = next index the leader will try
```

Invariant:

```text
Match < Next
```

Rejection hints let the leader jump backward by term instead of decrementing
one index at a time.

Run:

```bash
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Debug --no-restore \
  --filter "FullyQualifiedName~InflightWindowTests|FullyQualifiedName~ProgressTests|FullyQualifiedName~RaftCoreFlowControlTests|FullyQualifiedName~RaftCoreRejectionHintTests"
```

Questions:

1. Why may `Next` advance optimistically while `Match` may not?
2. Why is probe mode limited to one effective append?
3. What event moves a follower from replicate back to probe?

### Module 5: Snapshots and compaction

**Goal:** distinguish a consensus snapshot from log compaction and understand
slow-follower recovery.

Read:

1. [Snapshot design](design/d16-snapshots.md)
2. `MaybeSendSnapshot`, `HandleSnapshot`, and `RestoreSnapshot` in
   [RaftCore](../src/DotnetRaft/Core/RaftCore.cs)
3. [Snapshot restore tests](../tests/DotnetRaft.Tests/Core/RaftCoreSnapshotRestoreTests.cs)
4. [MemoryStorage snapshot operations](../src/DotnetRaft/Storage/MemoryStorage.cs)

A Raft snapshot includes:

```text
application bytes through index N
term at index N
ConfState established at index N
```

Compaction removes old log payloads only after a usable snapshot covers them.
A snapshot is not automatically a backup policy; it is a compact recovery
point for Raft and the application state machine.

Follower catch-up involves three distinct acknowledgements and must not be
read as one atomic network event:

```text
follower.Next points below leader.FirstIndex
    -> retained entries are compacted
    -> leader sends MsgSnap
    -> transport completion makes the sender call ReportSnapshot
    -> leader progress moves Snapshot -> paused Probe

follower Ready processing
    -> persist the consensus snapshot
    -> durable MsgAppResp may be sent
    -> restore the application snapshot before later entries and Advance

later heartbeat response
    -> clears the probe pause
    -> ordinary append replication resumes
```

`ReportSnapshot.Success` reports transport delivery. It is not proof that the
remote application state machine has already restored the snapshot.

Run:

```bash
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Debug --no-restore \
  --filter "FullyQualifiedName~RaftCoreSnapshot|FullyQualifiedName~RawNodePersistenceTests"
```

Questions:

1. Why can an obsolete snapshot still require a durable term update?
2. Why must snapshot application precede later committed entries?
3. Why must snapshot `ConfState` match membership at exactly its index?

### Module 6: Membership and joint consensus

**Goal:** understand why membership is part of replicated state and why
changing multiple voters needs joint consensus.

Read:

1. [Configuration changer](../src/DotnetRaft/ConfChange/ConfigurationChanger.cs)
2. [Configuration restore](../src/DotnetRaft/ConfChange/ConfigurationRestore.cs)
3. [Configuration design](design/d10-configuration-changes.md)
4. [Membership integration](design/d17-membership-integration.md)
5. `ApplyConfigurationChange` and `SwitchToConfiguration` in
   [RaftCore](../src/DotnetRaft/Core/RaftCore.cs)

Membership changes become active when the committed configuration entry is
applied, not when it is proposed or merely appended.

Simple changes may alter at most one voter. Multi-voter changes use:

```text
old configuration
    -> joint configuration: old AND new quorums
    -> new configuration
```

Learners replicate data but cannot campaign and never contribute to election,
read, activity, or commit quorums. This implementation still permits a learner
to grant a vote or pre-vote request; quorum tallying ignores that learner's
response.

Run:

```bash
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Debug --no-restore \
  --filter "FullyQualifiedName~Configuration|FullyQualifiedName~RaftCoreMembership"
```

Questions:

1. Why is changing from `{1,2,3}` directly to `{3,4,5}` unsafe?
2. Why must a demoted voter sometimes remain a staged learner until joint
   consensus is left?
3. Why must acceptance or rejection of a configuration entry be deterministic
   application behavior?

### Module 7: Linearizable reads

**Goal:** explain why a local read from a follower is not automatically
linearizable and how `ReadIndex` avoids appending a log entry for every read.

Read:

1. [ReadOnlyTracker](../src/DotnetRaft/Read/ReadOnlyTracker.cs)
2. [ReadIndexCoordinator](../src/DotnetRaft/Core/ReadIndexCoordinator.cs)
3. [Safe-read design](design/d18-safe-linearizable-reads.md)
4. `HandleLeaderReadIndex`, `TrackReadIndex`, and
   `CompleteReadIndexRequests` in
   [RaftCore](../src/DotnetRaft/Core/RaftCore.cs)
5. [ReadIndex tests](../tests/DotnetRaft.Tests/Core/RaftCoreReadIndexTests.cs)

The ordinary non-singleton safe-read flow is:

```text
request reaches leader
  -> leader must have committed an entry in its current term
  -> leader records current commit index
  -> heartbeat context is confirmed by the voter quorum
  -> ReadState(index, requestContext) is returned
  -> host waits until physicalApplied >= index
  -> application executes the read
```

Two optimized paths need separate treatment:

- a local, non-joint single-voter leader returns its current commit index
  immediately, even before its current-term no-op is persisted; and
- lease-based reads still require the current-term authority gate, but after
  that gate they use the valid `CheckQuorum` leader lease instead of a
  per-request heartbeat round.

The returned index is a barrier, not the value itself.

The durable KV example exposes both:

```text
GET /kv/{key}       linearizable ReadIndex path
GET /local/{key}    local, potentially stale path
```

Run:

```bash
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Debug --no-restore \
  --filter "FullyQualifiedName~ReadOnlyTrackerTests|FullyQualifiedName~ReadIndex"
```

Questions:

1. Why must a non-singleton new leader first commit a current-term entry?
2. Why is `ReadState.Index` insufficient without a physical application
   barrier?
3. Why do joint configurations require both majorities for a safe read?

### Module 8: Availability extensions and leadership transfer

**Goal:** separate safety from mechanisms that reduce unnecessary disruption.

Read:

- [Availability extensions](design/d19-availability-extensions.md)
- [Leadership transfer](design/d20-leadership-transfer.md)
- `HandleCheckQuorum`, `HandleLeaderTransfer`, and related methods in
  [RaftCore](../src/DotnetRaft/Core/RaftCore.cs)

Features:

| Feature | Purpose |
|---|---|
| `PreVote` | Prevent isolated nodes from needlessly increasing terms |
| `CheckQuorum` | Make a leader step down when it cannot contact a quorum |
| Lease-based reads | Avoid quorum round trips under a bounded-clock assumption |
| Leadership transfer | Move leadership to an eligible caught-up voter |
| `ForgetLeader` | Let a follower stop deferring elections to a stale leader |

`PreVote` and `CheckQuorum` improve behavior, but they do not allow a minority
partition to commit.

Run:

```bash
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Debug --no-restore \
  --filter "FullyQualifiedName~PreVote|FullyQualifiedName~CheckQuorum|FullyQualifiedName~LeadershipTransfer"
```

### Module 9: RawNode, Ready, and durability ordering

**Goal:** understand the most important host integration contract.

Read:

1. [Ready](../src/DotnetRaft/Ready.cs)
2. [RawNode](../src/DotnetRaft/RawNode.cs)
3. [RawNode and Ready design](design/d21-rawnode-ready.md)
4. [RawNode Ready tests](../tests/DotnetRaft.Tests/RawNode/RawNodeReadyTests.cs)

`Ready` contains:

| Field | Meaning |
|---|---|
| `SoftState` | Volatile role or leader change |
| `HardState` | Persistent term, vote, and commit change |
| `Entries` | Unstable entries to persist |
| `Snapshot` | Unstable snapshot to persist and restore |
| `CommittedEntries` | Entries ready for ordered application |
| `Messages` | Network or local messages to deliver |
| `ReadStates` | Completed linearizable-read barriers |
| `MustSync` | Whether entries or a term/vote change require a synchronous stable-storage write; snapshot presence is not part of the calculation |

Safe synchronous order:

```text
Ready
  -> atomically persist Snapshot + Entries + HardState
  -> honor MustSync for Entries + HardState
  -> complete Snapshot persistence under the snapshot store's durability contract
  -> send Messages
  -> restore application Snapshot
  -> apply CommittedEntries in order
  -> admit ReadStates to the same ordered application path
  -> Advance(the exact Ready instance)
```

Never send a vote grant or append acknowledgement before the state it
acknowledges is durable.

A commit-only `HardState` update may have `MustSync == false`. A snapshot-only
generation may also have `MustSync == false`. The snapshot must still be
successfully stored before its dependent acknowledgement is published, but
`MustSync` does not define an additional generic snapshot flush.

Run:

```bash
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Debug --no-restore \
  --filter "FullyQualifiedName~RawNodeReadyTests|FullyQualifiedName~RawNodePersistenceTests"
```

Focus on:

- `SingletonSelfAppendWaitsForAdvance`
- `ReadyRequiresExactOutstandingInstance`
- `WorkAddedAfterAcceptanceSurvivesOlderAdvance`
- `SnapshotRemainsInProgressUntilAdvance`

### Module 10: RaftNode concurrency and asynchronous storage

**Goal:** understand how concurrency is added without making the core
concurrent.

Read:

1. [RaftNode](../src/DotnetRaft/RaftNode.cs)
2. [Concurrent Node design](design/d24-concurrent-node.md)
3. [Asynchronous storage design](design/d25-asynchronous-storage-writes.md)

`RaftNode` owns one background loop. Concurrent callers enqueue owned copies
of commands. Only that loop calls `RawNode`.

Cancellation rule:

```text
waiting -> canceled     operation never dispatched
waiting -> claimed      operation runs exactly once
claimed -> completed    later caller cancellation cannot roll it back
```

Proposals remain queued while no leader is known. Ticks, ordinary commands,
and proposals are scheduled fairly enough that one busy lane cannot
indefinitely starve another.

With `AsyncStorageWrites = true`:

- do not process the diagnostic Ready persistence/application fields;
- do not call `Advance`;
- route `MsgStorageAppend` to a reliable append worker;
- route `MsgStorageApply` to a reliable application worker; and
- step their exact response messages back into the same node instance.

Run:

```bash
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Debug --no-restore \
  --filter "FullyQualifiedName~RaftNode|FullyQualifiedName~AsyncStorage"
```

### Module 11: Durable SQLite Raft storage

**Goal:** understand what it means to persist the consensus state safely.

Read:

1. [SQLite storage design](design/sqlite-durable-storage.md)
2. [SqliteStorage](../src/DotnetRaft.Sqlite/SqliteStorage.cs)
3. [SQLite storage tests](../tests/DotnetRaft.Sqlite.Tests/SqliteStorageDurabilityTests.cs)

The database stores:

- `HardState`;
- retained snapshot and exact `ConfState`;
- retained log entries;
- pending application-snapshot recovery metadata; and
- format/schema metadata.

The storage transaction publishes one complete Raft generation. It must never
recover a hard-state commit beyond the durable log or below the published
snapshot boundary.

An incoming snapshot creates a cross-database problem:

```text
raft.db says snapshot is durable
application.db may not yet have restored snapshot data
```

The package records a pending application-snapshot marker. Startup must
restore the application snapshot idempotently, repair commit only when safely
proven, and acknowledge the marker before restarting Raft.

Run:

```bash
dotnet test tests/DotnetRaft.Sqlite.Tests/DotnetRaft.Sqlite.Tests.csproj \
  -c Debug --no-restore
```

### Module 12: The durable distributed KV host

**Goal:** connect the consensus algorithm to a real replicated application.

Read:

1. [Example README](../examples/DotnetRaft.KvCluster/README.md)
2. [Durable host ADR](adr/0007-durable-fixed-membership-kv-host.md)
3. [Durable host design](design/durable-grpc-kv-host.md)
4. [RaftClusterHost](../examples/DotnetRaft.KvCluster/RaftClusterHost.cs)
5. [DurableHostRecovery](../examples/DotnetRaft.KvCluster/DurableHostRecovery.cs)
6. [Application state machine](../examples/DotnetRaft.KvCluster/SqliteKeyValueStateMachine.cs)
7. [Integration tests](../tests/DotnetRaft.Tests/Examples/KvClusterIntegrationTests.cs)

Each node owns two databases:

```text
raft.db          consensus term, vote, log, snapshot
application.db   KV rows, request results, ConfState, physicalApplied
```

They are intentionally separate. Crash consistency comes from durable cursors,
replay, idempotent restore, and safe compaction order, not from pretending the
two databases share one atomic transaction.

Mutation transaction:

```text
KV effect
request-ID deduplication row
application ConfState
physicalApplied
```

all commit together in `application.db`.

The MVP retains every unique request ID in the application database and in
later snapshots. This makes retries idempotent without a time window, but
storage and snapshot size grow with mutation history because pruning is
deliberately out of scope.

The example is:

- one Raft group;
- one replicated partition;
- exactly three fixed members;
- strongly consistent for the linearizable API; and
- available for confirmed operations while any two members can communicate.

It is not:

- an etcd-compatible server;
- a sharded Multi-Raft system;
- an MVCC database;
- a watch/lease/auth system; or
- a dynamic production deployment platform.

Both listeners are loopback-only and unauthenticated. Structural Raft message
validation is not peer authentication or client authorization.

Run its tests:

```bash
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Debug --no-restore \
  --filter "FullyQualifiedName~Examples"
```

## 7. End-to-end traces

### 7.1 Write through a follower

```text
1. Client sends PUT to follower 2.
2. Host encodes a deterministic command with a request ID.
3. Follower's Raft core forwards MsgProp to the known leader.
4. Leader appends the command with its current term and a new index.
5. Ready exposes the unstable entry; host persists it.
6. Leader sends MsgApp to followers.
7. Followers verify previous index/term, append, persist, and acknowledge.
8. Leader computes the quorum index.
9. Current-term commit rule advances commit.
10. Ready exposes the committed entry on every node.
11. application.db transaction applies the command and physicalApplied.
12. Originating node completes the matching request waiter.
13. HTTP response is returned.
```

The client response means the originating node has durably applied the
committed command, not merely that a leader received it.

### 7.2 Linearizable read through a follower

```text
1. Client sends GET to follower 3.
2. Follower forwards a ReadIndex request to the leader.
3. Leader waits for a current-term commit if necessary.
4. Leader confirms authority through quorum heartbeat acknowledgements.
5. Follower receives ReadState(index, requestId).
6. Host waits until application physicalApplied >= index.
7. Host reads application.db and returns the value.
```

### 7.3 Leader failure

```text
leader fails
  -> followers stop receiving valid leader traffic
  -> randomized election timeout expires
  -> a candidate requests votes
  -> a majority elects a new leader
  -> new leader appends a no-op
  -> current-term entry commits
  -> normal writes and ReadIndex continue
```

The failed node may later restart and catch up through retained entries or a
snapshot.

### 7.4 Loss of quorum

In a three-node cluster:

```text
3 reachable nodes -> available
2 mutually reachable nodes -> available
1 isolated node -> no confirmed writes or linearizable reads
```

The isolated node may still serve an explicitly local stale read. Returning a
confirmed write from a minority would violate Raft safety.

## 8. Manual laboratory

Use three terminals from the repository root. The commands below keep learning
data under `artifacts/learning-cluster`.

### 8.1 Start the nodes

Terminal 1:

```bash
dotnet run --project examples/DotnetRaft.KvCluster -c Debug --no-restore -- \
  --Raft:NodeId=1 \
  --Raft:HttpPort=7101 \
  --Raft:GrpcPort=7201 \
  --Raft:DataDirectory="$PWD/artifacts/learning-cluster/node-1"
```

Terminal 2:

```bash
dotnet run --project examples/DotnetRaft.KvCluster -c Debug --no-restore -- \
  --Raft:NodeId=2 \
  --Raft:HttpPort=7102 \
  --Raft:GrpcPort=7202 \
  --Raft:DataDirectory="$PWD/artifacts/learning-cluster/node-2"
```

Terminal 3:

```bash
dotnet run --project examples/DotnetRaft.KvCluster -c Debug --no-restore -- \
  --Raft:NodeId=3 \
  --Raft:HttpPort=7103 \
  --Raft:GrpcPort=7203 \
  --Raft:DataDirectory="$PWD/artifacts/learning-cluster/node-3"
```

Wait for election or request a campaign:

```bash
curl -X POST http://127.0.0.1:7101/campaign
```

Inspect all nodes:

```bash
curl http://127.0.0.1:7101/status
curl http://127.0.0.1:7102/status
curl http://127.0.0.1:7103/status
```

Record:

- leader ID;
- term;
- commit index;
- logical applied index; and
- physical applied index.

### 8.2 Write through a nonleader

Choose a node whose status says it is not leader:

```bash
curl -X PUT http://127.0.0.1:7102/kv/color \
  -H 'content-type: application/json' \
  -d '{"value":"blue","requestId":"11111111-2222-3333-4444-555555555555"}'
```

Read through a different node:

```bash
curl http://127.0.0.1:7103/kv/color
curl http://127.0.0.1:7103/local/color
```

Explain why the first endpoint is linearizable and why the second endpoint is
allowed to be stale.

### 8.3 Verify durable request deduplication

Repeat the exact PUT with the same request ID:

```bash
curl -X PUT http://127.0.0.1:7102/kv/color \
  -H 'content-type: application/json' \
  -d '{"value":"blue","requestId":"11111111-2222-3333-4444-555555555555"}'
```

The response reports a duplicate and preserves the original result index.

Reuse the same ID for a different command:

```bash
curl -i -X PUT http://127.0.0.1:7102/kv/color \
  -H 'content-type: application/json' \
  -d '{"value":"green","requestId":"11111111-2222-3333-4444-555555555555"}'
```

The host returns HTTP 409 because one request identity cannot represent two
different commands.

### 8.4 Fail the leader

Stop the leader with `Ctrl-C`. Use the leader ID recorded in section 8.1
rather than assuming node 1 was leader. Node `N` uses HTTP port `710N`. Query
the two surviving URLs from this table until one reports itself as leader:

| Failed leader | Surviving status URLs |
|---:|---|
| 1 | `http://127.0.0.1:7102/status`, `http://127.0.0.1:7103/status` |
| 2 | `http://127.0.0.1:7101/status`, `http://127.0.0.1:7103/status` |
| 3 | `http://127.0.0.1:7101/status`, `http://127.0.0.1:7102/status` |

Write through the surviving nonleader and confirm the cluster still makes
progress with two of three nodes.

Restart the old leader with the same command and data directory. Confirm it
catches up and serves the latest linearizable value.

### 8.5 Lose quorum

After the old leader has restarted, stop any two nodes so exactly one process
remains.

Try a write and a linearizable read. They must not succeed as confirmed quorum
operations. A local read may still return previously applied data.

Restart one stopped node and observe availability return.

### 8.6 Full restart

Stop all three processes cleanly, then restart them with the same directories.
Confirm:

- the term and log recover;
- the KV values recover;
- request deduplication still recognizes the prior request ID; and
- new writes and linearizable reads continue.

### 8.7 Automated crash and snapshot laboratory

The repository also contains a bounded real-process test:

```bash
dotnet build examples/DotnetRaft.KvCluster -c Release --no-restore
./eng/smoke-durable-kv-cluster.sh "$PWD"
```

It requires `curl`, `jq`, and `python3`, plus unused loopback ports
`17101`-`17103` and `17201`-`17203`.

It exercises:

- follower-originated writes;
- leader-view convergence and retry-safe transient mutation failures;
- an offline follower;
- a 31 MiB value and snapshot catch-up;
- leader `SIGKILL`;
- quorum progress while the leader is down;
- old-leader recovery; and
- complete cluster restart.

On failure it prints the three node logs before cleanup.

## 9. Tests as an executable specification

For each concept, read one design section, one production method, and one test
that names the expected behavior.

Example:

```text
Concept: current-term commit rule
Design:  docs/design/d14-basic-log-replication.md
Code:    RaftCore.MaybeCommit
Test:    CurrentTermRuleBlocksOlderEntriesUntilNoOpIsAcknowledged
```

Recommended test-reading method:

1. read the test name only and predict the outcome;
2. draw the initial term, log, role, and progress state;
3. list the input messages in order;
4. predict state changes and emitted messages;
5. read the assertions;
6. run the isolated test; and
7. only then read the production method.

For multi-step scenarios, inspect:

- [Interaction environment](../tests/DotnetRaft.Tests/Interaction/InteractionEnvironment.cs)
- [Pinned interaction corpus](../tests/DotnetRaft.Tests/Interaction/PinnedTestData)
- [Interaction harness design](design/d23-interaction-harness.md)

The pinned corpus contains 28 upstream scenario files and 558 cases. It is the
best place to study partitions, message delivery, membership changes,
snapshots, `PreVote`, and asynchronous storage as sequences rather than
isolated methods.

## 10. Common misconceptions

### "Raft is a distributed database"

Raft is a replicated-log consensus algorithm. A database product must still
provide transport, durable storage, an application state machine, client
semantics, snapshots, recovery, observability, and operations.

### "If an entry is on a majority, it is always committed immediately"

A leader advances commit through a current-term entry. Majority replication
of only an older-term entry is insufficient by itself.

### "Committed and applied mean the same thing"

Committed means safe in the replicated log. Applied means delivered to the
application. A durable product may additionally distinguish logical admission
from physical completion.

### "A heartbeat contains no useful state"

Heartbeats carry term and commit information, maintain leader authority, drive
`ReadIndex` quorum confirmation, and may trigger catch-up appends.

### "Followers cannot accept writes"

A follower cannot decide or append a client command as leader, but it may
forward a proposal to the known leader. The durable KV example accepts HTTP
mutations on any node for this reason.

### "Two nodes tolerate one failure"

A majority of two is two. A two-node cluster loses availability when either
node is unavailable.

### "Snapshots replace persistence"

Snapshots compact an already durable state prefix. They do not remove the
need to persist term, vote, log suffix, and application state safely.

### "ReadIndex means the read is already safe to execute"

`ReadIndex` returns a required index. The host must still wait until the
application has physically applied through that index.

### "Request IDs make the network exactly once"

The network remains retrying and failure-prone. Durable request IDs let the
application make retries idempotent by recognizing the same logical command.

## 11. Review questions and answers

### 11.1 Why does a new leader append a no-op?

It creates an entry in the new leader's current term. Committing that entry
establishes current leadership authority, permits safe release of gated
`ReadIndex` requests, and commits preceding safe entries.

### 11.2 Why persist a vote before sending the grant?

Without durable vote state, a crash could let the node grant another vote in
the same term after restart, violating election safety.

### 11.3 Why can a follower delete an uncommitted suffix?

The leader's matching previous `(index, term)` proves the shared prefix.
Conflicting entries after that point were never committed under Raft's safety
rules and can be replaced by the leader's log.

### 11.4 Why is `Match` conservative while `Next` can be optimistic?

`Match` records proof from acknowledgements. `Next` is only the leader's next
attempt and can be moved forward optimistically in replicate mode.

### 11.5 Why apply membership at the entry's log position?

Quorum rules are replicated state. Applying them earlier or based on local
observations could make nodes use different voter sets for the same log
position.

### 11.6 Why can one isolated node not acknowledge a write?

It cannot prove that the entry will survive future leader elections. A
majority intersection is what carries committed information into future
quorums.

### 11.7 Why are Raft and application databases separate in the example?

Consensus storage and application state have different schemas and lifecycle
responsibilities. The separation exposes the real recovery boundary and is
similar to production systems that separate a WAL/consensus log from an
application backend.

The two files must still be backed up and restored as one compatible recovery
set. Crash reconciliation can replay or restore supported in-flight states,
but it cannot prove that every independently captured, valid-looking pair came
from the same application generation.

## 12. Suggested four-week plan

| Session | Topic | Deliverable |
|---:|---|---|
| 1 | Mental model, terms, roles, quorum | Explain two-node and three-node availability |
| 2 | Protocol schema and quorum code | Calculate vote and commit outcomes by hand |
| 3 | Stable/unstable storage | Draw one logical log across both layers |
| 4 | RaftLog conflict and cursors | Explain `MaybeAppend` and cursor invariants |
| 5 | Elections | Trace timeout to durable vote and leader |
| 6 | Basic replication | Trace one proposal to commit |
| 7 | Progress and flow control | Explain probe, replicate, snapshot |
| 8 | Snapshots | Trace compacted-follower recovery |
| 9 | Membership | Explain a joint voter transition |
| 10 | ReadIndex and availability | Explain a follower linearizable read |
| 11 | RawNode and Ready | Write the safe host-processing order from memory |
| 12 | RaftNode and async storage | Explain ownership and cancellation |
| 13 | SQLite durability and recovery | Explain pending application snapshots |
| 14 | Durable KV laboratory | Demonstrate failure, restart, and deduplication |

At the end of each session, write:

1. one invariant;
2. one failure scenario;
3. the production method that handles it; and
4. the test that proves it.

## 13. Capstone exercises

### Capstone A: Explain one committed write

Using actual `/status` output and test names, explain:

- who was leader;
- which term created the entry;
- which two nodes formed the quorum;
- when the entry became durable;
- when it became committed;
- when each node physically applied it; and
- why the client response was safe.

### Capstone B: Diagnose an unavailable cluster

Given:

```text
node 1 can contact node 2
node 2 can contact node 1
node 3 is offline
```

Explain why the cluster remains available.

Then change the scenario:

```text
node 1 is isolated
node 2 is isolated
node 3 is offline
```

Explain why no node can safely confirm a write.

### Capstone C: Design a new host

Without copying the KV example, write a host checklist covering:

- timer ownership;
- network message validation;
- Ready persistence order;
- application transaction boundaries;
- physical-applied tracking;
- request deduplication;
- snapshot creation and restore;
- compaction safety;
- startup reconciliation; and
- behavior without quorum.

Compare your checklist with [the public integration guide](public-api.md).

## 14. Completion checklist

You are ready to move beyond introductory Raft when you can answer all of
these without reading the code:

- [ ] Why does a majority intersection protect committed entries?
- [ ] Why does vote persistence precede a vote response?
- [ ] Why does a leader append a current-term no-op?
- [ ] Why can old-term majority replication be insufficient for commitment?
- [ ] What are `Match` and `Next`?
- [ ] When does progress enter snapshot state?
- [ ] What is the difference between committed and physically applied?
- [ ] What exact work is carried by `Ready`?
- [ ] What must happen before `Advance`?
- [ ] Why does `ReadIndex` still need an application barrier?
- [ ] Why does joint consensus require two majorities?
- [ ] Why can a three-node cluster tolerate one failure but a two-node cluster
      cannot?
- [ ] Why are `raft.db` and `application.db` recovered together but not stored
      in one transaction?
- [ ] Which responsibilities remain outside the Raft library?

## 15. Local reference index

Start with these documents:

1. [Reference architecture](reference-architecture.md)
2. [Implementation DAG](implementation-dag.md)
3. [Public API and host integration](public-api.md)
4. [Behavioral parity matrix](parity-matrix.md)
5. [RawNode and Ready](design/d21-rawnode-ready.md)
6. [Concurrent RaftNode](design/d24-concurrent-node.md)
7. [SQLite durable storage](design/sqlite-durable-storage.md)
8. [Durable KV host](design/durable-grpc-kv-host.md)

Use the design sequence D04-D26 when you need a deeper explanation of one
component. Use the tests when you need a precise, executable answer.
