# Reference Architecture

The first sections describe the pinned Go reference. For the implementation
in this repository, start at [the C# component map](#implemented-c-core-boundaries).
The [host integration contract](public-api.md) defines the current public
behavior and recovery requirements.

## Reference snapshot

The analysis is based on `etcd-io/raft` commit
`1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`. The module contains six internal
packages:

```mermaid
flowchart LR
    rafttest --> raft
    rafttest --> raftpb
    rafttest --> tracker

    raft --> confchange
    raft --> quorum
    raft --> raftpb
    raft --> tracker

    confchange --> quorum
    confchange --> raftpb
    confchange --> tracker

    tracker --> quorum
    tracker --> raftpb
```

`raftpb` and `quorum` are leaves. `tracker` builds on both.
`confchange` builds on `tracker`. The root `raft` package composes every
algorithmic component, and `rafttest` is an external black-box harness.

## Architectural boundary

The reference library delegates network transport, durable writes, and
application effects to its host. Its core can be modeled as:

```text
current state + Message/Tick
              |
              v
       deterministic Raft step
              |
              v
Ready {
  state to persist,
  entries to persist,
  messages to send,
  entries to apply
}
              |
              v
application persists, sends, and applies
              |
              v
Advance / local storage response
```

This boundary is the most important design choice to preserve. A message may
represent a remote RPC, a local timer event, a proposal, or completion of local
storage work. The core does not perform the publication or transport steps
itself, but synchronously queries the host's stable storage. `IStorage`
reads may block on disk, including when `AsyncStorageWrites` is enabled.

## Component map

| Component | Reference files | Responsibility | Direct logical dependencies |
|---|---|---|---|
| Protocol model | `raftpb/raft.proto`, `raftpb/*.go` | Entries, messages, hard state, snapshots, and membership-change payloads | None |
| Quorum algebra | `quorum/*.go` | Majority and joint-quorum vote and commit calculations | Node identifiers and indexes |
| Replication tracking | `tracker/*.go` | Per-follower progress, inflight windows, voters, learners, votes, and committed index | Protocol model, quorum algebra |
| Configuration changes | `confchange/*.go` | Simple changes, joint consensus, restore, and invariant validation | Protocol model, quorum algebra, tracker |
| Stable storage | `storage.go` | Storage contract and in-memory implementation | Protocol model |
| Unstable log | `log_unstable.go` | Entries and snapshots not yet known to be durable | Protocol model |
| Unified Raft log | `log.go`, `types.go` | Stable/unstable merged view, conflict detection, slicing, commit, and apply cursors | Storage, unstable log, protocol model |
| Read-only tracking | `read_only.go` | Quorum confirmation for linearizable reads | Protocol model, quorum algebra |
| Core state machine | `raft.go` | Terms, roles, ticks, elections, replication, commits, snapshots, membership, and extensions | Every algorithmic component |
| Deterministic API | `rawnode.go`, `bootstrap.go` | `Step`, `Tick`, `Ready`, `Advance`, bootstrap, and reporting APIs | Core state machine |
| Concurrent API | `node.go` | Channel-based serialization and lifecycle around `RawNode` | Deterministic API |
| Status and diagnostics | `status.go`, `util.go`, `state_trace*.go`, `logger.go` | Inspection, formatting, logging, and tracing | Core and tracker |
| External test harness | `rafttest/*`, `interaction_test.go`, `testdata/*` | Message delivery, partitions, storage processing, and golden interaction scenarios | Public Raft API |

## Implemented C# core boundaries

The C# implementation preserves the reference package boundary while
decomposing mutable core ownership:

```mermaid
flowchart TD
    RawNode --> RaftCore
    RaftNode --> RawNode

    RaftCore --> RaftClock
    RaftCore --> RaftOutput
    RaftCore --> RaftRoleState
    RaftCore --> ReadIndexCoordinator
    RaftCore --> ProposalAdmission
    RaftCore --> ProgressTracker
    RaftCore --> RaftLog

    RaftCore --> RoleStrategies
    RoleStrategies --> RaftCore

    ProgressTracker --> Quorum
    ReadIndexCoordinator --> ReadOnlyTracker
    RaftLog --> Storage
    RaftLog --> UnstableLog
```

| C# component | Authoritative responsibility |
|---|---|
| `RaftCore` | Cross-component sequencing, term routing, coordinated reset, log mutation, commit/read ordering, membership orchestration, async storage responses, tracing, and logging |
| `RaftClock` | Election/heartbeat elapsed values, configured ticks, randomized timeout, and overflow-safe tick/reset behavior |
| `RaftOutput` | Immediate and after-append queues, outbound cloning, sender/term normalization, and queue classification |
| `RaftRoleState` | Term, vote, leader ID, role, and leadership transferee |
| `ReadIndexCoordinator` | `ReadOnlyTracker`, gated reads, quorum advancement, and completed `ReadState` output |
| `ProposalAdmission` | Uncommitted payload accounting and pending configuration index |
| `ProgressTracker` | Installed configuration/progress maps, votes, quorum-derived commit, and voter/learner queries |
| Role strategies | Stateless follower, pre-candidate, candidate, and leader message routing resolved from the authoritative role on every dispatch |

Only role strategies reference `RaftCore`, through explicit internal handler
methods. Other collaborators never reference the aggregate. Mutable component
state has one owner; tests use component fixtures or explicit `ForTesting`
hooks rather than writable forwarding properties.

`RawNode` publishes and acknowledges Ready work. `RaftNode` adds concurrency
around `RawNode`, not another implementation of consensus or persistence.
`IStorage` is the core's stable read boundary; its in-memory implementation
and the optional `DotnetRaft.Sqlite` package supply it without putting SQLite
in the core package. `examples/DotnetRaft.KvCluster` composes `RaftNode`,
`SqliteStorage`, transport, and a separate application database. The example
owns disk publication, application effects, and recovery across both stores.
See the [host contract](public-api.md) for ordering at these boundaries.

## Core state

The root `raft` object combines five categories of state:

1. **Persistent consensus state:** current term, vote, and committed index.
2. **Log state:** stable storage, unstable entries, committed/applying/applied
   cursors, and an optional snapshot.
3. **Role state:** follower, pre-candidate, candidate, or leader; current
   leader; election and heartbeat counters.
4. **Leader replication state:** a `Progress` object for each voter or learner,
   including `Match`, `Next`, mode, and inflight append messages.
5. **Extension state:** pending membership changes, read-index requests,
   leadership transfer, quorum activity, and storage-work completion.

Role-specific behavior is selected through a state-specific step function:

- follower: forward proposals, receive append/heartbeat/snapshot messages;
- candidate or pre-candidate: collect votes and step down for a valid leader;
- leader: append proposals, replicate entries, advance commit, and manage
  follower progress.

## Important invariants

The C# port must make these invariants explicit and test them close to the
component that owns them.

### Log invariants

- Log indexes are contiguous.
- Terms never decrease within a log.
- A committed entry is never overwritten.
- `applied <= applying <= committed <= lastIndex`.
- Stable and unstable log ranges form one coherent logical log.
- A leader commits an index through quorum acknowledgement only when the entry
  at that index belongs to the leader's current term.

### Election invariants

- Terms increase monotonically.
- A node grants at most one real vote per term.
- A candidate's log must be at least as up to date as the voter's log.
- A relevant higher-term message normally makes a candidate or leader a
  follower. Pre-vote requests and granted pre-vote responses do not advance
  the term. `CheckQuorum` can also suppress ordinary vote requests during an
  active leader lease. See [term handling](learning/01-safety-and-elections.md#the-actual-election-path-in-c).
- Election timeouts are logical ticks and are randomized in
  `[ElectionTick, 2 * ElectionTick)`.

### Replication invariants

- For each follower, `0 <= Match < Next`.
- An acknowledgement can only raise `Match` within one progress record.
  Role/term resets and configuration reconstruction can reset that record.
- Probe mode has at most one effective append in flight.
- Replicate mode may optimistically advance `Next`, bounded by inflight limits.
- Snapshot mode pauses append replication.
- Append rejection handling never moves `Next` below `Match + 1`.

### Persistence invariants

- Term and vote changes are durable before a granted vote response is sent.
- New log entries are durable before an append acknowledgement is sent.
- Synchronous `Ready` batches are advanced in order. In asynchronous storage
  mode, append and apply each have their own ordered completion stream. See
  [the host contract](public-api.md#asynchronous-storage-writes) for the
  different acknowledgement rules.

### Membership invariants

- Every voter or learner has a corresponding progress record.
- Active learners and voters are disjoint.
- Joint consensus requires a majority of both incoming and outgoing voter
  sets.
- Configuration changes become active when applied, not merely appended.
- Ordinary proposal admission prevents overlapping unapplied configuration
  changes unless `DisableConfChangeValidation` disables that admission check.

## Test architecture

The reference tests form a layered specification:

| Layer | Representative sources | What it specifies |
|---|---|---|
| Leaf units | `quorum/*_test.go`, `tracker/*_test.go`, `storage_test.go`, `log_unstable_test.go`, `log_test.go` | Local data structures and invariants |
| Paper scenarios | `raft_paper_test.go` | Elections, log matching, commitment, and safety rules from the Raft paper |
| Core behavior | `raft_test.go`, `raft_snap_test.go`, `raft_flow_control_test.go` | Full state-machine behavior and extensions |
| API contract | `rawnode_test.go`, `node_test.go` | `Ready`/`Advance`, persistence ordering, and concurrent wrapper behavior |
| Golden interactions | `interaction_test.go`, `testdata/*.txt` | Multi-step cluster scenarios, membership changes, snapshots, pre-vote, and async storage |
| End-to-end harness | `rafttest/network.go`, `rafttest/node.go` | Message loss, delay, disconnect, restart, and application integration |

The current C# test suites are mapped in the [parity matrix](parity-matrix.md).
The [test laboratory](learning/05-concurrency-and-testing.md) explains the
pinned scripts, local owner-loop tests, simulated crash schedules, and real
process smoke. The [implementation DAG](implementation-dag.md) records the
completed port's sequencing, not future implementation work.
