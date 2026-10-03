# Post-Parity RaftCore Refactor Plan

- **Status:** Accepted
- **Date:** 2026-10-03
- **Scope:** behavior-preserving refactor after D26
- **Related decision:** ADR 0003

## Objective

Reduce `RaftCore` responsibilities and mutable-state coupling through standard
refactoring techniques while preserving every D26 behavior, API, wire,
package, diagnostic, and performance contract.

This is not a partial-class/file-organization exercise.

## Safety baseline

Pre-Stage 0 Release coverage:

| Scope | Line | Branch |
|---|---:|---:|
| All instrumented code, including generated protobuf | 86.24% | 76.63% |
| Hand-written production code | 93.56% | not separately emitted by Cobertura |
| `RaftCore` | 95.85% | 95.68% |
| `RawNode` | 98.20% | 93.49% |
| `RaftLog` | 87.28% | 86.61% |
| `UnstableLog` | 94.68% | 93.51% |
| `RaftNode` top-level class | 74.87% | 75.27% |

Most uncovered `RaftCore` lines are impossible enum/default arms, arithmetic
overflow defenses, or invariant failures. Reachable gaps include selected
read/membership reevaluation, V2 trace, default random construction, and
debug-logger paths.

No production refactor starts until the characterization phase passes.

The completed Stage 0 measurements, mutation review, and performance ceilings
are recorded in `post-parity-refactor-safety-baseline.md`.

## Stage 0: strengthen the safety net

### Role/message transcript

Create a deterministic approval matrix for:

```text
Follower
PreCandidate
Candidate
Leader

x every MessageType
```

Each case starts from fresh term-one, three-voter storage with deterministic
election timeout. Messages carry the minimum valid fields for their type.

The transcript records:

- exception type/message;
- role, term, vote, leader, transfer target;
- committed/applied/applying/last and unstable cursors;
- configuration and progress;
- pending proposal accounting;
- immediate/after-append messages;
- read-state output; and
- complete trace event type, status, last index, configuration, message
  scalars, and detail; and
- logger level and exact text.

For term-bearing messages the matrix covers zero, lower, equal, and higher
term variants when valid. Vote/pre-vote messages cover lower/equal/higher
nonzero terms plus the invalid zero-term pre-trace rejection. Storage append
responses cover lower/equal terms and snapshot/no-snapshot variants.

This protects pre-trace vote validation, pre-vote higher-term exceptions,
lease suppression, lower-term routing, role transition before dispatch, and
role-specific handling.

The approval file has no rewrite path in ordinary tests.

### Reachable gap tests

Add focused tests for:

- default randomized constructor timeout range;
- `Campaign()` election wrapper;
- invalid deterministic timeout hook;
- leader misuse of election clock;
- pending-read release before/after current-term commit;
- membership reevaluation that requires a heartbeat;
- V2 configuration proposal tracing;
- enabled debug logger paths; and
- every role-specific proposal/read/transfer route.

### Mutation testing

Add local tool manifest `dotnet-stryker 5.0.0` and checked-in stage
configurations. Stage 0 targets:

```text
Core/RaftCore.cs
Core/RaftLog.cs
Core/UnstableLog.cs
```

Run focused mutation batches. Surviving mutants in reachable consensus logic
must receive tests or an explicit documented equivalence justification.
Generated protobuf and defensive impossible enum arms are excluded.
After extraction, mutation targets follow moved logic into every component and
role strategy. Every applicable stage reruns focused mutation before approval;
survivors must be killed or documented as equivalent/unreachable.

### Gate

Before Stage 1:

- `RaftCore` line coverage is at least 97%;
- `RaftCore` branch coverage is at least 96%;
- no reachable critical branch is intentionally uncovered;
- all 857+ tests and 34 goldens pass; and
- mutation results contain no unexplained surviving consensus mutant.

### Allocation/performance baseline

Keep the D26 release benchmark byte-identical to its provenance commit. Use the
separate `DotnetRaft.RefactorBenchmarks` project for
`allocatedBytesPerOperation`, measured by
`GC.GetAllocatedBytesForCurrentThread` around the same timed region.

For each stage, run three complete benchmark commands before and after:

- checksums must remain identical;
- median allocated bytes/operation may not increase by more than the greater
  of 1% or 16 bytes; exceeding this fails the stage;
- run three complete before/after commands; if the median of their reported
  benchmark medians regresses by more than 10%, the stage fails;
- CI smoke remains non-timing-gating but verifies checksum/allocation fields.

There is no waiver inside this refactor task. A deliberate regression would
require a separate design decision outside this plan.

## Stage 1: encapsulate ProgressTracker installation

Introduce:

```csharp
internal void Install(
    TrackerConfig config,
    ProgressMap progress);
```

`Config` and `Progress` become privately replaceable. Constructor recovery,
snapshot restoration, configuration application, and snapshot response
reassertion use `Install`.

`ProgressTracker` also exposes derived local queries:

```text
Contains(id)
IsLearner(id)
IsVoter(id)
IsSingleton
```

Stage 1 removes the mutable `RaftCore.IsLearner` backing field immediately and
replaces it with a forwarding getter to `Tracker.IsLearner(Id)`. No caller
must synchronize a second learner value after `Install`.

No vote/progress behavior otherwise moves in this stage.

## Stage 2: extract `RaftClock`

Authoritative state:

- election elapsed;
- heartbeat elapsed;
- election/heartbeat tick configuration;
- randomized election timeout; and
- random-offset provider.

Moved behavior:

- election and leader clock ticks;
- elapsed overflow checks;
- reset/randomization;
- deterministic timeout test hook.

`RaftCore` retains role/promotable decisions and leader tick action ordering.
Existing internal clock properties temporarily forward to `RaftClock`, then
tests migrate and setters not required by runtime are removed.

## Stage 3: extract `RaftOutput`

Authoritative state:

- immediate message queue; and
- after-append message queue.

Moved behavior:

- clone-on-enqueue;
- sender/term normalization;
- durable-response classification;
- immediate self-target validation;
- `Has`, `Peek`, and `Take`;

`RaftCore.Send` remains a thin sequencing wrapper:

```text
enqueue first
then MessageSent trace
```

This preserves trace-failure partial-mutation semantics.

## Stage 4: extract `RaftRoleState`

Authoritative state:

- term;
- vote;
- leader ID;
- role; and
- leader transferee.

Moved behavior:

- scalar transition mutations;
- hard/soft-state snapshots;
- term/vote reset;
- transfer start/abort.

Coordinated reset, tracker/log/read reset, leader no-op append, and role trace
remain in `RaftCore`.

No scalar field remains duplicated in the core.

## Stage 5: extract `ReadIndexCoordinator`

Authoritative state:

- `ReadOnlyTracker`;
- gated read-index requests awaiting current-term commit; and
- completed read-state output.

Moved behavior:

- request validation;
- safe-read tracking and acknowledgement;
- FIFO advancement;
- pending release;
- reset;
- detached result production.

It returns explicit outcomes such as:

```text
response to send
heartbeat required
read state completed
```

It never sends messages, changes role, or reads wall time. `RaftCore` retains
role routing, voter/singleton decisions, heartbeat transmission, and
commit-before-release sequencing.

## Stage 6: extract `ProposalAdmission`

Authoritative state:

- uncommitted payload bytes; and
- pending configuration index.

Moved behavior:

- pure quota/pending preparation;
- quota commit;
- overflow defense;
- applied payload release;
- pending configuration checks;
- reset.

Log index assignment, configuration-entry rewriting, append, broadcast, and
auto-leave orchestration remain in `RaftCore`.

`ProposalAdmission.Prepare` is mutation-free and returns the candidate payload
total and pending configuration index. Exact ordering remains:

```text
prepare entries/reservation without mutation
trace accepted configuration proposals
append entries to RaftLog
commit UncommittedSize in ProposalAdmission
trace EntriesAppended
enqueue and trace self MsgAppResp
publish PendingConfigurationIndex in ProposalAdmission
broadcast append
```

If tracing or append fails, partial state matches the current boundary at that
exact point. During application, `AppliedTo` and possible auto-leave run before
payload quota release, in both synchronous Advance and async apply-response
paths.

## Stage 7: replace role switches with State/Strategy

Add stateless singleton implementations:

```text
FollowerRoleStrategy
PreCandidateRoleStrategy
CandidateRoleStrategy
LeaderRoleStrategy
```

They replace role switches for:

- `MsgHup`;
- proposal handling;
- leader-message recognition;
- timeout-now;
- vote/pre-vote responses;
- read-index routing;
- transfer routing;
- heartbeat/append responses; and
- leader-only local messages.

Tick routing is intentionally retained by `RaftCore`: clock calculation moves
to `RaftClock`, while quorum-check, transfer-expiry, role recheck, and
heartbeat action order remain explicit orchestration.

Complete role-conditioned routing:

| Message/path | Follower | PreCandidate | Candidate | Leader |
|---|---|---|---|---|
| `MsgHup` | campaign when eligible | campaign when eligible | campaign when eligible | ignore |
| `MsgProp` | forward/drop | drop | drop | append/broadcast |
| `MsgBeat` | ignore | ignore | ignore | broadcast heartbeat |
| `MsgCheckQuorum` | ignore | ignore | ignore | quorum check |
| `MsgForgetLeader` | lease-aware forget | ignore | ignore | ignore |
| `MsgTransferLeader` | forward/ignore | ignore | ignore | transfer |
| `MsgTimeoutNow` | transfer campaign | ignore | ignore | ignore |
| `MsgAppResp` / `MsgHeartbeatResp` | ignore | ignore | ignore | handle |
| `MsgReadIndex` | forward/ignore | ignore | ignore | serve/track |
| `MsgReadIndexResp` | complete read state | ignore | ignore | ignore |
| `MsgUnreachable` / `MsgSnapStatus` | ignore | ignore | ignore | handle |
| `MsgApp` / `MsgHeartbeat` / `MsgSnap` | accept leader then handle | become follower then handle | become follower then handle | ignore |
| `MsgVoteResp` | ignore | ignore | tally | ignore |
| `MsgPreVoteResp` | ignore | tally | ignore | ignore |

`MsgStorageAppendResp`/`MsgStorageApplyResp`, vote requests, lower-term special
handling, and term transitions remain common and occur before strategy
dispatch.

`MsgStorageAppend` and `MsgStorageApply` requests are rejected by the public
`RawNode`/`RaftNode` validation boundary before core tracing. If internal tests
step them directly into `RaftCore`, current behavior is retained: input trace
and term comparison occur, then the request is a role-independent no-op before
strategy dispatch.

Common processing remains before strategy dispatch:

1. zero-term vote validation;
2. input trace;
3. term comparison/transition or lower-term special handling;
4. `MsgHup`/vote request common entry routing where applicable;
5. async storage responses; and
6. role strategy.

Strategies are resolved from the authoritative `RaftRole` each dispatch;
there is no second mutable strategy field. They allocate nothing per message.

Stage 7 is delivered in four reviewed sub-stages:

1. introduce strategies that delegate to existing handlers;
2. move candidate/pre-candidate routing;
3. move follower routing; and
4. move leader routing and remove old role switches.

## Stage 8: remove migration shims

- Remove duplicate forwarding setters and temporary aliases.
- Remove the temporary `RaftCore.IsLearner` forwarding property after all
  internal callers/tests use `ProgressTracker.IsLearner(Id)` directly.
- Move direct-state tests to component fixtures or explicit test hooks.
- Confirm only one owner for every extracted state value.
- Update internal architecture documentation and diagrams.
- Do not split remaining methods merely to reduce line count.

## Sequential delivery

Each stage is one coherent implementation unit:

1. focused tests first;
2. implementation;
3. focused and full validation;
4. independent GPT-5.6 Sol review;
5. commit and push; and
6. only then begin the next stage.

## Validation for every stage

- focused component/core tests;
- Debug and Release full suites;
- 34 interaction goldens twice;
- 32 deterministic randomized scenarios;
- API and protobuf approval manifests;
- formatting and diff checks;
- package build/verifier;
- clean-commit package reproducibility when source paths change; and
- benchmark smoke plus checksum/allocation comparison; and
- stage-specific mutation results.

No public API, wire descriptor, golden output, package metadata, benchmark
checksum, allocation increase beyond the stated measurement tolerance, or
three-run median performance regression over 10% is accepted.

## Acceptance criteria

The refactor is complete when:

1. Stages 0-8 are complete in order;
2. components own the documented state exclusively;
3. central role conditionals are replaced by reviewed strategies;
4. safety-critical cross-component ordering remains explicit in `RaftCore`;
5. no migration shim or duplicated mutable state remains;
6. every validation gate passes;
7. independent review approves each stage and the final architecture; and
8. all refactor commits are pushed.
