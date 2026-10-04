# RaftNode Concurrency Hardening Design

- **Status:** Implemented
- **Date:** 2026-10-04
- **Scope:** deterministic verification and surgical fixes
- **Related decision:** ADR 0005

## Objective

Build a deterministic regression boundary around `RaftNode` before any later
concurrency refactor. The work verifies observable ownership and completion
semantics; it does not redesign the concurrent facade.

## Baseline

Release coverage collected with the full test suite:

| Metric | Result |
|---|---:|
| `RaftNode` line coverage | 77.53% |
| `RaftNode` branch coverage | 75.27% |
| `RawNode` line coverage | 98.20% |
| `RawNode` branch coverage | 93.49% |

Focused Stryker baseline for `RaftNode.cs`:

| Outcome | Count |
|---|---:|
| killed | 115 |
| survived | 110 |
| timeout | 81 |
| no coverage | 74 |
| compile error | 46 |
| ignored | 165 |
| reported score | 51.58% |

The baseline is not an acceptance result. A mutation in `TryPublishReady`
causes CS0165 for `hasReady`, after which Stryker removes the entire method in
Safe Mode. The first production change initializes that local without
changing runtime behavior.

## Invariants under test

### Single ownership

- only the owner loop calls `RawNode`;
- caller continuations never execute inline on the owner;
- trace and logger callbacks reject same-node reentry;
- ordinary commands, eligible proposals, and ticks each receive fair
  scheduler turns;
- proposals remain FIFO within their lane.

### Request state

Every cancellable request follows exactly one path:

```text
waiting -> claimed -> completed
       \-> canceled
       \-> terminally failed
```

- cancellation can transition only `waiting -> canceled`;
- claim removes the cancellation registration before dispatch;
- a canceled request never mutates `RawNode`;
- a claimed request completes with its operation result or original
  operation exception;
- every queued request reaches one terminal task state.

### Ready ownership

- one waiter can be stored only when no incompatible waiter or batch exists;
- canceled waiters cannot consume a batch;
- synchronous storage mode stores the exact returned Ready until one
  successful advance;
- asynchronous storage mode never stores an outstanding Ready and rejects
  `AdvanceAsync`;
- stop or fault invalidates all unpublished or outstanding work.

### Proposal admission

- proposals remain FIFO while no leader is known;
- cancellation unlinks a waiting proposal and releases its payload;
- observed removal of local progress blocks proposals;
- learner progress remains valid local membership;
- re-addition enables proposal dispatch again;
- stop and fault drain the entire blocked proposal queue.

### Tick behavior

- `Tick` never blocks and never invokes the logger on the caller;
- bounded-channel overflow coalesces into one owner-loop warning per observed
  missed batch;
- a disabled warning logger produces no callback;
- a warning logger exception faults the node;
- stop or fault makes later ticks no-ops.

### Terminal behavior

- stop wins before any later claim;
- already claimed work may finish;
- a `RawNode` or callback fault wins over concurrent normal stop;
- the triggering request receives its original exception;
- all other pending and future calls receive the terminal wrapper;
- terminal status is captured after both normal stop and fault when possible;
- failure to capture diagnostic status never masks the original terminal
  cause.

## Deterministic test synchronization

`BlockingTraceSink` becomes a reusable owner-occupation gate:

```text
owner enters selected trace callback
  -> test observes Entered
  -> test enqueues/cancels/stops/overflows
  -> test signals Release
  -> callback signals Exited in finally
```

The callback waits for `Release` without its own timeout. Tests always release
the gate in `finally`. `Task.WaitAsync` or a bounded event wait remains only
outside the owner callback so a deadlock fails the test without injecting a
`TimeoutException` into production execution.

All bounded waits in the RaftNode tests use one shared ten-second outer
guard. A constrained two-core full-suite run demonstrated that the former
two-second value could expire before the thread-pool scheduled the owner
loop. The larger guard changes only failure latency; successful tests still
complete as soon as their deterministic gate or task settles.

The default trigger remains the first `MessageReceived` trace event. If a
test needs another deterministic boundary, the helper accepts a predicate
rather than adding a production hook.

`ReadyAccepted` provides the post-claim boundary for a Ready request.
`MessageReceived` from an acknowledged self-message provides the post-claim
boundary for synchronous `AdvanceAsync`.

## Test matrix

### Public command and ownership surface

- both configuration proposal overloads clone caller input and share proposal
  gating;
- both configuration application overloads clone caller input and return
  detached state;
- read-index context and proposal bytes are owned before enqueue;
- forget-leader, transfer-leadership, unreachable, and snapshot reports
  dispatch through the owner;
- local-message and unknown-response filtering covers synchronous and
  asynchronous storage modes;
- every public cancellable API has representative pre-canceled coverage;
- stopped and faulted nodes reject representative command, proposal, Ready,
  advance, and status calls immediately.

### Cancellation and claim order

- an ordinary command canceled while the owner is occupied never dispatches;
- a Ready wait canceled before the owner stores or claims it never consumes a
  batch;
- a blocked proposal canceled before eligibility never dispatches;
- cancellation after an ordinary command has entered its callback does not
  replace the operation result;
- stop after claim allows the claimed request to finish but fails later
  queued work.

### Scheduling and continuations

- while all three lanes are eligible, trace ordering proves that a finite
  command backlog cannot be drained before proposals and ticks receive turns;
- distinct blocked proposals are forwarded in FIFO payload order after leader
  discovery;
- an `ExecuteSynchronously` continuation registered before command
  completion can synchronously call an owner-routed API successfully,
  proving request completion did not invoke it inline under the owner marker.

### Ready and advancement

- second pending waiter and waiter-during-outstanding-batch both fail;
- missing and duplicate advance remain nonterminal request errors;
- canceled waiter cleanup allows a later waiter;
- cancellation or stop after `ReadyAccepted` cannot replace the claimed
  Ready result; stop then invalidates later advancement;
- cancellation after synchronous advancement reaches its traced self-message
  cannot replace the advance result;
- `RawNode.Ready` or `Advance` faults preserve trigger-versus-node exception
  semantics;
- asynchronous storage waits can pipeline without advance.

### Stop and fault draining

With the owner deterministically occupied, enqueue:

- one ordinary command;
- one blocked proposal;
- one Ready waiter; and
- more ticks than channel capacity.

Then request stop or trigger a callback fault. Verify every task settles, no
queued state mutation occurs, `Completion` settles last, later calls expose
the correct terminal exception, and `TerminalStatus` is detached and
available.

A separate precedence test combines both outcomes:

1. claim an operation and block inside its callback;
2. enqueue the other request categories;
3. call `StopAsync` so stop linearizes;
4. release the claimed callback into an injected exception; and
5. verify the triggering request receives its original exception while
   queued/future requests, `StopAsync`, and `Completion` receive
   `RaftNodeFaultedException`, not `RaftNodeStoppedException`.

### Tick warning

- overflow produces exactly one warning from the owner thread;
- the warning is emitted only after the occupied callback is released;
- disabled logging consumes the missed flag without calling `Log`;
- logger failure faults the node after the claimed command has completed;
- the previously flaky logger-failure scenario is run in 50 fresh test-host
  processes with no retries or allowed failures.

## Mutation strategy

The focused command is:

```bash
dotnet stryker \
  --config-file stryker-raftnode-config.json \
  --output artifacts/raftnode-mutation
```

The focused configuration disables coverage analysis. The owner loop runs on
a background task, and both per-test and project-level coverage attribution
can miss nested request dispatch that is observably exercised by the tests.
Running the complete fast test project for every focused mutant favors a
trustworthy result over coverage-based skipping.

The review groups survivors by invariant:

- request state and cancellation;
- stop/fault state;
- Ready waiter and outstanding batch;
- proposal membership gate;
- tick overflow and warning;
- scheduler fairness;
- terminal drain and status;
- channel configuration and defensive fallbacks.

Critical state-transition survivors require a stronger test or a production
fix. Critical timeouts are rerun in isolation and must be shown to represent
the expected liveness failure under an outer deadlock guard; helper-induced or
unexplained timeouts fail the gate. Equivalent mutations, diagnostic text
mutations, channel options not observable through the contract, and
lock-protected impossible states may be classified with evidence rather than
hidden through exclusions. The reported percentage is invalid until this
survivor and timeout review is complete.

## Production-change policy

Allowed before a failing behavior test:

- initialize `hasReady` in `TryPublishReady` so mutation instrumentation is
  valid.

Allowed after a failing behavior test:

- a minimal ordering, claim, cleanup, or exception-propagation correction;
- a test-helper-only synchronization improvement;
- documentation needed to keep the contract accurate.

Not allowed in this phase:

- splitting `RaftNode` into services or strategies;
- changing public completion semantics;
- adding retries or buffering policy;
- exposing internal channels or request objects;
- adding sleeps or runtime test switches.

Mutation analysis justified two additional behavior-preserving
simplifications:

- void commands now use `VoidNodeRequest` and `Action<RawNode>` instead of
  returning ignored Boolean sentinels through `OperationRequest<bool>`; and
- `PurgeCanceledReadyWaiter` was removed because cancellation either prevents
  storage, clears the stored waiter under `_claimGate`, or loses to claim.

Independent code review also identified that broad expected-request exception
types could misclassify a logger callback throwing `ArgumentException`.
Logger invocation is now wrapped in an internal `RaftLoggingException`, and
`RawNode` marks tracing and logging callback failures as terminal before the
concurrent wrapper classifies request errors.

## Validation

Inner loop:

```bash
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Release --filter 'FullyQualifiedName~DotnetRaft.Tests.Node'
```

Acceptance:

1. focused Node tests;
2. 50 fresh-process executions of the previously flaky test, with zero
   retries and zero failures:

   ```bash
   for iteration in $(seq 1 50); do
     dotnet test \
       tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
       -c Release --no-build --no-restore \
       --filter \
       'FullyQualifiedName=DotnetRaft.Tests.Node.RaftNodeTickTests.TickWarningLoggerFailureFaultsNode' \
       --verbosity quiet || exit 1
   done
   ```

3. focused Release coverage with at least 90% line / 85% branch for
   `RaftNode`;
4. focused Stryker with no `TryPublishReady` Safe Mode, at least 65%, and no
   unresolved critical survivor or timeout;
5. full Debug and Release suites;
6. formatting verification;
7. benchmark smoke;
8. package verification, isolated consumer, and two-worktree reproducibility;
9. independent GPT-5.6 Sol code review;
10. commit, push, and green CI.

## Validation outcome

### Focused tests and coverage

| Gate | Result |
|---|---:|
| focused Node tests | 58 passed |
| full Release tests during coverage | 971 passed |
| `RaftNode` line coverage | 91.71% |
| `RaftNode` branch coverage | 85.86% |
| fresh-process tick-warning repetitions | 50 / 50 passed |

The callback helper has no internal timeout. All bounded waits remain at the
outer test boundary as deadlock guards. The shared ten-second guard tolerates
CI scheduler contention without weakening any ordering assertion.

### Mutation

The final focused command used `coverage-analysis: off` and tested every
instrumentable `RaftNode` mutant:

| Outcome | Count |
|---|---:|
| assertion-killed | 340 |
| reviewed timeout | 58 |
| survived | 0 |
| no coverage | 0 |
| compile error | 20 |
| ignored by mutate/compiler filters | 158 |

Stryker reports 100%. Treating every timeout conservatively as not
assertion-killed still yields 85.43% (`340 / 398`), above the acceptance
threshold.

The timeout mutants were reviewed by source location and fall into expected
liveness categories:

- removed Ready, command, proposal, advance, or cancellation completion;
- disabled stop cancellation, wake signaling, or terminal draining;
- nonterminating or invalid scheduler/channel loops;
- suppressed tick-warning or callback-fault publication;
- broken waiting/claimed/completed request transitions;
- removed callback reentrancy rejection; or
- removed fault propagation required to settle `Completion`.

No timeout arose from the old callback-internal two-second wait, which no
longer exists. No critical survivor or unexplained timeout remains.
