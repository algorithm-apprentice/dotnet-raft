# ADR 0005: Deterministic RaftNode Concurrency Verification

- **Status:** Accepted
- **Date:** 2026-10-04
- **Decision owners:** dotnet-raft maintainers

## Context

`RaftNode` is the public concurrent facade around the synchronous
`RawNode`. Its single-owner loop, cancellation claims, Ready rendezvous,
proposal gating, tick coalescing, and terminal draining are now used by the
educational three-node host.

The existing tests establish the principal behavior, but the current Release
baseline leaves important concurrency paths weakly verified:

- `RaftNode` has 77.53% line coverage and 75.27% branch coverage;
- one CI run failed in `TickWarningLoggerFailureFaultsNode` before a retry
  passed;
- `BlockingTraceSink` has a routine two-second wait inside the callback, so a
  slow scheduler can become an injected production fault rather than a test
  deadlock failure; and
- a focused Stryker run reports 51.58%, but CS0165 causes Stryker Safe Mode to
  remove every mutation in `TryPublishReady`, making that score incomplete.

The main risk is incorrect ownership or completion during cancellation, stop,
or terminal-fault races. Structural extraction would move those risks without
first increasing confidence in their current semantics.

## Decision

Harden the existing `RaftNode` architecture through deterministic contract
tests before considering any further production refactor.

1. **Keep the production shape**
   - retain one owner loop, the existing command/proposal/tick lanes, and the
     current public API;
   - do not add a scheduler abstraction, event bus, dependency-injection
     boundary, or production-only test hook;
   - permit only behavior-preserving instrumentation fixes and defects proven
     by a focused test.
2. **Use deterministic synchronization**
   - occupy the owner loop through the existing trace callback boundary;
   - expose explicit entered, release, and exited gates from test helpers;
   - do not put a wall-clock timeout inside a callback running on the owner
     loop;
   - use bounded waits only at the outer test boundary as deadlock guards;
   - do not use sleeps, polling delays, or probabilistic stress loops in the
     normal unit suite.
3. **Verify the linearization rules**
   - cancellation wins only while a request is waiting;
   - a claimed request executes exactly once and ignores later cancellation;
   - the first running-to-stop-requested transition prevents every later
     claim;
   - a terminal fault drains every unclaimed Ready, proposal, command, and
     tick lane;
   - the triggering request receives its original exception while other
     requests receive `RaftNodeFaultedException`;
   - `Completion` settles only after terminal status capture and pending
     request completion.
4. **Make mutation analysis representative**
   - initialize `TryPublishReady` locals so Stryker can instrument the whole
     method without Safe Mode;
   - include `RaftNode.cs` in a focused mutation run;
   - review surviving and timeout mutations by behavior category rather than
     treating a percentage as sufficient evidence;
   - require non-equivalent mutations in critical state transitions to be
     killed or fixed; only proven equivalent or unreachable mutations may be
     accepted;
   - do not treat an unexplained tool timeout as evidence that a behavioral
     assertion detected the mutation.
5. **Treat coverage as a map, not the objective**
   - cover every reachable public API and critical state transition;
   - do not add unsafe hooks or contort production code solely to execute
     invariant fallback branches;
   - document any remaining defensive-only gaps.

## Rationale

Callbacks already execute synchronously under the same owner marker as
`RawNode`, so they provide a faithful way to pause a claimed operation while
other callers enqueue, cancel, stop, or overflow ticks. Explicit gates make
the ordering deterministic without exposing production internals.

Keeping the architecture fixed isolates test discoveries from refactor
effects. Mutation analysis then distinguishes assertions that merely execute
code from assertions that protect the owner-loop protocol.

## Consequences

### Positive

- Concurrency contracts become reproducible on developer machines and CI.
- Cancellation, stop, fault, Ready, and tick behavior can be changed later
  with a stronger regression boundary.
- The focused mutation report becomes valid for `TryPublishReady`.
- Production changes remain small and attributable to a demonstrated defect.

### Tradeoffs

- Some lock-protected invariant fallbacks may remain uncovered.
- Synchronous callback gates can deadlock when a test is incorrect, so every
  test must release them in `finally` and retain an outer deadlock guard.
- Focused mutation testing is slower than ordinary unit testing and remains a
  release-hardening gate rather than a per-edit inner loop.

## Rejected alternatives

- **Refactor `RaftNode` before testing it:** changes ownership and scheduling
  while the current behavioral boundary is incomplete.
- **Add production scheduler hooks:** expands the public or internal runtime
  surface solely for tests.
- **Use sleeps or repeated race loops:** can increase execution count without
  proving which transition won.
- **Chase 100% coverage:** rewards impossible or defensive paths and can make
  the implementation less clear.
- **Accept the current Stryker score:** Safe Mode excludes a central Ready
  publication method, so the result is not representative.

## Acceptance criteria

This decision is implemented when:

1. focused Release coverage reaches at least 90% line and 85% branch for
   `RaftNode`;
2. `TryPublishReady` is mutation-instrumented without method-wide Safe Mode;
3. no routine unit-test synchronization timeout runs inside an owner
   callback;
4. deterministic tests cover waiting-versus-claimed cancellation, queued and
   claimed stop races, Ready exclusivity, proposal gating, tick overflow,
   fault draining, and terminal status;
5. focused mutation reaches at least 65%, with every surviving or timeout
   mutation in a critical transition killed, fixed, or proven
   equivalent/unreachable;
6. the flaky tick-warning scenario passes repeated focused execution;
7. full Debug/Release tests, formatting, package verification,
   reproducibility, and benchmark smoke remain green; and
8. an independent GPT-5.6 Sol review approves the design and implementation.

## Outcome

Implemented without changing the public API or single-owner architecture.

- test synchronization now uses explicit entered, release, and exited gates;
  no timeout runs inside an owner callback;
- `TryPublishReady` is fully mutation-instrumentable without Safe Mode;
- logger callbacks are wrapped distinctly, so even an `ArgumentException`
  cannot be mistaken for a nonterminal request-validation error after state
  mutation;
- void commands use a dedicated void request instead of mutation-equivalent
  Boolean sentinel results;
- the redundant canceled-Ready purge was removed after its claim/cancellation
  states were proven unreachable;
- focused Node tests increased to 58 and remain below 80 milliseconds on the
  development host;
- full Release coverage reached 91.71% line and 85.86% branch for
  `RaftNode`;
- the unfiltered focused mutation run tested all 398 instrumentable mutants:
  340 were assertion-killed, 58 produced reviewed liveness timeouts, none
  survived, and none were skipped for missing coverage;
- the conservative assertion-only mutation score is 85.43% (`340 / 398`);
  Stryker reports 100% when the reviewed timeout detections are included; and
- the formerly flaky tick-warning logger-failure test passed 50 of 50 fresh
  test-host executions with no retries.

A later constrained full-suite run reproduced the same test failure before
the owner loop entered its trace gate. The fixed two-second outer wait was a
test scheduler deadline, not a Raft liveness requirement. RaftNode tests now
share a ten-second outer deadlock guard. The guard remains outside production
callbacks and does not delay successful tests.
