# ADR 0003: RaftCore Collaborator Boundaries

- **Status:** Accepted
- **Date:** 2026-10-03
- **Decision owners:** dotnet-raft maintainers

## Context

Behavioral parity and release hardening are complete. `RaftCore` still owns
more than 2,600 lines of construction, scalar role state, clocks, output
queues, read coordination, proposal accounting, message routing, election,
replication, snapshots, membership, availability, async storage, status, and
diagnostics.

Splitting that code into a partial class would improve navigation but would
not reduce responsibilities, coupling, or duplicated state ownership.

The completed parity suite provides a safe point for standard refactoring
techniques, provided state ownership and safety-critical sequencing remain
explicit.

## Decision

Apply the refactor incrementally with these techniques:

1. **Extract Class / Encapsulate State**
   - `RaftClock` owns all logical clock state and random timeout generation.
   - `RaftOutput` owns immediate and after-append message queues.
   - `RaftRoleState` owns term, vote, leader ID, role, and transferee.
   - `ReadIndexCoordinator` owns `ReadOnlyTracker`, gated read requests, and
     completed read-state production.
   - `ProposalAdmission` owns uncommitted payload accounting and pending
     configuration index.
2. **Encapsulate Collection**
   - `ProgressTracker.Install` becomes the only operation that replaces
     configuration and progress maps.
3. **Move Method**
   - Move behavior with the state it manipulates when it does not coordinate
     multiple safety domains.
4. **Replace Conditional with State/Strategy**
   - Four stateless singleton role strategies replace role switches for
     proposal, leader-message, read, timeout, transfer, and response routing.

`RaftCore` remains the aggregate root and sequencing orchestrator for:

- construction/recovery;
- top-level term handling;
- coordinated reset;
- leader activation/no-op append;
- commit then read release;
- snapshot restoration;
- configuration installation;
- async storage response ordering; and
- trace/logger mutation boundaries.

## Constraints

- No public API, protobuf, persisted format, exception text, log text, trace
  order, message order, or durability rule may change.
- No extracted component or strategy may add a per-message architectural
  allocation. Measured allocation and timing gates are defined by the
  refactor plan and are mandatory.
- Each extracted mutable value has exactly one authoritative owner.
- Temporary forwarding properties are removed before completion.
- Components never reference `RaftCore`.
- Role strategies may receive `RaftCore` as context but mutate only through
  explicit internal methods, never fields.
- No event bus, dependency-injection container, generic command framework, or
  per-message allocation is introduced.

## Consequences

### Positive

- State ownership becomes explicit.
- Role behavior becomes locally reviewable and exhaustively testable.
- The central core focuses on safety-critical orchestration.
- Future changes can target a subsystem without editing the entire state
  machine.

### Tradeoffs

- Internal method boundaries increase.
- Temporary migration shims exist between sequential stages.
- Role strategies still collaborate with the aggregate root by design.
- The refactor requires stronger characterization and mutation tests before
  implementation.

## Outcome

Implemented in sequential reviewed stages.

- `RaftClock` owns every logical clock value and timeout randomization.
- `RaftOutput` owns both message queues and outbound normalization.
- `RaftRoleState` owns term, vote, leader, role, and transfer state.
- `ReadIndexCoordinator` owns safe-read tracking, gated reads, and completed
  read states.
- `ProposalAdmission` owns uncommitted quota and pending configuration state.
- `ProgressTracker.Install` is the only replacement boundary for tracker
  configuration and progress maps.
- Four cached stateless role strategies own role-conditioned message routing.
- `RaftCore` remains the sequencing aggregate for cross-component commit
  points, log mutation, reset orchestration, tracing, and diagnostics.

No mutable value is duplicated, no strategy instance is stored separately from
the authoritative role, and no migration forwarding setter remains.

## Rejected alternatives

- **Partial-class-only split:** cosmetic; does not change responsibilities.
- **Immediate replication service extraction:** too much ordering/state risk
  for one refactor step.
- **Rewrite around an event bus:** obscures deterministic call order.
- **Duplicate old/new state during migration:** risks two authoritative values.
- **Rename and reformat while extracting:** makes behavioral review
  unnecessarily difficult.
