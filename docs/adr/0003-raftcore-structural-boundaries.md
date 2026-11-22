# ADR 0003: One owner for mutable consensus state

- **Status:** Accepted
- **Date:** 2026-10-03

## Context

Moving methods among partial classes can make a large state machine easier
to browse but does not make its state ownership explicit. Extracting a
service that updates shared fields independently would make reset, commit,
and snapshot ordering harder to see.

## Decision

Move state together with the behavior that owns it. Keep clocks, role state,
outbound work, admission, read coordination, progress, and the log as
separate owners. `ProgressTracker.Install` is the replacement boundary for
configuration and progress. Route messages by the current role after shared
term handling; never select a route using the role from before the transition.

`RaftCore` sequences transitions involving multiple owners. The original
stateless role strategy objects only routed back to core methods. Private
role dispatch removes that extra jump without changing who owns the state.
Partial files make election, replication, membership, and read flows easier
to find; they are a reading aid, not separate owners. Collaborators do not
reach back into the core to mutate one another. The
[component map](../internals/consensus.md#coordinate-transitions-in-one-place)
describes the resulting boundaries.

## Trade-offs and evidence

The design adds no dispatcher queue, role object, or per-message allocation.
Behavior-preserving edits should use
[dispatch characterization](../../tests/DotnetRaft.Tests/Core/RaftCoreDispatchCharacterizationTests.cs)
to compare effects, not just return values. Use
[the same benchmark workload](../maintaining/benchmarks.md) to check any
cost change. A message bus or independent replication service would hide
the ordering we want the core to enforce explicitly.
