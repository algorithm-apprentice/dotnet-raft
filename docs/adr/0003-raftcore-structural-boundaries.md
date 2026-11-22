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
configuration and progress. Select stateless message strategies from the
current role after shared term handling; never let a cached strategy become
a second source of role state.

`RaftCore` still sequences transitions involving multiple owners. Role
strategies call explicit core handlers; other collaborators do not reach
back into the core to mutate one another. The
[component map](../internals/consensus.md#coordinate-transitions-in-one-place)
describes the resulting boundaries.

## Trade-offs and evidence

The design adds internal method boundaries, but no new dispatcher queue or
per-message strategy allocation. Behavior-preserving edits should use
[dispatch characterization](../../tests/DotnetRaft.Tests/Core/RaftCoreDispatchCharacterizationTests.cs)
to compare effects, not just return values. Use
[the same benchmark workload](../maintaining/benchmarks.md) to check any
cost change. A message bus or independent replication service would hide
the ordering we want the core to enforce explicitly.
