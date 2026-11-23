# Maintain dotnet-raft

Start with the [implementation map](../internals/README.md) to find the
owner of a change. The consensus core, node facade, storage engine, and
example host must not implement the same state transition independently.

## Make a change

1. Identify the contract or invariant that changes. Use the
   [host contract](../integration/README.md) for externally observable
   ordering; use the [decision index](../adr/README.md) for trade-offs.
2. Write a focused failing test first. Prefer a deterministic execution
   that exposes the wrong transition over retries or timing guesses. Use
   the [test map](testing.md) to choose the boundary. If a rule is shared
   across storage modes, exercise both; if durability changes, reopen the
   actual store as well as testing the in-memory model.
3. Fix the behavior in its owner. Keep protocol, public API, and SQLite
   schema approvals unchanged unless a compatibility change is intentional.
4. Update the owning documentation and run the relevant checks below.

The [readability roadmap](readability-roadmap.md) records the scope and
verification gates for organizing the core and tracing a write.
The [input boundary design](input-boundary-design.md) covers parsing peer
input once and keeping diagnostic snapshots out of node scheduling.
The [serialization-boundary audit](serialization-boundary-audit.md) tracks
which domain paths still depend on generated messages.

## Verify and package

| Change | Check |
|---|---|
| Consensus, facade, or host | [Tests and failure labs](testing.md), including the applicable Ready and worker schedules. |
| Storage and recovery | [Test map](testing.md#exercise-the-boundary-that-changed) and the [real-process smoke](../../examples/DotnetRaft.KvCluster/README.md#automated-real-process-smoke). |
| API, wire, package, or schema | [Compatibility and release](release.md). |
| Allocation or throughput | [Benchmarks](benchmarks.md); record a same-environment comparison. |

The [CI workflow](../../.github/workflows/ci.yml) is the executable source
for the full test, format, smoke, and package sequence.

## Maintaining the docs

- Put runnable commands in the [example runbooks](../../examples/README.md)
  or the [installer](../integration/install.md), not in design records.
- Put host rules in [integration](../integration/README.md) and
  backend-specific operations in the
  [SQLite package guide](../../src/DotnetRaft.Sqlite/README.md). Link to the
  matching heading elsewhere rather than copying a contract.
- Use [learning](../learning/README.md) for worked examples and explanations;
  use [internals](../internals/README.md) for code ownership, ordering
  rationale, and a link to the test that distinguishes correct behavior.
- Record decisions and rejected alternatives in [ADRs](../adr/README.md).
  Keep one current explanation rather than a new milestone document for
  every implementation step. Link an earlier outcome only when it helps
  explain a current choice.

The root and SQLite READMEs ship in NuGet packages, so use absolute HTTPS
links there. Use relative links elsewhere within the checkout. The release
tests check local targets, the packaged README links that refer to this
repository, and whether the docs are reachable from the root README.

If a bookmark no longer matches the layout, use the
[old path lookup](documentation-history.md).
