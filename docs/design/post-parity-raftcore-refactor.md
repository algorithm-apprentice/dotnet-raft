# Post-Parity RaftCore Refactor Plan

Completed work, not a backlog. [ADR 0003](../adr/0003-raftcore-structural-boundaries.md)
records the decision; the [C# architecture](../reference-architecture.md#implemented-c-core-boundaries)
shows current ownership. The [design archive](README.md#completed-reference-port)
preserves the full plan.

The refactor moved mutable state with its behavior rather than just splitting
a large file. It first put configuration/progress replacement behind an
installation boundary, then extracted clocks, output, role state, read
coordination, and proposal admission. Stateless role strategies replaced
dispatch switches only after that ownership was explicit. `RaftCore` kept
the operations where the ordering of more than one component matters.

To distinguish reorganization from behavior changes, the work first froze
[role/message transcripts](../../tests/DotnetRaft.Tests/Core/RaftCoreDispatchCharacterizationTests.cs).
The final cleanup removed migration forwarding setters rather than leaving
two places able to change the same state. The
[recorded baseline](post-parity-refactor-safety-baseline.md) explains how
mutation and allocation measurements complemented those transcripts.
