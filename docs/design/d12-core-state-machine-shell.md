# D12 Core State-Machine Shell Design

Historical [port milestone](README.md#completed-reference-port). The original
aggregate layout was replaced by the
[C# component boundaries](../reference-architecture.md#implemented-c-core-boundaries).

D12 established deterministic initialization and separated immediate
messages from messages that must wait for the required durable write. At
startup, retained membership and hard state are installed before the node
processes traffic. A logical tick is an input to the state machine, not a
timer created by it.

These boundaries outlast the original layout. For example,
[`RaftOutput`](../../src/DotnetRaft/Core/RaftOutput.cs) now owns the two
queues, while `RawNode` exposes them according to the
[host contract](../public-api.md). See the
[initialization tests](../../tests/DotnetRaft.Tests/Core/RaftCoreInitializationTests.cs)
for retained-boundary and restored-membership cases.
