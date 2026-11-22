# Implementation DAG

D00–D26 were the work packages of the completed reference port. They are
historical labels, not a backlog or the current module graph. See the
[C# component map](reference-architecture.md#implemented-c-core-boundaries)
for ownership and the [parity matrix](parity-matrix.md) for executable coverage.

## Milestones

The port ordered work by dependency: build the protocol model and quorum
math first, then the log and elections, then replication and higher-level
protocols. Host facades followed the core, and release validation closed the
port. The work packages below were completed in ascending ID order.

| Stage | Work packages | Outcome |
|---|---|---|
| Foundation | D00–D03 | Buildable solution, generated protocol model, shared types, quorum algebra. |
| Data structures | D04–D11 | Stable and unstable log, replication progress, membership changes, and read-only tracker. |
| Election core | D12–D13 | Deterministic initialization, clocks, and election state machine. |
| Replication | D14–D16 | Log matching and commit, flow control, and snapshot recovery. |
| Protocol extensions | D17–D20 | Live membership, safe and lease reads, availability, and leader transfer. |
| Host APIs | D21–D25 | Ready/Advance, bootstrap and diagnostics, interaction harness, concurrent calls, and async storage writes. |
| Release | D26 | Verified parity corpus, package and API baselines, CI, and benchmark workloads. |

<a name="dependency-graph"></a>

## Work packages

The [milestone notes](design/README.md#completed-reference-port) link each D04–D26
topic to current reading and evidence. The
[original DAG and acceptance gates](https://github.com/algorithm-apprentice/dotnet-raft/blob/45160468cb7a0a3465c5fbebf088b5998188890b/docs/implementation-dag.md#dependency-graph)
are preserved in Git.
