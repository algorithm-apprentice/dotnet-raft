# ADR 0001: Porting Strategy and Architectural Boundary

- **Status:** Accepted
- **Date:** 2026-09-30
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Context

`etcd/raft` is not a complete distributed server. It is a deterministic Raft
state-machine library. Applications provide network transport, durable log
storage, snapshot persistence, and the replicated state machine.

A direct file-by-file translation would preserve incidental Go structure,
create artificial dependency cycles inside the root package, and hide the
algorithm behind concurrency plumbing. The C# implementation needs a stable
boundary and an implementation order that exposes the protocol incrementally.

## Decision

1. **Port behavior, invariants, and externally observable state transitions.**
   The implementation is not a line-by-line translation and does not attempt
   source compatibility with Go.
2. **Keep the same library boundary as `etcd/raft`.** The project implements
   consensus. It does not implement a transport, WAL, database, or client API.
3. **Build a deterministic, single-threaded core first.** `RawNode` is the
   primary integration boundary. A concurrent `Node` wrapper is added only
   after the core and `Ready`/`Advance` contract are complete.
4. **Use the Raft protobuf schema as the protocol model.** C# classes will be
   generated at build time with `Grpc.Tools` and use `Google.Protobuf`. This
   preserves optional-field semantics, cloning behavior, and encoded-size
   calculations used by flow control.
5. **Start with one production assembly and one test assembly.** Logical
   components use namespaces and internal types instead of premature assembly
   boundaries. Assemblies may be split only if a demonstrated deployment or
   dependency requirement appears.
6. **Target `net10.0`.** The initial implementation uses the installed .NET 10
   SDK and current language features without multi-targeting.
7. **Implement strictly through the dependency DAG.** A node may start only
   after every predecessor is complete. Independent ready nodes are still
   implemented sequentially using the selected stable topological order.
8. **Treat tests as executable protocol specifications.** Relevant
   `etcd/raft` tests are translated with each capability instead of postponing
   validation until the end.
9. **Preserve license provenance.** The repository will carry the Apache 2.0
   license and identify the pinned `etcd/raft` source snapshot. Derived files
   will retain appropriate attribution.

## Non-goals

- Providing a production network transport.
- Providing a disk-backed WAL or snapshot store.
- Implementing the user's replicated state machine.
- Preserving Go-specific channels, goroutines, pointer conventions, or API
  names where they do not improve C# semantics.
- Optimizing before behavioral parity and invariant coverage are established.

## Consequences

- The algorithm can be tested without threads, wall-clock time, sockets, or
  disks.
- Persistence ordering remains explicit through `Ready` plus either
  `Advance` in synchronous mode or ordered local storage response messages in
  asynchronous mode.
- Generated protocol types add two package dependencies but avoid a later
  domain-model migration and make size-based behavior faithful to the
  reference.
- The first useful milestone is intentionally smaller than full feature
  parity, but no temporary architecture is introduced for it.
- The DAG and its acceptance gates become part of the project contract; scope
  changes require updating this ADR or adding a superseding ADR.

## Alternatives rejected

### Mechanical file-by-file translation

Rejected because files in the Go root package freely reference one another.
Their file graph is not a useful implementation graph, and translating it
directly would mix core consensus work with host concurrency and diagnostics.

### Implement the concurrent `Node` API first

Rejected because channel scheduling would obscure protocol behavior and make
failures harder to reproduce. The deterministic `RawNode` boundary is the
better learning and testing surface.

### Hand-written protocol records

Rejected because optional protobuf fields, cloning, encoded sizes, and
configuration-change payloads affect behavior. Recreating these details
manually would introduce avoidable semantic drift.

### Multiple production assemblies from the start

Rejected as unnecessary complexity. The implementation DAG describes logical
dependency boundaries without forcing deployment boundaries.
