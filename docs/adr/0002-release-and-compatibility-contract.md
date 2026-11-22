# ADR 0002: Release and compatibility contract

- **Status:** Accepted
- **Date:** 2026-10-03

## Context

Keeping method signatures stable is not enough for a consensus library. A
host can still break if a message field changes, old state can no longer
restart, or a completion now requires different persistence ordering.

## Decision

The `1.0.0` package baseline targets `net10.0`. Define compatibility across:

- managed binary and C# source API, including nullability, named parameters,
  defaults, constraints, and accessors;
- protobuf field numbers, types, cardinality, and enum values;
- persisted log/snapshot restart and the separately approved SQLite schema;
- [host ordering and recovery](../integration/README.md), including local
  Ready and storage acknowledgement protocols; and
- supported target frameworks and dependency-major requirements that a
  consumer must satisfy.

An incompatible change in these dimensions requires a new ADR and a
**major** version. Use a **minor** version for compatible additions or opt-in
behavior preserving existing defaults, and a **patch** for compatible bug
fixes, documentation and diagnostic improvements, performance changes, or
dependency updates that preserve the contracts.

Check managed API, wire, and schema approvals separately. Keep package
contents, dependencies, symbols, and attribution verifiable. The
[release procedure](../maintaining/release.md) checks these surfaces without
publishing or tagging a package.

## Trade-offs

Generated public message types are part of the managed API, so generator
updates can require compatibility review. SQLite has a separate native
dependency footprint. Timing measurements are comparative, not CI thresholds:
reproducible workloads are useful without requiring identical machine speed.
