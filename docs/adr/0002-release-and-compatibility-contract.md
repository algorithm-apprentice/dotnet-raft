# ADR 0002: Release and Compatibility Contract

- **Status:** Accepted
- **Date:** 2026-10-03
- **Decision owners:** dotnet-raft maintainers

## Context

D00-D25 complete the declared synchronous and asynchronous `etcd/raft`
behavioral port. D26 must turn that implementation into a reproducible,
documented package without inventing compatibility or migration machinery for
deployed users that do not yet exist.

The repository already exposes public protocol, storage, `RawNode`, status,
diagnostic, and concurrent `RaftNode` APIs. Packaging them without a recorded
baseline would make later refactoring unable to distinguish accidental API
breakage from intentional evolution.

## Decision

1. The first package version is `DotnetRaft` `1.0.0`, targeting `net10.0`.
2. The package is an educational consensus library, not a transport, gRPC
   service, durable database, or replicated application.
3. Public API compatibility begins at D26:
   - a checked-in reflection-generated API manifest records the shipped
     surface;
   - later intentional breaking changes require a new ADR and appropriate
     semantic-version change; and
   - pre-D26 commits are not treated as released compatibility baselines.
4. Semantic-version compatibility covers:
   - managed binary API;
   - C# source API, including nullability, named-argument parameter names,
     optional defaults, generic constraints, and accessor modifiers;
   - protobuf wire field numbers, types, cardinality, and enum numeric values;
   - persisted log/snapshot restart compatibility;
   - D21-D25 host ordering, durability, recovery, Ready, and local-storage
     protocols;
   - supported target frameworks; and
   - dependency-major requirements visible to consumers.
5. Version classification is:
   - **major:** any incompatible change in the dimensions above, removal of a
     target framework, or dependency-major change requiring consumer
     migration;
   - **minor:** backward-compatible additive API/protobuf fields or enum
     values, an additional target framework, or opt-in behavior that preserves
     existing defaults and protocols; and
   - **patch:** compatible bug fixes, documentation/diagnostic improvements,
     performance work, and dependency patch/minor updates that preserve the
     declared contracts.
6. The package includes:
   - `DotnetRaft.dll`;
   - portable symbols in a `.snupkg`;
   - `README.md`;
   - `LICENSE`; and
   - `THIRD-PARTY-NOTICES.md`.
7. Runtime package dependencies include `Google.Protobuf`; `Grpc.Tools`
   remains build-private and must not appear as a consumer dependency.
8. The package is tag-ready but D26 does not publish to nuget.org or create a
   Git tag automatically.
9. Behavioral parity is documented in a feature/test matrix. Intentional
   .NET/API deviations are listed explicitly rather than hidden.
10. Performance baselines are informative and reproducible, not flaky CI
   thresholds. CI runs a smoke workload; maintainers capture comparative
   baselines before performance-sensitive changes.

## Consequences

### Positive

- Consumers receive complete license and provenance metadata.
- Package contents and dependencies are mechanically verified.
- The first stable API boundary is explicit.
- Post-D26 internal refactoring can rely on both behavioral tests and an API
  manifest.
- Benchmarks can detect major regressions without making unit tests timing
  dependent.

### Tradeoffs

- `net10.0` consumers require the .NET 10 runtime/toolchain.
- Generated protobuf public types are part of the initial API manifest.
- API evolution after 1.0.0 requires explicit compatibility decisions.
- The repository maintains package, parity, and benchmark documentation.

## Rejected alternatives

- **Publish as `0.x` indefinitely:** rejected because D26 completes the
  declared feature graph and establishes an explicit supported baseline.
- **Multi-target older frameworks now:** rejected because no consumer
  requirement justifies the additional compatibility/test matrix.
- **Add migration shims before any released data/API dependency exists:**
  rejected as premature complexity.
- **Gate CI on wall-clock benchmark thresholds:** rejected as hardware-noisy
  and likely to make tests flaky.
- **Omit generated protocol types from compatibility tracking:** rejected
  because they are public package types used by every host integration.
