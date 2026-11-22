# ADR 0007: Recover the KV host across two stores

- **Status:** Accepted
- **Date:** 2026-10-04

This supersedes the volatile application and storage choices of
[ADR 0004](0004-educational-grpc-kv-cluster.md), not its teaching scope.

## Context

Raft may make a command durable before the application runs it; application
may commit before the node sees `Advance`. A received snapshot can be
durable on one side of that boundary without its bytes being installed on
the other. A timed-out client also cannot infer whether a write took effect.

## Decision

Keep Raft storage reusable and separate from application storage. Commit
effects, request outcomes, configuration, and the physical application
cursor together on the application side. Resolve incoming snapshot restores
explicitly, then restart at the recovered physical cursor with matching
membership. Preserve both stores as one recovery set. Recovering only one
database is not a supported substitute for reconciling that set.

Use durable client request identities and include their results in
application snapshots; a retry should not replay an effect after catch-up.
Keep the example fixed-membership and stop compaction before it would leave
lagging nodes unable to receive any usable prefix.

## Trade-offs

This demonstrates real cross-store recovery without claiming cross-store
atomicity. Keeping request history increases storage and snapshot size.
A separate recovery protocol is needed for coordinated online backup or
safe pruning. See the [KV host design](../internals/kv-host.md) for current
application and startup mechanics and the
[runbook](../../examples/DotnetRaft.KvCluster/README.md) for operation.
