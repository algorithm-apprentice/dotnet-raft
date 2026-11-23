# Audit the serialization boundaries

See [ADR 0010](../adr/0010-separate-state-from-serialization.md) for the
dependency rule. The target is a domain model with no generated types or
protobuf runtime dependency. A protobuf reference in a codec or a wire-format
test is expected. Moving an unchanged generated object behind a differently
named property does not satisfy the rule.

## Audit results

| Area | Finding and follow-up | Evidence to preserve |
|---|---|---|
| Hard state | **Migrated.** [`RaftHardState`](../../src/DotnetRaft/RaftHardState.cs) is used by the core, Ready, memory and SQLite storage, diagnostics, and recovery. Only the disk/async append adapters use protobuf for these values. | Nullable update versus an explicit zero vote; term and commit ordering; skipped flush for a commit-only change; old SQLite blob/read compatibility. |
| KV commands and deduplication | **Migrated for ordinary commands.** [`RaftClusterHost.Ready`](../../examples/DotnetRaft.KvCluster/RaftClusterHost.Ready.cs) maps committed entries to `CommittedKvEntry` before application; the request registry and results use `KvCommandFingerprint`, not `ByteString`. Snapshot and membership work is covered below. | Ordered application, no-op cursor advance, stable request digest bytes, batch rollback, and deduplication after restart/snapshot. |
| Entry and log | **Open.** [`RaftLog`](../../src/DotnetRaft/Core/RaftLog.cs), [`EntrySizing`](../../src/DotnetRaft/Core/EntrySizing.cs), [`Ready`](../../src/DotnetRaft/Ready.cs), and [`IStorage`](../../src/DotnetRaft/Storage/IStorage.cs) still use generated entries. Define owned domain entries and carry encoding evidence from the adapter. | Entries forwarded or persisted must retain presence and unknown fields. Byte quotas have distinct encoded-entry and payload meanings; retain budget and oversize-first-entry tests. |
| Snapshot and membership | **Open.** Generated snapshots and `ConfState` still cross the storage API and enter [configuration and restore](../../src/DotnetRaft/Core/RaftCore.Membership.cs). `ParsedProposal` and the configuration code use generated changes, and KV stores protobuf configuration metadata. | Snapshot boundary and pending marker ordering; exact membership prefix; joint quorum and deterministic bootstrap; compatibility of `.proto` and both SQLite schemas. |
| Peer protocol | **Open.** `RawNode.Step`, `RaftNode.StepAsync`, the role dispatch, and outgoing messages still consume/produce generated `Message`. gRPC envelope encoding/decoding belongs at the adapter and is not a violation. | Parse once and reject malformed input before mutation; preserve term/role gates, peer routing, local-only message isolation, and unknown-field forwarding. |
| Local work and read barriers | **Partly migrated.** [`ReadState`](../../src/DotnetRaft/ReadState.cs) exposes detached .NET bytes, but internal ReadIndex tracking still uses protobuf for peer context. Async append/apply work and completions still use `Message`, although they never travel on the network. | Represent local work and completions explicitly while preserving both Ready modes, durable reply release, cursor reconciliation, and byte-wise context identity. |
| Tests, examples, and tools | Protocol fixtures, raw compatibility tests, and `Sqlite.CrashHarness` may construct protobufs to test encoding. Consensus simulations, benchmark work, and teaching examples must migrate when their domain APIs move. | Preserve deterministic Counter traces, interaction goldens, wire/schema approvals, old storage recovery, and real-process smoke. Update managed API approvals only for intentional changes. |

## Migration order

1. Define domain entry and snapshot/configuration ownership and adapters. Verify
   round trips, forwarding evidence, and sizes before switching the log/store
   and their public Ready fields.
2. Replace peer commands and response shapes at the ingress/egress boundary
   using those entries and snapshots. Keep a single transition owner, and move
   shape validation into the adapter without moving term or quorum decisions.
3. Replace local work/completions, read contexts, and their diagnostics with
   typed domain inputs and outputs. Remove any remaining generated references
   from the consensus and application contracts before claiming the split is
   complete. Once those types are isolated, enforce the dependency direction
   by compiling the domain without protobuf, not by inspecting namespace spelling.
