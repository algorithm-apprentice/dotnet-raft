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
| Entry and log | **Migrated.** `RaftLog`, `Ready`, and `IStorage` use owned domain [`Entry`](../../src/DotnetRaft/Protocol/Entry.cs) values. `IStorage.LogFormat` measures encoded entries. | Forwarding and storage keep presence and extensions. The encoded-size budget remains distinct from the command payload budget. |
| Snapshot and membership | **Migrated.** Snapshot, configuration, and change objects are domain types. `DotnetRaft.Protobuf` owns the original membership and snapshot encoding for storage and transport. | Snapshot and pending marker ordering, joint quorum and bootstrap, the wire approval, and both SQLite schemas stay unchanged. |
| Peer protocol | **Migrated.** `Step`, `StepAsync`, and role dispatch consume domain [`Message`](../../src/DotnetRaft/Protocol/Message.cs) values. The gRPC adapter decodes and encodes them. | Reject malformed input before mutation. Keep term gates, local-work isolation, and forwarded extensions. |
| Local work and read barriers | **Migrated.** `Ready` has typed append and apply work; completion receipts return to the issuing node separately from peer traffic. Internal request contexts use [`RaftBytes`](../../src/DotnetRaft/Protocol/RaftBytes.cs). | Separate queues, durable reply release, nonreplayable receipts, and byte-wise context identity. |
| Tests, examples, and tools | Protocol fixtures, raw compatibility tests, and `Sqlite.CrashHarness` may construct protobufs to test encoding. Consensus simulations, benchmark work, and teaching examples must migrate when their domain APIs move. | Preserve deterministic Counter traces, interaction goldens, wire/schema approvals, old storage recovery, and real-process smoke. Update managed API approvals only for intentional changes. |

## Regression gates

The completed migration is recorded in [ADR 0011](../adr/0011-complete-domain-codec-separation.md).
Keep the core project independently buildable without runtime packages. The
assembly reference test checks this boundary; the adapter owns all generated
messages. Run the descriptor and schema approvals, codec round trips, typed
worker tests, legacy recovery, and real-process smoke when the boundary changes.

The [3.0 review follow-up](serialization-boundary-review.md) records the
receipt rejection, test coverage, transport allocation, and host guide fixes.
