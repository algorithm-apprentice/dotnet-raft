# Durable gRPC KV Host Design

Part of the [implementation map](README.md). This page explains application
choices in the fixed three-node example. The
[runbook](../../examples/DotnetRaft.KvCluster/README.md) owns commands and
HTTP behavior. The [host contract](../integration/README.md) owns rules
that any service must follow.

## Two stores, one application prefix

`raft.db` owns the stable log, hard state, retained Raft snapshot, and pending
restore marker. `application.db` owns the KV rows, mutation results, physical
cursor and its `ConfState`, and the immutable local node ID. The two databases
do not share a transaction. The app can nevertheless advance its own cursor
atomically with the effects and membership at that index.

[`SqliteKeyValueStateMachine`](../../examples/DotnetRaft.KvCluster/SqliteKeyValueStateMachine.cs)
applies entries in order, checking each is the next index. The
[schema approval](../../tests/DotnetRaft.Tests/Examples/TestData/DurableApplicationSchema.approved.sql)
records the exact tables; the design depends on the cursor, configuration,
and command result committing together, not on a specific SQL listing here.

## Follow a fresh KV write

Start with the [Counter write trace](../../examples/DotnetRaft.Counter/README.md#trace-one-write)
in Ready/Advance mode. For a new request ID with no stored result, follow
the same host phases in the KV example:

| Phase | Counter | Durable KV host |
|---|---|---|
| Submit | `Program.SubmitIncrement` calls `RawNode.Propose` directly. | [`KvHttpEndpoints.PutAsync`](../../examples/DotnetRaft.KvCluster/KvHttpEndpoints.cs) maps `PUT /kv/{key}` to `PutAsync`. [`RaftClusterHost.MutateAsync`](../../examples/DotnetRaft.KvCluster/RaftClusterHost.Requests.cs) registers and awaits a pending result; `SubmitProposalAsync` calls `RaftNode.ProposeAsync`. A follower can forward the proposal. |
| Save and send | `CounterCluster.ProcessSynchronousReady` stores the memory batch before enqueueing messages. | [`ProcessReadyAsync`](../../examples/DotnetRaft.KvCluster/RaftClusterHost.Ready.cs) calls `SqliteStorage.PersistReady` for `raft.db` before `SendMessagesAsync` uses the gRPC transport. Neither operation sets the KV value. |
| Apply and finish the caller | [`CounterReplica.Apply`](../../examples/DotnetRaft.Counter/CounterReplica.cs) updates the local counter. | [`ApplyCommittedEntriesAsync`](../../examples/DotnetRaft.KvCluster/RaftClusterHost.Ready.cs) groups ordinary entries and decodes each batch into `CommittedKvEntry` values. [`ApplyCommittedBatch`](../../examples/DotnetRaft.KvCluster/SqliteKeyValueStateMachine.Apply.cs) then commits the effects, request results, and physical cursor to `application.db` together. `pendingProposals.Complete` releases the local waiters after that transaction. |
| Acknowledge to Raft | `AcknowledgeReady` calls `Advance` on that batch. | [`RunReadyLoopAsync`](../../examples/DotnetRaft.KvCluster/RaftClusterHost.Ready.cs) calls `AdvanceAsync` after `ProcessReadyAsync`. This is not what releases the HTTP waiter. |

[`ThreeNodeClusterReplicatesFollowerProposalAndRead`](../../tests/DotnetRaft.Tests/Examples/KvClusterIntegrationTests.cs)
exercises this path without starting a gRPC server. The next section covers
the different case where a durable request result already exists.

## Mutation application and deduplication

The durable winner for a mutation ID is the first applied command using it.
The application keeps that command's SHA-256 fingerprint and result index.
A matching retry returns the original result; a different command with that
ID conflicts. Neither repeats the KV effect, but both still advance the
physical cursor if they appear in the log.

[`RaftClusterHost`](../../examples/DotnetRaft.KvCluster/RaftClusterHost.cs)
checks durable results before proposing and the owning registration checks
again after registering. A result can become durable in that interval. The
[pending proposal registry](../../examples/DotnetRaft.KvCluster/PendingProposalRegistry.cs)
coalesces matching local callers and owns the submission's cancellation:
one timed-out caller must not cancel work still awaited by another. Apply or
snapshot completion resolves the current waiter against the durable winner,
not against the bytes of a later conflicting entry.

Mutation success follows local durable application or an existing durable
result. It does not follow merely from `ProposeAsync` returning. Reads use
the [physical ReadIndex barrier](../integration/application.md#read-index); the local-read
endpoint makes no linearizability promise.

## Configuration and no-op entries

The host validates and accepts the three bootstrap entries in order. `Start`
preinstalls all three voters in the node, so `ApplyConfChangeAsync` can return
the full set while applying the first or second record. Persist the actual
prefix instead: zero voters at index 0, then the first one, first two, and all
three sorted peers as the respective bootstrap entries apply. Empty entries
and later, rejected configuration changes advance only the physical cursor.
[`FixedMembershipConfiguration`](../../examples/DotnetRaft.KvCluster/FixedMembershipConfiguration.cs)
shares this rule between application and recovery checks.

## Snapshot format

An application snapshot carries the physical index, its exact membership,
KV rows, and all durable mutation results. Omitting the request table would
allow a previously successful mutation to execute again after catch-up.
Serialization and validation use the same ordinal key order; SQLite's UTF-8
`BINARY` order is not equivalent to .NET ordinal order for all Unicode keys.
Restore validates the payload against Raft metadata and replaces all
application tables and metadata in one transaction. It can repeat a restore
at the same index, but refuses to move the application backward.

## Ready processing

The host uses `RaftNode` with [synchronous storage](../integration/ready.md#synchronous-ready-loop).
It persists the batch before sending messages, restores an incoming snapshot
if present, and applies committed entries before `AdvanceAsync`. The same
consumer runs restoration and application in order; it does not use early
admission.

The consumer batches up to 64 consecutive ordinary entries already in
one Ready. It does not delay a write to fill a batch or cross a configuration
entry. A transaction commits the write results and final cursor together;
failure in the batch rolls back even the earlier statements in that batch.
A result's `AppliedIndex` is still the index of the original command;
`PhysicalApplied` can report the end of the same durable batch. See the
[gated application test](../../tests/DotnetRaft.Tests/Examples/KvClusterIntegrationTests.cs)
for rollback, waiter completion, and reopen.

### Pending application snapshot

Live processing and restart share `CompletePendingSnapshot` in
[`DurableHostRecovery`](../../examples/DotnetRaft.KvCluster/DurableHostRecovery.cs).
It completes application restoration, ensures durable hard state and commit
coverage, then clears the exact marker. Recovery also checks that durable
term covers the retained snapshot and log tail; neither an application cursor
nor a snapshot term supplies a replacement term or vote. A failed restore
does not clear the marker or complete client requests from the snapshot.

## Startup and recovery

An absent or recreated application database beside nonempty Raft storage is
an error, even if some data could be replayed. The host cannot reconstruct
the missing database's node-ID binding or assume the pair forms a valid
generation. Treat both databases as one [recovery set](../../examples/DotnetRaft.KvCluster/README.md#durable-files-and-restart).

`Reconcile` finishes any pending restore, then uses a retained snapshot if
the existing application is still behind it. Recovery verifies the physical
cursor, fixed membership, and durable log bounds before constructing the
node. If the trusted application cursor is ahead of the recovered commit,
it repairs only commit as described in the [restart contract](../integration/recovery.md#restart).
Replay starts after the recovered physical cursor, not after the last
volatile `Advance`.

After application has advanced beyond the retained snapshot, recovery tries
to publish a matching snapshot and compact. If the new payload is too large
to transmit, it retains the old snapshot and full suffix. Startup still
requires that the retained prefix is covered and the retained snapshot's
configuration matches the application at its recovered cursor. An old but
transportable snapshot is not permission to restart with stale membership.

The startup gate keeps external Raft input and client requests that depend
on Raft out until the bootstrap prefix is physically applied and the durable
configuration matches. A fresh cluster does not wait for a network quorum
to apply its local bootstrap records.

## Snapshot and compaction policy

While running, the Ready loop considers a new local snapshot after
`AdvanceAsync`, when the application cursor is already physical. It
reconciles a configuration mismatch or takes a snapshot once the configured
entry threshold is reached. It does not compact into an undeliverable
snapshot. The [runbook](../../examples/DotnetRaft.KvCluster/README.md#snapshot-policy)
owns the size settings and retry interval.

Live snapshot publication and recovery share `PublishSnapshotAndCompact` in
`DurableHostRecovery`: durably publish the physical prefix before removing its
log. The writes need not be one transaction; a crash between them retains the
snapshot and the extra prefix. The existing snapshot/reopen component tests
and real-process smoke verify the recovery boundary in both call paths.

## Testing

The [component tests](../../tests/DotnetRaft.Tests/Examples/DurableKvComponentTests.cs)
exercise exact bootstrap prefixes, deduplication across snapshots, repeated
pending restore, and recovery without compacting into an oversized snapshot.
The [integration tests](../../tests/DotnetRaft.Tests/Examples/KvClusterIntegrationTests.cs)
cover delivery and restart; the [process smoke](../../eng/smoke-durable-kv-cluster.sh)
runs the separately hosted cluster. A green process-crash run does not
establish that arbitrary independently captured database copies are compatible.
