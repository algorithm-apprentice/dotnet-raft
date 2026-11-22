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
applies one entry at a time, checking it is the next index. The
[schema approval](../../tests/DotnetRaft.Tests/Examples/TestData/DurableApplicationSchema.approved.sql)
records the exact tables; the design depends on the cursor, configuration,
and command result committing together, not on a specific SQL listing here.

## Follow a fresh KV write

Start with the [Counter write trace](../../examples/DotnetRaft.Counter/README.md#trace-one-write)
in Ready/Advance mode. For a new request ID with no stored result, follow
the same host phases in the KV example:

| Phase | Counter | Durable KV host |
|---|---|---|
| Submit | `Program` calls `RawNode.Propose` directly. | [`Program` maps `PUT /kv/{key}`](../../examples/DotnetRaft.KvCluster/Program.cs#L85) to `PutAsync`. [`RaftClusterHost.MutateAsync`](../../examples/DotnetRaft.KvCluster/RaftClusterHost.cs#L503) registers and awaits a pending result; [`SubmitProposalAsync`](../../examples/DotnetRaft.KvCluster/RaftClusterHost.cs#L648) calls `RaftNode.ProposeAsync`. A follower can forward the proposal. |
| Save and send | `CounterCluster.ProcessReady` writes to memory and enqueues peer messages. | [`ProcessReadyAsync`](../../examples/DotnetRaft.KvCluster/RaftClusterHost.cs#L374) calls `SqliteStorage.PersistReady` for `raft.db` before using the gRPC transport. Neither operation sets the KV value. |
| Apply and finish the caller | `CounterReplica.Apply` updates the local counter. | [`ApplyEntryAsync`](../../examples/DotnetRaft.KvCluster/RaftClusterHost.cs#L430) calls [`SqliteKeyValueStateMachine.ApplyCommand`](../../examples/DotnetRaft.KvCluster/SqliteKeyValueStateMachine.cs#L150), which commits the value, request result, and physical cursor to `application.db` together. Then `pendingProposals.Complete` releases the local waiter so the endpoint can return success. |
| Acknowledge to Raft | `ProcessReady` calls `Advance` on that batch. | [`RunReadyLoopAsync`](../../examples/DotnetRaft.KvCluster/RaftClusterHost.cs#L353) calls `AdvanceAsync` after `ProcessReadyAsync`. This is not what releases the HTTP waiter. |

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
[pending registry](../../examples/DotnetRaft.KvCluster/PendingOperations.cs)
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

## Testing

The [component tests](../../tests/DotnetRaft.Tests/Examples/DurableKvComponentTests.cs)
exercise exact bootstrap prefixes, deduplication across snapshots, repeated
pending restore, and recovery without compacting into an oversized snapshot.
The [integration tests](../../tests/DotnetRaft.Tests/Examples/KvClusterIntegrationTests.cs)
cover delivery and restart; the [process smoke](../../eng/smoke-durable-kv-cluster.sh)
runs the separately hosted cluster. A green process-crash run does not
establish that arbitrary independently captured database copies are compatible.
