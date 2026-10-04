# Durable gRPC KV Host Design

- **Status:** Implemented
- **Date:** 2026-10-04
- **Scope:** crash-recoverable fixed three-node example
- **Related decision:** ADR 0007

## Objective

Turn the existing educational gRPC cluster into a minimal durable distributed
KV store while preserving its fixed-membership and teaching-oriented scope.

## Configuration

`ClusterOptions` adds:

```text
Raft:DataDirectory
Raft:SnapshotThresholdEntries
Raft:MaxTransportMessageBytes
```

When `DataDirectory` is omitted:

```text
<current working directory>/data/node-<NodeId>
```

Validation requires:

- an absolute normalized local path after resolution;
- positive snapshot threshold;
- transport maximum greater than 128 KiB;
- the existing three loopback peers and port/timeouts; and
- distinct node data directories in multi-process documentation/tests.

Each node owns:

```text
<DataDirectory>/raft.db
<DataDirectory>/application.db
```

## Projects and dependencies

`examples/DotnetRaft.KvCluster` references:

- `DotnetRaft`;
- `DotnetRaft.Sqlite`;
- `Microsoft.Data.Sqlite 10.0.12`; and
- the existing gRPC packages.

The example remains non-packable.

## Replicated commands

```csharp
public enum KvCommandType
{
    Set = 1,
    Delete = 2,
}

public sealed record KvCommand(
    Guid RequestId,
    KvCommandType Type,
    string Key,
    string? Value);
```

Validation:

- request ID nonempty;
- key nonempty;
- set requires a value;
- delete requires a null value;
- unknown command types fail decoding.

Encoding is deterministic UTF-8 JSON. SHA-256 of the encoded bytes is the
durable deduplication fingerprint.

## Application database

Schema version 1:

```sql
CREATE TABLE app_metadata (
    singleton        INTEGER PRIMARY KEY CHECK (singleton = 1),
    format           TEXT NOT NULL,
    schema_version   INTEGER NOT NULL,
    node_id          BLOB NOT NULL CHECK (length(node_id) = 8),
    physical_applied BLOB NOT NULL CHECK (length(physical_applied) = 8),
    conf_state       BLOB NOT NULL
) STRICT;

CREATE TABLE kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID, STRICT;

CREATE TABLE requests (
    request_id   BLOB PRIMARY KEY CHECK (length(request_id) = 16),
    command_hash BLOB NOT NULL CHECK (length(command_hash) = 32),
    result_index BLOB NOT NULL CHECK (length(result_index) = 8)
) WITHOUT ROWID, STRICT;

PRAGMA user_version = 1;
```

Indexes use the same eight-byte big-endian encoding as
`DotnetRaft.Sqlite`.

The store uses:

```text
locking_mode=EXCLUSIVE
journal_mode=WAL
synchronous=FULL
fullfsync=ON
checkpoint_fullfsync=ON
```

One private monitor serializes all connection access.

`node_id` is immutable. Opening a populated application database with another
configured node ID fails before Raft starts. The store reports whether it was
newly created; a missing/recreated application database beside nonempty Raft
state is always rejected. Process-crash recovery assumes both durable database
files remain present.

## Application invariants

Metadata always represents one physically complete prefix:

```text
physicalApplied = highest transactionally applied committed entry
ConfState       = membership after exactly physicalApplied
```

Every apply operation requires:

```text
entry.Index == physicalApplied + 1
```

and commits the cursor in the same transaction as its effects.

## Mutation application and deduplication

Before proposing, the host queries `requests`:

- absent: register a pending proposal and propose;
- matching hash: return the stored result index as a durable duplicate;
- different hash: return conflict without proposing.

The pending registry stores `(requestId, expectedHash, completion)`.
Same-ID/same-hash local callers share one completion. Same-ID/different-hash
callers fail immediately. Application completion supplies the committed hash;
a mismatching winner completes the waiter as conflict rather than
acknowledging another command.

The owning registration starts one shared proposal submission. Its cancellation
token is owned by the registry entry rather than by the first HTTP caller, and
is canceled only after every waiter leaves or the request completes. A timed
out owner therefore cannot remove the only queued proposal while another
identical waiter is still active.

When applying a committed command:

1. read existing request row;
2. if absent, execute set/delete and insert request result;
3. if hash matches, perform no KV effect and return the original result index;
4. if hash differs, perform no KV effect and return deterministic conflict;
5. update physical-applied to the current log index; and
6. commit one FULL transaction.

The pending origin receives:

```text
request ID
original result index
current physical-applied index
duplicate flag
conflict flag
```

## Configuration and no-op entries

For bootstrap indexes 1-3:

1. validate the exact generated entry;
2. call `ApplyConfChangeAsync`;
3. transactionally persist the expected configuration prefix for that
   bootstrap index and the physical index.

`RaftNode.Start` installs the complete peer set internally before it emits the
generated bootstrap entries, so `ApplyConfChangeAsync` can report all three
voters while index 1 or 2 is being applied. Persisting the deterministic
prefix, rather than that preinstalled runtime result, preserves the
application invariant at every crash boundary.

Empty normal entries and rejected later configuration entries transactionally
advance only physical-applied.

The application store validates recovered configuration:

```text
Applied = 0 -> no voters
Applied = 1 -> first sorted peer
Applied = 2 -> first two sorted peers
Applied >= 3 -> all three sorted peers
```

No accepted configuration change exists after index 3 in the fixed-membership
host.

## Snapshot format

Application snapshot version 1 is deterministic JSON:

```text
version
physicalApplied
ConfState protobuf bytes
sorted [{ key, value }]
sorted [{ requestId, commandHash, resultIndex }]
```

Restore validates:

- version;
- payload physical index equals Raft snapshot index;
- payload configuration equals Raft snapshot `ConfState`;
- unique sorted keys and request IDs;
- request hash/index lengths; and
- no result index above physical-applied.

KV rows are materialized and sorted with `StringComparer.Ordinal` before
serialization, and restore uses the same UTF-16 ordinal comparison. SQLite
`BINARY` collation is not used as the snapshot ordering contract because its
UTF-8 byte order differs for some supplementary Unicode keys.

Restore replaces metadata, KV rows, and request rows in one FULL transaction.
Repeating the same restore is idempotent.

After a live Ready snapshot is restored and acknowledged, pending proposal
registrations are resolved against the restored request table. Matching
requests complete as durable duplicates and mismatching fingerprints complete
as conflicts.

## Ready processing

For one Ready:

```text
raftStorage.PersistReady
send Ready.Messages in order
if Ready.Snapshot:
    applicationStore.Restore
    repair HardState.Commit if required
    raftStorage.AcknowledgeApplicationSnapshot
apply every Ready.CommittedEntry transactionally
complete or defer Ready.ReadStates until physicalApplied reaches their index
AdvanceAsync
maybe create application snapshot and compact
```

No application effect occurs before Raft persistence.

## Transport size boundary

The default maximum serialized Raft envelope is 64 MiB.

- ASP.NET Core gRPC send/receive limits use the same configured value;
- Kestrel's HTTP request-body limit uses the same configured value rather than
  its smaller default;
- `RaftConfig.MaxSizePerMessage` reserves envelope overhead below that value;
- commands exceeding the supported payload are rejected before proposal with
  HTTP 413;
- outbound oversize messages return unavailable/snapshot-failure semantics
  instead of faulting the Ready loop; and
- if application snapshot bytes exceed the transport maximum minus envelope
  reserve, the host retains log entries and skips compaction rather than
  creating a snapshot no peer can receive.

## Startup and recovery

1. open `raft.db` and `application.db`, validating immutable local node ID;
2. if Raft has a pending application snapshot:
   - restore it transactionally;
   - require trusted hard-state term;
   - repair commit upward only when needed;
   - acknowledge the exact snapshot;
3. read the retained Raft snapshot;
4. when application physical-applied is below that snapshot index, restore the
   retained snapshot transactionally even if it has no pending marker;
5. read application physical-applied and `ConfState`;
6. validate fixed-membership configuration for that cursor;
7. require physical-applied not beyond retained Raft last index;
8. require or repair `HardState.Commit >= physicalApplied` only when retained
   log/snapshot proves the cursor;
9. when physical-applied exceeds the retained snapshot index, serialize the
   current application state and durably create/compact a matching local Raft
   snapshot before node construction when it fits the transport-safe payload;
10. if the matching application snapshot is too large, retain the previous
    transportable snapshot and the complete log suffix instead of compacting
    into an undeliverable recovery point;
11. require the retained snapshot not to exceed physical-applied, require the
    retained prefix to be covered by that snapshot, and require its
    `ConfState` to equal application `ConfState`;
12. if both stores are logically empty, call `RaftNode.Start` with sorted
    peers;
13. otherwise call `RaftNode.Restart` with
   `RaftConfig.Applied = physicalApplied`.

Committed entries after physical-applied are emitted and reapplied by the
normal Ready loop.

If only application storage is nonempty while Raft storage is empty, startup
fails. If application storage was newly created while Raft is nonempty, startup
fails even when a retained snapshot or full log could rebuild application
content. Deleting the application database also deletes the immutable local
node-ID binding and is treated as storage corruption. An existing application
database at physical index zero may replay from zero only when the retained
snapshot index is zero and the log starts at index 1.

A startup latch guards inbound messages, campaign, mutation, and ReadIndex.
It completes only after the complete fixed bootstrap prefix is physically
applied, advanced, and reconciled with durable configuration. Host failure
completes the latch exceptionally.

## Snapshot and compaction policy

After `AdvanceAsync`:

1. read physical-applied;
2. read retained Raft snapshot index;
3. create a snapshot when:
   - the retained snapshot configuration does not represent the current
     bootstrap/application cursor; or
   - `physicalApplied - snapshotIndex >= SnapshotThresholdEntries`;
4. serialize the application snapshot under the application-store monitor;
5. call `raftStorage.CreateSnapshot` with exact application `ConfState`;
6. call `raftStorage.Compact` at the same index.

Compaction never precedes `Advance` or durable application snapshot creation.
If serialization exceeds the transport-safe size, the host records that
attempted physical index and waits another configured threshold interval
before retrying. The prior transportable snapshot and retained suffix remain
the recovery point.

## HTTP API

```text
PUT    /kv/{key}       { value, requestId? }
DELETE /kv/{key}?requestId=<guid>
GET    /kv/{key}       linearizable
GET    /local/{key}    potentially stale
GET    /status
POST   /campaign
```

Mutation responses report duplicate/conflict state. Conflicting request ID
reuse maps to HTTP 409.

When request ID is omitted, the generated ID is returned only with a
successful response. Such calls are not safely retryable after an unknown
timeout outcome. Retry-safe clients supply and reuse their own ID.

## Lifecycle

The host continues supervising Ready, tick, and `RaftNode.Completion`.
Shutdown order:

1. cancel network/tick/Ready work;
2. stop and dispose `RaftNode`;
3. dispose transport;
4. host DI disposes application and Raft SQLite stores.

Any application database, schema, decode, restore, or persistence failure
faults the host and stops ASP.NET.

## Testing

### Component

- options and data-directory validation;
- command set/delete encoding and fingerprints;
- application schema approval;
- set/delete/no-op/configuration transactions and reopen;
- identical and conflicting request IDs;
- shared proposal submission lifetime across caller cancellation;
- restored request rows completing live proposal waiters;
- deferred read barriers completing only after physical application;
- deterministic snapshot bytes and idempotent restore;
- cursor/configuration invariants;
- wrong-node-ID reopen and missing/recreated application database rejection.

### Deterministic integration

- three nodes bootstrap durably;
- follower proposal and linearizable read;
- stop all hosts, recreate from the same directories, and continue;
- duplicate request retry after restart;
- different commands concurrently proposed under one request ID, proving the
  losing waiter receives conflict;
- oversized application snapshots wait another threshold interval before a
  new serialization attempt.

### Recovery components

- application-ahead recovery creates and compacts a matching local snapshot;
- application-behind recovery restores the retained snapshot;
- replay from zero behind a compacted prefix is rejected;
- a recoverable application-ahead cursor repairs HardState commit;
- missing/recreated application databases beside nonempty Raft fail startup;
- oversized recovery snapshots preserve the prior transportable snapshot and
  retained suffix; and
- pending snapshot recovery is idempotent at restore, commit-repair, and
  acknowledgement crash cuts.

### Real processes

- launch three Kestrel processes with separate data directories;
- elect and write through a follower;
- kill the leader by PID;
- use SIGKILL for the crash phase and bounded curl/process deadlines;
- restart it with the same directory;
- continue quorum writes while it is down;
- verify the restarted node catches up and serves a linearizable read;
- stop and restart all nodes, then verify retained data.

Pending-snapshot recovery additionally terminates at:

```text
Raft snapshot committed / application not restored
application restored / commit not repaired
commit repaired / marker not acknowledged
marker acknowledged / node not constructed
```

Every restart must preserve hard-state term/vote, restore idempotently, clear
only the exact marker, and continue linearizable reads/writes.

Wall-clock timeouts are deadlock guards only. Process IDs are tracked
explicitly; no name-based process termination is used.

## Non-goals

- MVCC history;
- compare/transaction;
- watch;
- lease/TTL;
- auth;
- dynamic membership;
- sharding;
- cross-database transactions;
- deduplication pruning;
- online application-schema migration;
- TLS or deployment automation; and
- etcd-compatible APIs.

## Validation sequence

1. design review;
2. failing component/integration/recovery tests;
3. implementation;
4. recovery crash-cut tests and real-process crash smoke;
5. focused coverage/mutation where valuable;
6. full Debug/Release and formatting;
7. benchmark and package/reproducibility gates;
8. independent GPT-5.6 Sol code review;
9. commit, push, and green CI.

## Implementation evidence

The completed implementation has the following local evidence:

- 998 core/example tests and 33 SQLite tests pass in both Debug and Release;
- focused tests for durable example behavior pass as part of the core/example
  suite;
- `SqliteKeyValueStateMachine` reaches 94.37% line and 74.59% branch coverage;
- `DurableHostRecovery` reaches 89.54% line and 78.78% branch coverage;
- `DurableKvCommandCodec` reaches 100% line and 92.85% branch coverage;
- `PendingProposalRegistry` reaches 81.96% line and 76.31% branch coverage;
- `PendingReadRegistry` reaches 91.66% line and 87.50% branch coverage;
- the final pre-review focused mutation run exercised 513 mutants, killing
  295 and producing three reviewed liveness timeouts; subsequent review fixes
  have dedicated regression tests instead of another full mutation rerun;
- both benchmark smoke suites preserve their expected checksums and allocation
  fields; and
- three consecutive real-process runs passed follower-originated writes,
  31 MiB HTTP ingestion and snapshot catch-up, SIGKILL leader loss, quorum
  progress, old-leader recovery, and full-cluster restart.
