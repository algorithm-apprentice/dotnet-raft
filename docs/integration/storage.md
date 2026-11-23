# Implement or choose a storage backend

Part of [host integration](README.md). Use the
[SQLite package](../../src/DotnetRaft.Sqlite/README.md) for a durable backend
with an atomic write API. `MemoryStorage` is useful in deterministic tests
and in-process examples; it does not survive restart of the process.

## Storage reads

[`IStorage`](../../src/DotnetRaft/Storage/IStorage.cs) is a synchronous read
interface. It is not the write side of Ready. Calls may block on disk even
with async storage writes enabled. Synchronize reads with publication so
the node never sees a partial batch. Return detached domain entries,
snapshots, and configurations; the caller must not be able to change the
store by editing a returned object.

| Method | Contract |
|---|---|
| `LogFormat` | Return the stable [`IRaftLogFormat`](../../src/DotnetRaft/Protocol/IRaftLogFormat.cs) used to measure entries and encode/decode replicated membership payloads. |
| `GetInitialState()` | Return the recovered `RaftHardState?` (may be null for a fresh store) and non-null `ConfState` at the [recovery point](recovery.md#restart). |
| `GetFirstIndex()` | First index after the retained boundary; a fresh store starts at 1. The term at `FirstIndex - 1` must remain available. |
| `GetLastIndex()` | Last retained log position, including the boundary if there are no real entries. Require `1 <= FirstIndex <= LastIndex + 1` and a representable successor for `LastIndex`. |
| `GetTerm(index)` | Return the term in `[FirstIndex - 1, LastIndex]`. Throw `StorageException` with `Compacted` below that range and `Unavailable` above it. |
| `GetEntries(lowInclusive, highExclusive, maxSize)` | Return a contiguous prefix of the half-open range, limited by the sum of `LogFormat.MeasureEntry(entry)`. Include its first entry even when the budget is zero or too small for it; do not include another entry if it would exceed the budget. |
| `GetSnapshot()` | Return the retained snapshot with metadata and configuration, or an empty snapshot for an uninitialized store. Throw `StorageException` with `SnapshotTemporarilyUnavailable` if generation is still in progress. |

`GetEntries` reports `Compacted` for a low index below `FirstIndex`. Reject
reversed ranges and high bounds beyond `LastIndex + 1`. A nonempty retained
range must not return an empty success. The built-in stores return an empty
page for a valid empty range with real entries retained, and `Unavailable`
if only the boundary remains. Compaction can require snapshot catch-up;
unavailability inside an expected stable range is not a normal empty page.

### Choose a log format

`MemoryStorage` takes a format in its constructor; `SqliteStorage` uses
[`ProtobufRaftCodec.Instance`](../../src/DotnetRaft.Protobuf/README.md).
For a custom backend, budget the bytes for a standalone encoded entry,
including its fields and retained extension data, not database row overhead,
RPC framing, or just the command payload. Keep the measurement consistent
with the encoded size of existing rows: Raft uses it for both stable and
unstable entries and to account for outstanding application work.

The format also encodes and decodes both kinds of configuration change
stored in the replicated log. Every replica and recovery process must be
able to decode the same membership payloads. Use the protobuf adapter for
the existing `raftpb` format; selecting a different format on one node is
not a protocol negotiation or a migration of the durable log.

## Publishing a storage batch

Publish snapshot, entries, and hard state as one coherent storage generation,
installing an incoming snapshot before appending its suffix. A missing hard
state update leaves the stored value unchanged. A crash may leave the old or
the new complete generation, never post-snapshot entries without their
snapshot or a commit beyond the resulting log. A published Ready snapshot
must be covered by commit.

Atomic publication and forcing a write to durable media are distinct. Use
the [synchronous](ready.md#synchronous-ready-loop) or
[asynchronous](ready.md#append-worker) flush and response rules, and honor
the backend's stronger requirements if it has any. In particular, SQLite's
batch methods run in FULL-synchronization transactions; separate calls for
snapshot, entries, and hard state would not reproduce that atomic boundary.
