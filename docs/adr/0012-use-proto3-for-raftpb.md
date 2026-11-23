# ADR 0012: Use proto3 without changing the raftpb binary contract

- **Status:** Accepted
- **Date:** 2026-11-24

## Decision

Move the adapter's [`raft.proto`](../../src/DotnetRaft.Protobuf/Protocol/raft.proto)
to proto3. This is a schema source and generator change, not a new protocol
version. The gRPC envelope already uses proto3, and the adapter already uses
`Google.Protobuf` 3.x. Do not change their dependencies, the managed API,
consensus, Ready, or the KV application format. Keep the ownership established
in [ADR 0011](0011-complete-domain-codec-separation.md): only the adapter
generates Raft messages, and it continues to use the same domain mappings.

| Schema elements | Migration rule and reason |
|---|---|
| 30 singular scalars (including enums and bytes) | Keep `optional` on all of them. Absent type is not explicit enum zero; absent data or context is not present-empty; absent `reject` or `auto_leave` is not present-false. The legacy storage append decoder needs `term`, `vote`, and `commit` to be all present or all absent; an explicit zero vote cannot disappear. |
| Three singular messages (`SnapshotMetadata.conf_state`, `Snapshot.metadata`, `Message.snapshot`) | Drop the `optional` label. Proto3 already distinguishes absent messages from present-empty ones and merges occurrences of the same field. No real oneof or new discriminator is needed. |
| Four repeated `uint64` membership fields | Put `[packed = false]` on each. Both readers accept packed and unpacked input, but the writer must continue emitting separate varint tags in existing list order, including inside snapshots. |
| Three repeated message fields and four enums | Preserve field and enum numbers, names, types, and cardinality. Every enum already starts at zero. There are no required fields, custom defaults, or schema extensions to port. Leave the file and package names, namespaces, and generated C# visibility unchanged. |

Proto3 optional scalars introduce synthetic oneofs in raw descriptors. Those
and the syntax flag are intentional metadata changes; they do not add fields
to the binary format. In C# reflection, `IsPacked` reports the inherited
proto3 feature even on fields that cannot be packed. Read the flag only on
repeated varint, fixed32, and fixed64 fields when normalizing the wire approval.
Keep the approved file unchanged. Check presence independently: its `optional`
label describes singular cardinality and would not detect a scalar losing presence.

Proto2 enums are nominally closed, whereas proto3 enums are open. The pinned
C# generator already assigns `ReadEnum()` directly to all five proto2 enum
fields, setting presence even for an unrecognized number. The new reader
must keep that behavior. It must not move those values into unknown fields,
coerce them to zero, or change which input the existing validators reject.
Unknown tags remain in the adapter's opaque extension data where they did
before, including inside snapshots and log entries. This decision covers
the .NET adapter's binary behavior, not raw descriptor identity, generated
source or ProtoJSON compatibility in other languages, or arbitrary unknown
field order in a re-encoded protobuf stream. See protobuf's
[presence](https://protobuf.dev/programming-guides/field_presence/) and
[enum behavior](https://protobuf.dev/programming-guides/enum/) guides.

## Durable data and rollout

The same encoding carries peer messages, SQLite Raft hard state, entries and
snapshots, the KV store's applied membership, and both generations of
configuration changes embedded in replicated entries. Preserve the bytes our
writers emit for the same values. In particular, keep the entry-size budget
and the storage position reader tied to the same entry tags and sizes.
`RaftHardState` remains a value projection: a newly written value emits all
three fields, while an unchanged legacy BLOB is not normalized or rewritten.

Keep the database schema and format marker unchanged. There is no migration
to run and no format negotiation. Upgrade a node using both of its existing
databases as one recovery set; enough peers must remain up for quorum.
Rolling back to the pre-migration binary should use that same recovery set,
not an old copy of either database. This covers the migration alone, not
arbitrary versions or unrelated storage changes.

## Verification gates

- Use `56a4a4dde31f8bb95b7ce237ffd07e872d8c668e` as the pre-migration
  .NET oracle. Freeze small binary cases from its codec, rather than
  comparing only the new writer against the new reader. Check presence
  and original output for explicit defaults, empty nested messages,
  unrecognized enum numbers, unknown tags, and all four membership lists.
  Also verify that packed membership input still decodes. Existing entry
  measurement, validator, and nested extension tests must keep passing.
- Check the generated descriptor's syntax and presence separately from the
  existing wire approval. The syntax assertion must fail on proto2; a
  proto3 variant that omits an unpacked option must fail the legacy encoding
  check. Do not reapprove managed API, wire, SQLite schema, or goldens.
- Reopen SQLite with fixed legacy hard-state bytes; verify a write that does
  not update hard state leaves the old BLOB intact, and a new write includes
  zero vote and commit fields. Existing durable tests cover entry and
  snapshot reopen; the mixed-process check below uses actual old and new
  databases, not fixtures produced by the candidate parser.
- Reuse the [durable KV process smoke](../../examples/DotnetRaft.KvCluster/README.md#automated-real-process-smoke)
  with two builds. Start the cluster with one build and restart the lagging
  follower with the other, in the same data directory. Read a previously
  applied value while the other peers are stopped so they cannot repair the
  store. Then observe an incoming snapshot past the old log, verify the local
  read barrier and payload digest, and continue writes with just one node of
  each build alive. Reverse the build order to exercise the downgrade. Keep
  the same-build smoke in CI; the two-build run is the rollout check, and the
  fixed wire fixtures guard later changes in CI.
- Run Debug and Release, formatting, and the pinned Go comparison. The latter
  checks consensus semantics, not byte compatibility or a rolling restart.

## Migration evidence

With the pinned generator, only the generated C# reflection descriptor
region changed; the generated parsers, writers, and size methods are
identical. On the old schema the new syntax test failed and the compatibility
fixtures passed. A proto3 variant that let `voters` default to packed failed
both membership fixture cases and the wire approval.

The final schema passed 1,263 tests each in Debug and Release, formatting,
and all 13 pinned Go comparison checkpoints. The normal durable smoke and
both mixed-build directions passed, including offline reopen, the 31 MiB
snapshot restore, and a write with only one node of each build available.
No managed API, wire, SQLite schema, or golden approvals changed.

## Alternatives

- Keeping proto2 is wire-safe but leaves the requested source migration undone.
- Changing the syntax alone would switch membership writes to packed encoding.
  Removing scalar `optional` would also lose default-value presence and the
  generated `Has*` API. Readability of some old bytes is not sufficient.
- A second codec, versioned envelopes, or a disk rewrite would add support and
  recovery paths for binary values that do not need a format change.
