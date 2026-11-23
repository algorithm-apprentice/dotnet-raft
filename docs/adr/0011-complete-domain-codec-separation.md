# ADR 0011: Keep consensus and worker work independent of protobuf

- **Status:** Accepted
- **Date:** 2026-11-23

## Decision

Complete the migration from [ADR 0010](0010-separate-state-from-serialization.md).
The core assembly defines owned log records, snapshots, configuration changes,
peer messages, and opaque read contexts. It has no reference to a serialization
assembly. `IStorage.LogFormat` supplies the encoded-entry budget and the codec
for configuration payloads already stored in the replicated log. This avoids
implementing a second protobuf size calculator in consensus.

`DotnetRaft.Protobuf` depends on the core, not the reverse. It uses the generated
codec for the existing `raftpb` schema. The generated C# namespace and visibility
changed to keep raw types inside the adapter. Field numbers, enum numbers, field
presence, unknown fields, and SQLite table and BLOB formats remain compatible.
Only the adapter interprets extension data; a forwarded record keeps it intact.

`Ready.Messages` contains peer messages. Local storage consumes typed append
and apply work. An append exposes its post-persistence responses separately.
Return its optional receipt after publishing and releasing the ordered responses;
return the apply receipt after the effects are visible. A receipt belongs to a
node instance and is single-use. The previous synthetic message shapes remain
available for historical trace rendering and deterministic fault fixtures, not
as a peer API. The SQLite adapter also reads legacy append envelopes, through the
same atomic write path.

## Compatibility and evidence

This changes the host and managed APIs, so it is a 3.0 change under
[ADR 0002](0002-release-and-compatibility-contract.md). Older databases remain
readable. The codec tests assert exact entry measurements and presence, nested
snapshot extensions, and the unchanged wire descriptor. The local worker tests
verify the publication order, detached work, and that replayed or foreign
receipts are rejected. The build, assembly reference test, and package verifier
enforce the dependency direction.
