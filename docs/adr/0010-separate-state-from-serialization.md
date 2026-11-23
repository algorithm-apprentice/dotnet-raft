# ADR 0010: Separate domain state from its encoding

- **Status:** Accepted; the remainder is implemented by [ADR 0011](0011-complete-domain-codec-separation.md)
- **Date:** 2026-11-23

## Context

The representation decision in [ADR 0001](0001-porting-strategy.md) reused the
generated Raft types to preserve presence, cloning, and wire sizes. That also
made local state and business operations depend on a serialization library.
`HardState`, in particular, is three local values, not a peer RPC. Preparing a
Ready should not require constructing and cloning a protobuf to compare them.

## Decision

Use domain values in consensus and in application logic. Keep generated types
and serialization at the transport, persistence and host protocol boundaries.
Do not introduce a second consensus path or replace the existing codecs with
hand-written encoding. This supersedes the representation decision in ADR 0001,
not the separation between consensus and its host.

The first migration uses `RaftHardState` for `Term`, `Vote`, and `Commit` in the
core, Ready, stable storage APIs, and recovery. `null` on Ready means no update;
a present state sets the whole tuple, including `Vote = 0`. Keep the old
`raftpb.HardState` encoding in the SQLite codec. Do not rewrite an unchanged
hard-state blob on entry or snapshot work. The asynchronous append adapter
continues to require all three fields or none and provides one mapper for
storage implementations and hosts. No wire descriptor or database schema changes.

Decode ordinary committed KV commands in the Ready consumer before handing a
batch to the application. The named command, cursor, and SHA-256 identity are
C# values. Keep the existing canonical JSON and raw digest bytes. Application
effects, request results, and the cursor still commit in one transaction.

Return ReadIndex completions as `ReadState` with owned, read-only .NET bytes.
The output context compares by byte content; the peer message still uses the
original encoding.

## Compatibility and evidence

The changed `Ready`, `StorageState`, storage, diagnostics, and `ReadState`
signatures are a managed API break and are versioned as 2.0.0 under
[ADR 0002](0002-release-and-compatibility-contract.md).
We rejected a legacy protobuf overload on every state operation: it would make
both models public during the migration. Existing databases do not need a format
migration. Reopen tests read the old protobuf representation, verify that an
unchanged state retains its presence bits, and use the generated parser to
read a state written through the new API. The asynchronous Ready tests check
presence and acknowledgement ordering separately.

Further migration and its evidence gates are tracked in the
[serialization-boundary audit](../maintaining/serialization-boundary-audit.md).
