# Protocol Input Hardening

[ADR 0008](../adr/0008-protocol-input-hardening.md) separates malformed peer
input from a corrupted or partially mutated node. For caller requirements,
see the [network transport contract](../public-api.md#network-transport).

## Validation boundaries

1. **Host transport** validates peer identity and destination. The loopback
   example's allowlist and routing checks are not authentication.
2. **Public message admission** distinguishes local storage responses from
   peer traffic. `RawNode.Step` validates an owned message before dispatching it
   into the core. `RaftNode` serializes that path instead of running consensus
   validation again. An ordinary structural `ArgumentException` fails the
   submitted operation without faulting the node.
3. **Core and application invariants** are not peer-input errors. A storage
   fault or a committed configuration application that fails after dispatch
   is terminal; retry on the same instance could repeat a partial change.

## Why validation depends on dispatch

Shape checks belong in
[`NetworkMessageValidation`](../../src/DotnetRaft/Core/NetworkMessageValidation.cs).
State-dependent checks follow the term and role path: a message the core
would ignore should not be rejected for an unused commit, snapshot, or read
context. A matching snapshot tuple can advance commit without installing its
configuration, so there is no reason to validate that unused configuration
as a restore. See the
[network validation regressions](../../tests/DotnetRaft.Tests/RawNode/RawNodeNetworkValidationTests.cs).

Configuration proposals use shared V1/V2 structural validation at both
public proposal and network admission paths. Structural validation is not
a dry run of future membership. The configuration changer still protects
application, bootstrap, and restore against invalid voter or learner sets,
including reserved worker IDs. It is the final safeguard for those paths,
not a second network validator.
