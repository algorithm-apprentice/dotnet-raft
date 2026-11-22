# ADR 0008: Reject malformed input before mutation

- **Status:** Accepted
- **Date:** 2026-10-04

## Context

A malformed message can be rejected while a node is still healthy. An
invariant failure after mutation is different: continuing the node may
expose inconsistent log, membership, or output state. Peer authentication
is a third concern and is not inferred from a well-shaped message.

## Decision

Validate owned message structure at `RawNode.Step` before core mutation.
Check state-dependent content only along a term/role path that will use it.
When such a check decodes a value, pass the parsed result into the
transition instead of discarding it. For a leader's heartbeat acknowledgement
that is a read position; for an Append it is a contiguous log slice. See the
[input boundary design](../maintaining/input-boundary-design.md) for the
preparation and dispatch lifetime.
Use the same structural configuration checks for local proposals and peer
input. Node ID zero represents an explicit no-op in a configuration change;
reserved local worker IDs are not member IDs. Disabling proposal admission
policy does not disable structural validation.

A structural `ArgumentException` rejects one operation. Do not turn an
internal invariant, committed configuration, or storage failure into a
recoverable peer error on the same node. The host still authenticates peers
and authorizes routing. See [facade validation](../internals/node.md#validation-and-failure).

## Trade-offs

Prevalidation can parse configuration entries before proposal dispatch.
That cost keeps malformed input outside the mutation boundary. Validating
only the example leaves other hosts unprotected; catching all invariant
exceptions would hide failures after a partial transition.
