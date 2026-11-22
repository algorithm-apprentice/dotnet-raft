# ADR 0004: Teach with a bounded gRPC KV host

- **Status:** Accepted; the initial volatile storage and application choices
  were superseded by [ADR 0007](0007-durable-fixed-membership-kv-host.md).
- **Date:** 2026-10-04

## Context

A real transport example should show where consensus stops, not turn the
library into a deployed database or hide application behind a client API.

## Decision

Use one `RaftNode` per process, one supervised Ready loop, and host-owned
ticks. Use fixed membership so the example can focus on persistence,
application, and reads rather than administrative reconfiguration. Put
client HTTP and peer gRPC on separate loopback ports: plaintext HTTP/1 and
HTTP/2 need not negotiate on a single listener. The gRPC envelope carries
serialized Raft messages and does not duplicate the consensus schema.

Complete a write only after the host applies it, and expose a linearizable
read separately from a local inspection endpoint. Use a fresh quorum read
barrier rather than requiring lease timing assumptions in the example.

## Consequences

The [runbook](../../examples/DotnetRaft.KvCluster/README.md) demonstrates a
real multi-process host but deliberately does not offer production identity,
TLS, or an administration service. The
[current host design](../internals/kv-host.md) explains the durable state
and request rules added after the initial example.
