# ADR 0001: Separate consensus from its host

- **Status:** Accepted
- **Date:** 2026-09-30

## Context

An election, append, or read transition has external effects: send a reply,
persist a vote, apply a command. If consensus executes those effects itself,
it also has to choose the service's clocks, network, files, and database.
That would make the state machine harder to embed and failure orders harder
to reproduce.

## Decision

Drive consensus with serialized inputs and make the host execute the
resulting work. `RawNode` is the deterministic boundary; `RaftNode` only adds
serialization for concurrent callers. Keep the stable read interface
explicit, and keep transport, timer scheduling, durable publication, and
client semantics out of consensus. An optional durable backend may live in
a separate package without becoming a core dependency.

Use generated Raft messages in the core and in transport. A second DTO
model would require reproducing presence, cloning, wire fields, and encoded
size semantics and could drift from the log that is actually replicated.

## Consequences

Tests can place a crash between an announced effect and its acknowledgement
without opening sockets or relying on elapsed wall time. A real service
must implement the [host workflow](../integration/README.md); calling
`Propose` alone does not give it a durable result. The
[implementation map](../internals/README.md) shows where each effect starts
and where it finishes.
