# RaftNode Concurrency Hardening Design

[ADR 0005](../adr/0005-deterministic-raftnode-concurrency-verification.md)
chose deterministic boundary tests instead of redesigning the owner loop.
Use the [RaftNode contract](../public-api.md#raftnode) for public call semantics.

## Deterministic test synchronization

[`BlockingTraceSink`](../../tests/DotnetRaft.Tests/Node/RaftNodeTestSupport.cs)
holds the owner in a selected synchronous trace callback. The test waits for
entry, arranges the competing action, then releases the callback in `finally`.
The bounded wait belongs outside the callback: timing out inside it would
inject a different terminal failure into the execution being tested.

For a pre-claim race, hold the owner on unrelated work and cancel the queued
operation. For a post-claim race, hold it at a trace event from the operation
itself. `ReadyAccepted` distinguishes a waiter that already owns a batch;
the traced self-message on `AdvanceAsync` distinguishes an advancement already
in progress. The event order, not a sleep, establishes which side won.

## Evidence to inspect

| Question | Evidence |
|---|---|
| Did a canceled command mutate the node? | [Command and claim tests](../../tests/DotnetRaft.Tests/Node/RaftNodeCommandTests.cs) observe dispatch and continuation behavior. |
| Was Ready consumed, advanced, or abandoned exactly once? | [Ready tests](../../tests/DotnetRaft.Tests/Node/RaftNodeReadyTests.cs) distinguish pending, claimed, and outstanding work; [async tests](../../tests/DotnetRaft.Tests/Node/RaftNodeAsyncStorageTests.cs) check pipelining without Advance. |
| Did failure reach all lanes without masking its cause? | [Lifecycle tests](../../tests/DotnetRaft.Tests/Node/RaftNodeLifecycleTests.cs) arrange stop and an already-claimed fault, then compare the triggering call with the queued calls and `Completion`. |
| Can eligible work make progress while another lane is busy? | [Scheduling tests](../../tests/DotnetRaft.Tests/Node/RaftNodeSchedulingTests.cs) and [tick tests](../../tests/DotnetRaft.Tests/Node/RaftNodeTickTests.cs) exercise observable fairness and overflow. |

These tests cover local caller races, not distributed interleavings. The
[testing lab](../learning/05-concurrency-and-testing.md) explains why both
layers are needed. Historical mutation and coverage results belong to the
[verification decision's outcome](../adr/0005-deterministic-raftnode-concurrency-verification.md#outcome),
not to the current test contract.
