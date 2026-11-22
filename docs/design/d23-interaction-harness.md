# D23 Deterministic Interaction Harness Design

Historical [port milestone](README.md#completed-reference-port). Use the
[test laboratory](../learning/05-concurrency-and-testing.md)
for current test commands and examples.

The [interaction environment](../../tests/DotnetRaft.Tests/Interaction/InteractionEnvironment.cs)
keeps messages, persistence, and application under explicit test control.
A test can pause at a durability boundary or drop a message without racing
wall-clock timers. The first harness used synchronous Ready; [D25](d25-asynchronous-storage-writes.md#interaction-harness)
made local append and apply scheduling independently observable.

Pinned scripts check reference behavior and stable output. Local scenarios
supplement them with partitions, physical application cursors, and recovery.
Neither simulates the operating system's durability guarantees; use the
[test map](../reference-architecture.md#test-architecture) to choose evidence.
