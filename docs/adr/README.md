# Architecture decisions

These records explain *why* the project chose a boundary and which
alternatives it rejected. Find the current mechanism in the
[implementation map](../internals/README.md); use the
[host contract](../integration/README.md) for operating requirements.

| Decision | Reason to read it |
|---|---|
| [0001: Separate consensus from its host](0001-porting-strategy.md) | Why the library produces effects but does not run the service. |
| [0003: Give mutable consensus state one owner](0003-raftcore-structural-boundaries.md) | Why cross-component transitions stay in `RaftCore`. |
| [0005: Verify concurrency at the claim boundary](0005-deterministic-raftnode-concurrency-verification.md) | Why a paused owner loop proves more than a timed stress test for a particular race. |
| [0008: Reject malformed input before mutation](0008-protocol-input-hardening.md) | Why a bad peer request and a partially mutated node need different treatment. |
| [0009: Retry joint exit when transfer unblocks it](0009-auto-leave-retry-after-transfer-timeout.md) | Why another application acknowledgement is not a sufficient liveness trigger. |
| [0006: Ship SQLite separately](0006-sqlite-durable-raft-storage.md) | Why durable atomic batches use an optional package with one durability policy. |
| [0004: Teach with a bounded gRPC example](0004-educational-grpc-kv-cluster.md) | Why the envelope carries Raft bytes, and why transport and application endpoints are separate. Its original volatile storage choice was superseded by 0007. |
| [0007: Recover the KV host across two stores](0007-durable-fixed-membership-kv-host.md) | Why application effects, progress, and retry identity survive together. |
| [0002: Define the compatibility surface](0002-release-and-compatibility-contract.md) | Why binary, wire, host behavior, and persisted data must be checked together. |
| [0010: Separate domain state from its encoding](0010-separate-state-from-serialization.md) | Why business and consensus values should not be generated messages, and what must remain compatible. |

An ADR is not a backlog. When changing a decision, explain what problem
changed and keep the earlier context traceable in Git. See
[maintaining the docs](../maintaining/README.md#maintaining-the-docs).
