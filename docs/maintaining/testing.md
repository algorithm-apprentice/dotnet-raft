# Verify behavior at the right boundary

Part of the [maintainer guide](README.md). The goal of a regression is to
observe the safety property that would be wrong. Agreeing final indexes
cannot by itself prove that the same commands ran, and a delivered message
does not prove that an application applied it.

## Exercise the boundary that changed

| Boundary | Representative evidence | What to observe |
|---|---|---|
| Election, log, and membership | [Core](../../tests/DotnetRaft.Tests/Core/), [quorum](../../tests/DotnetRaft.Tests/Quorum/), [tracker](../../tests/DotnetRaft.Tests/Tracker/), [configuration](../../tests/DotnetRaft.Tests/ConfChange/) | Which transition was permitted, which quorum counted, and whether committed entries or membership were preserved. |
| Safe and lease reads | [Read quorum tests](../../tests/DotnetRaft.Tests/Core/RaftCoreReadIndexQuorumTests.cs), [lease tests](../../tests/DotnetRaft.Tests/Core/RaftCoreForgetLeaderAndLeaseReadTests.cs), [single-voter commit barrier](../../tests/DotnetRaft.Tests/RawNode/RawNodeReadIndexTests.cs) | Which commit the index includes, whose acknowledgements count, and whether a nonvoter or a new term can answer too early. |
| Ready identity and persistence | [RawNode Ready](../../tests/DotnetRaft.Tests/RawNode/RawNodeReadyTests.cs), [replay](../../tests/DotnetRaft.Tests/RawNode/RawNodePersistenceTests.cs), [async responses](../../tests/DotnetRaft.Tests/AsyncStorage/AsyncStorageResponseTests.cs) | Which work was emitted and acknowledged, what is stable, and whether an old response overwrote newer state. |
| Local caller races | [Node tests](../../tests/DotnetRaft.Tests/Node/) | Claim vs. cancel, one Ready consumer, proposals blocked on eligibility, stop vs. fault, and eventual completion of every request. |
| Cluster schedules | [Deterministic failure labs](../../tests/DotnetRaft.Tests/Interaction/RaftLabTests.cs), [seeded schedules](../../tests/DotnetRaft.Tests/Release/RandomizedInteractionPropertyTests.cs) | Minority cannot apply a conflicting command; surviving nodes converge after healing; crash replay starts from an actual physical prefix. |
| Storage and client effects | [SQLite durability](../../tests/DotnetRaft.Sqlite.Tests/SqliteStorageDurabilityTests.cs), [KV recovery](../../tests/DotnetRaft.Tests/Examples/DurableKvComponentTests.cs), [KV integration](../../tests/DotnetRaft.Tests/Examples/KvClusterIntegrationTests.cs) | Atomic publication across process stop, physical recovery and read barriers, request identity, and legal histories of completed operations. |
| Observable formats and APIs | [Protocol model](../../tests/DotnetRaft.Tests/Protocol/), [diagnostics](../../tests/DotnetRaft.Tests/Diagnostics/), [API and wire approvals](../../tests/DotnetRaft.Tests/Release/PublicApiApprovalTests.cs) | Owned values, consistent wire shape, detached output and stable approved contracts. |

Owner-loop race tests use
[`BlockingTraceSink`](../../tests/DotnetRaft.Tests/Node/RaftNodeTestSupport.cs)
to pause a known transition. Pause unrelated work to prove pre-claim
cancellation; pause `ReadyAccepted` or a traced self-response to prove the
operation has already claimed work. Release the gate in `finally`. An outer
deadlock guard is enough; an exception inside a trace callback would inject
a different failure into the node. See [ADR 0005](../adr/0005-deterministic-raftnode-concurrency-verification.md).

## Run the suite

From the repository root with the .NET 10 SDK:

```bash
dotnet restore DotnetRaft.sln
dotnet format DotnetRaft.sln --no-restore --verify-no-changes
dotnet test DotnetRaft.sln -c Debug --no-restore
dotnet test DotnetRaft.sln -c Release --no-restore
```

Use `--filter 'FullyQualifiedName~RawNodeReadyTests|FullyQualifiedName~AsyncStorageResponseTests'`
on `tests/DotnetRaft.Tests` for a focused Ready change. The
[testing chapter](../learning/05-concurrency-and-testing.md#run-and-read-the-results)
has commands for individual labs and shows how to reduce a failing seed.

Run the [KV process smoke](../../examples/DotnetRaft.KvCluster/README.md#automated-real-process-smoke)
when a change crosses the application/storage or real transport boundary.
Abrupt process termination verifies publication, reopening, and replay. It
does not emulate power loss or prove that independent backups of the two
stores are compatible.

## Script fixtures

[`InteractionGoldenTests`](../../tests/DotnetRaft.Tests/Interaction/InteractionGoldenTests.cs)
runs scripts through our own node and host model. The imported fixtures
provide fixed command inputs alongside approved C# output, not a second
specification. Their [manifest](../../tests/DotnetRaft.Tests/Interaction/PinnedTestData/manifest.json)
and [integrity test](../../tests/DotnetRaft.Tests/Release/PinnedInteractionCorpusTests.cs)
keep input changes distinguishable from output changes. The
[third-party notices](../../THIRD-PARTY-NOTICES.md) identify their source.
