# Post-Parity Refactor Safety Baseline

This is historical evidence for the [completed core refactor](post-parity-raftcore-refactor.md),
not current coverage numbers or performance thresholds. The
[full recorded baseline](https://github.com/algorithm-apprentice/dotnet-raft/blob/45160468cb7a0a3465c5fbebf088b5998188890b/docs/design/post-parity-refactor-safety-baseline.md)
has the stage-by-stage measurements and mutant analysis.

The refactor combined three checks:

- **Behavior:** [dispatch approvals](../../tests/DotnetRaft.Tests/Core/RaftCoreDispatchCharacterizationTests.cs)
  compare log, role, queue, and trace effects, not only return values.
- **Sensitivity:** focused mutation runs checked that assertions catch
  real changes. Equivalent or unreachable survivors were analyzed separately
  from missing tests.
- **Cost:** allocation and throughput measurements used repeated workloads
  with identical checksums. When short-loop timings were bimodal, the
  relevant commits were measured in interleaved runs on the same machine.

Use the [current benchmark guide](../performance.md) for new measurements.
