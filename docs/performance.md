# Performance Baselines

Performance measurements are comparative diagnostics, not CI pass/fail
thresholds.

## Run the benchmarks

From the repository root, run the release workload:

```bash
dotnet run \
  --project benchmarks/DotnetRaft.Benchmarks/DotnetRaft.Benchmarks.csproj \
  -c Release \
  -- --operations 10000 --iterations 5
```

The refactor suite also reports allocations for the proposal cycle, follower
heartbeat dispatch, and safe-read completion:

```bash
dotnet run \
  --project benchmarks/DotnetRaft.RefactorBenchmarks/DotnetRaft.RefactorBenchmarks.csproj \
  -c Release
```

[CI](../.github/workflows/ci.yml) runs both projects with `--smoke`. That
checks that the workloads execute and produce valid results, not whether
timings meet a threshold. These are in-memory microbenchmarks, not measures
of SQLite, transport, or end-to-end KV throughput.

## Methodology

The [release workload source](../benchmarks/DotnetRaft.Benchmarks/Program.cs)
defines the exact setup, operations, checksum, and JSON output. It runs one
warmup, forces GC before each measured iteration, and verifies that measured
iterations produce identical checksums. Construction, snapshot restore, and
election are outside proposal and status timing. The proposal cycle includes
memory publication and application acknowledgement, not a real application
transaction. The [refactor workloads](../benchmarks/DotnetRaft.RefactorBenchmarks/Program.cs)
also report per-operation allocations.

## D26 reference capture

This is the historical D26 reference capture, not a measurement of the
current checkout. Later refactor measurements are recorded separately in the
[refactor safety baseline](design/post-parity-refactor-safety-baseline.md).

```text
commit: 2907292d80fc7401e48ea2094eeb750d473aca90
utcDate: 2026-10-03T08:36:42Z
sdk: 10.0.100
runtime: 10.0.0
configuration: Release
os: macOS 27.0 (Darwin 27.0.0)
architecture: arm64
cpu: Apple M4 Pro
gcMode: server
command: dotnet run --project benchmarks/DotnetRaft.Benchmarks/DotnetRaft.Benchmarks.csproj -c Release --no-restore -- --operations 10000 --iterations 5
```

```json
{"name":"sync-proposal-cycle","operations":10000,"iterations":5,"medianNanosecondsPerOperation":5075.3958,"operationsPerSecond":197028.9686569863,"checksum":9240325797370691861}
{"name":"status-snapshot","operations":100000,"iterations":5,"medianNanosecondsPerOperation":338.17459,"operationsPerSecond":2957052.4503334207,"checksum":16443957934187860133}
{"name":"describe-message","operations":100000,"iterations":5,"medianNanosecondsPerOperation":624.685,"operationsPerSecond":1600806.806630542,"checksum":16029445456918749989}
```

Future measurements should record the same environment fields and compare
JSON results at the same operation/iteration counts.
