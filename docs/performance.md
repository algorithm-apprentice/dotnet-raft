# Performance Baselines

Performance measurements are comparative diagnostics, not CI pass/fail
thresholds.

## Command

```bash
dotnet run \
  --project benchmarks/DotnetRaft.Benchmarks/DotnetRaft.Benchmarks.csproj \
  -c Release \
  -- --operations 10000 --iterations 5
```

CI uses:

```bash
dotnet run \
  --project benchmarks/DotnetRaft.Benchmarks/DotnetRaft.Benchmarks.csproj \
  -c Release \
  --no-restore \
  -- --smoke
```

## Methodology

The benchmark project follows the exact workloads, setup boundaries, FNV-1a
checksum recurrence, GC policy, and JSON schema in
[D26 parity/release design](design/d26-parity-release-hardening.md).

Construction, snapshot restore, and election are outside proposal/status timed
regions. Persistence and application operations inside a proposal cycle are
included.

## D26 reference capture

This section is populated by D26 validation after the benchmark project is
built and run.

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
