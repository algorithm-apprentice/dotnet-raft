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
commit: b3fb99a23cc3b27e8220253f8316d67bcb348058
utcDate: 2026-10-03T07:55:07Z
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
{"name":"sync-proposal-cycle","operations":10000,"iterations":5,"medianNanosecondsPerOperation":4410.0792,"operationsPerSecond":226753.2973103975,"checksum":9240325797370691861}
{"name":"status-snapshot","operations":100000,"iterations":5,"medianNanosecondsPerOperation":203.285,"operationsPerSecond":4919202.105418501,"checksum":16443957934187860133}
{"name":"describe-message","operations":100000,"iterations":5,"medianNanosecondsPerOperation":589.56166,"operationsPerSecond":1696175.4263328454,"checksum":16029445456918749989}
```

Future measurements should record the same environment fields and compare
JSON results at the same operation/iteration counts.
