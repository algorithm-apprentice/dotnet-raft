# Post-Parity Refactor Safety Baseline

- **Status:** Recorded
- **Date:** 2026-10-03
- **Pre-refactor base:** `cb12e5140643ca0c364c57c4531ed3af1d8fdb61`
- **Related plan:** `post-parity-raftcore-refactor.md`
- **Related decision:** ADR 0003

## Scope

This baseline was captured before the first architectural extraction. The only
production change is an internal pending-read count used by characterization
tests. The release benchmark remains byte-identical to its D26 provenance
commit; refactor measurements use the separate
`DotnetRaft.RefactorBenchmarks` project.

## Characterization

The Stage 0 suite contains 900 tests.

`raftcore-dispatch.approved.txt` freezes 232 role/message/term transcripts
covering:

- follower, pre-candidate, candidate, and leader roles;
- every protobuf `MessageType`;
- zero, lower, equal, and higher term variants where valid;
- snapshot and non-snapshot storage append responses;
- complete core, log, configuration, progress, queue, read-state, trace, logger,
  and exception output.

The approval test has no rewrite path. Focused tests additionally cover
transition overflow, proposal and read admission, replication rejection
boundaries, snapshot retry termination, snapshot configuration reassertion,
application accounting, exact configuration commit broadcasts, and trace
emission at commit and configuration boundaries.

## Coverage

Release coverage was collected with:

```bash
dotnet test tests/DotnetRaft.Tests/DotnetRaft.Tests.csproj \
  -c Release \
  --collect:"XPlat Code Coverage" \
  -- DataCollectionRunSettings.DataCollectors.DataCollector.Configuration.Format=cobertura
```

| Scope | Line | Branch |
|---|---:|---:|
| All instrumented code, including generated protobuf | 86.97% | 77.43% |
| `RaftCore` | 97.02% | 96.84% |
| `RawNode` | 98.20% | 93.49% |
| `RaftLog` | 89.91% | 91.54% |
| `UnstableLog` | 100.00% | 99.07% |
| `RaftNode` top-level class | 75.20% | 75.82% |

The `RaftCore` Stage 0 gate is satisfied: line coverage is at least 97% and
branch coverage is at least 96%.

## Mutation testing

The repository pins `dotnet-stryker 5.0.0` and checks in
`stryker-config.json`. The authoritative run uses that complete configuration
rather than combining partial-span scores.

| Target | Score | Killed | Timeout | Survived | No coverage | Compile error | Ignored |
|---|---:|---:|---:|---:|---:|---:|---:|
| Complete configured target | 85.56% | 1145 | 10 | 129 | 66 | 119 | 375 |
| `RaftCore` | 89.44% | 746 | 8 | 57 | 32 | 61 | 246 |
| `RaftLog` | 75.79% | 239 | 2 | 44 | 33 | 30 | 79 |
| `UnstableLog` | 84.66% | 160 | 0 | 28 | 1 | 28 | 50 |

The Safe Mode warnings are confined to `RaftNode.TryPublishReady` and
`ProgressTracker.TallyVotes`, which are outside the configured Stage 0 target.
`AppendLeaderEntries`, `MaybeSendAppend`, and `RaftLog.Slice` are fully
instrumented; their executable mutants are included above. Remaining target
compile errors are individual syntactically invalid mutations rather than
method-wide exclusions. Timeouts are detected infinite-loop mutations and
therefore count as killed behavior.

Every surviving or uncovered mutant was reviewed. The remaining cases fall
into these documented categories:

- defensive enum/default arms that cannot be produced by validated protocol
  values;
- leader no-op quota rejection and non-follower snapshot restoration branches
  that are unreachable through the validated call graph;
- duplicated validation where `RaftLog` and `UnstableLog` independently reject
  the same invalid entry sequence or committed overwrite;
- checked integer conversions already bounded by validated list counts and
  index ranges;
- mathematically equivalent boundaries, such as subtracting an equal
  uncommitted size, empty half-open slices, and equality cases that produce the
  same zero result;
- private boolean return mutations whose callers observe the state/message
  side effect rather than the return value;
- trace helpers with a second zero-count/null-sink guard; and
- defensive exception or log strings that do not change consensus state,
  output queues, persistence, or the approved normal-path diagnostic
  transcript.

No reachable surviving mutant changes election, replication, commit, read,
membership, snapshot, storage-response, or proposal-admission behavior.

## Allocation and timing

Environment:

```text
.NET SDK: 10.0.100
Architecture: arm64
macOS: 27.0.1
Configuration: Release
```

Command:

```bash
dotnet run \
  --project benchmarks/DotnetRaft.RefactorBenchmarks/DotnetRaft.RefactorBenchmarks.csproj \
  -c Release \
  --no-restore
```

Three complete process runs produced:

| Benchmark | Run 1 ns/op | Run 2 ns/op | Run 3 ns/op | Median ns/op | Bytes/op | Checksum |
|---|---:|---:|---:|---:|---:|---:|
| Sync proposal cycle | 4997.9083 | 4918.1708 | 5057.5125 | 4997.9083 | 6050.2424 | 9240325797370691861 |
| Follower heartbeat dispatch | 297.4 | 308.52708 | 299.71542 | 299.71542 | 2464.00088 | 10936930208570869669 |

For every later stage:

- both checksums must remain identical;
- sync proposal allocation must not exceed 6110.7448 bytes/op;
- follower heartbeat allocation must not exceed 2488.6409 bytes/op;
- sync proposal median must not exceed 5497.69913 ns/op; and
- follower heartbeat median must not exceed 329.686962 ns/op.

The byte limits are the baseline plus the greater of 1% or 16 bytes. The time
limits are the baseline plus 10%. A stage that exceeds either limit fails.

## Stage comparisons

### Stage 1: tracker installation ownership

`ProgressTracker` mutation testing scored 97.14%: 34 killed, one survived,
four compile errors, and 17 ignored. The sole survivor changes the initial
value of a local that is always overwritten by `TryGetValue` before use and is
equivalent. The compile errors are invalid mutations of collection `Count`
expressions in `IsSingleton`.

| Benchmark | Median ns/op | Change | Bytes/op | Checksum | Result |
|---|---:|---:|---:|---:|---|
| Sync proposal cycle | 5072.6125 | +1.49% | 6050.2424 | 9240325797370691861 | Pass |
| Follower heartbeat dispatch | 293.29583 | -2.14% | 2464.00088 | 10936930208570869669 | Pass |

Stage 1 preserves both checksums, adds no measured allocation, and remains
within the 10% timing ceiling.

### Stage 2: Raft clock extraction

`RaftClock` mutation testing scored 100%: all 42 executable mutants were
killed and 12 redundant block mutants were ignored.

| Benchmark | Median ns/op | Change | Bytes/op | Checksum | Result |
|---|---:|---:|---:|---:|---|
| Sync proposal cycle | 5038.2875 | +0.81% | 6050.2424 | 9240325797370691861 | Pass |
| Follower heartbeat dispatch | 301.41959 | +0.57% | 2464.00088 | 10936930208570869669 | Pass |

Stage 2 preserves both checksums, adds no measured allocation, and remains
within the 10% timing ceiling.

### Stage 3: Raft output extraction

`RaftOutput` mutation testing scored 100%: 31 mutants were killed, one
infinite-loop mutant timed out, ten redundant block mutants were ignored, and
six syntactically invalid mutants were excluded.

| Benchmark | Median ns/op | Change | Bytes/op | Checksum | Result |
|---|---:|---:|---:|---:|---|
| Sync proposal cycle | 4883.7292 | -2.28% | 6050.2424 | 9240325797370691861 | Pass |
| Follower heartbeat dispatch | 287.25917 | -4.16% | 2464.00088 | 10936930208570869669 | Pass |

Stage 3 preserves both checksums, adds no measured allocation, and remains
within the 10% timing ceiling.

### Stage 4: Raft role state extraction

`RaftRoleState` mutation testing scored 100%: all 14 executable mutants were
killed and two redundant block mutants were ignored.

| Benchmark | Median ns/op | Change | Bytes/op | Checksum | Result |
|---|---:|---:|---:|---:|---|
| Sync proposal cycle | 4771.3666 | -4.53% | 6050.2424 | 9240325797370691861 | Pass |
| Follower heartbeat dispatch | 282.97417 | -5.59% | 2464.00088 | 10936930208570869669 | Pass |

Stage 4 preserves both checksums, adds no measured allocation, and remains
within the 10% timing ceiling.

Before Stage 5, the same Stage 4 commit was measured with the new safe-read
completion benchmark in a detached worktree. Three complete runs reported
1695.6625, 1671.5333, and 1661.7292 ns/op. The Stage 4 read baseline is
1671.5333 ns/op, 5432.0248 bytes/op, and checksum 3884428198604542453.
Its limits are 1838.68663 ns/op and 5486.345048 bytes/op.

### Stage 5: read-index coordinator extraction

`ReadIndexCoordinator` mutation testing scored 100%: all 31 executable mutants
were killed, 11 redundant block mutants were ignored, and seven syntactically
invalid count mutants were excluded.

| Benchmark | Median ns/op | Change | Bytes/op | Checksum | Result |
|---|---:|---:|---:|---:|---|
| Sync proposal cycle | 4661.2041 | -6.74% | 6050.2424 | 9240325797370691861 | Pass |
| Follower heartbeat dispatch | 296.77875 | -0.98% | 2464.00088 | 10936930208570869669 | Pass |
| Safe read completion | 1784.4625 | +6.76% | 5432.0248 | 3884428198604542453 | Pass |

Stage 5 preserves all three checksums, adds no measured allocation, and
remains within every timing ceiling.
