# D26 Parity and Release Hardening Design

- **Status:** Accepted
- **Date:** 2026-10-03
- **DAG node:** D26
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`
- **Related decision:** ADR 0002

## Purpose

D26 closes the declared behavioral parity matrix and establishes a
reproducible 1.0.0 release baseline. It adds the remaining pinned interaction
corpus, deterministic randomized invariants, public integration
documentation, API-surface approval, NuGet metadata/content verification, CI,
and focused performance baselines.

D26 changes no intended consensus behavior. Any parity test that exposes a
bug is fixed surgically and reviewed before release.

## Goals

- Execute every pinned `testdata/*.txt` interaction command flow.
- Document feature parity and every intentional deviation.
- Add deterministic randomized cluster invariants.
- Freeze the first shipped public API surface.
- Produce and inspect a compliant `DotnetRaft.1.0.0.nupkg` and `.snupkg`.
- Verify a clean consumer can restore and compile against the package.
- Document synchronous and asynchronous host responsibilities.
- Establish repeatable non-gating performance baselines.
- Add CI for restore, format, test, pack verification, and benchmark smoke.

## Non-goals

- Publishing the package or creating a Git tag.
- Multi-targeting frameworks older than `net10.0`.
- Transport, gRPC services, disk-backed storage, or application examples that
  conceal host responsibilities.
- Timing thresholds in unit tests.
- Post-parity `RaftCore` structural refactoring; the final separate todo owns
  that work.

## Complete interaction corpus

The pinned reference contains 28 interaction files. D25 already ports the two
asynchronous flows. D26 imports the remaining 26 files with their original
file names under:

```text
tests/DotnetRaft.Tests/Interaction/PinnedTestData/
```

The D25 async files move into that directory as:

```text
async_storage_writes.txt
async_storage_writes_append_aba_race.txt
```

The six educational C# scenarios remain under `Interaction/TestData`.

`InteractionGoldenTests` recursively executes:

```text
6 educational files
28 pinned command flows
= 34 deterministic files
```

Each pinned file preserves command/input order. Expected output is rewritten
to the accepted C# logger, enum, and description grammar, then checked in.
Every file runs twice with a fresh environment to catch hidden state and
nondeterminism.

`Interaction/PinnedTestData/manifest.json` pins the corpus itself:

```json
{
  "referenceCommit": "1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e",
  "totalCases": 558,
  "files": [
    {
      "name": "campaign.txt",
      "caseCount": 4,
      "commandInputSha256": "<lowercase hex>"
    }
  ]
}
```

For each parsed case, the digest input is UTF-8:

```text
<command line>\n
<input, LF-normalized>\n
----\n
```

Cases concatenate in source order. Comments, expected output, and blank
separators do not participate. File names sort ordinally for the manifest.

Tests require exactly 28 files, 558 cases, the pinned commit, each per-file
case count, and each SHA-256 digest. This detects truncation or changed
commands/inputs even when rewritten expected output still passes.

The rewrite path remains opt-in through
`DOTNET_RAFT_REWRITE_INTERACTION=1` for the six educational files only.
Pinned corpus files have no rewrite path and always execute read-only.
Ordinary CI never rewrites files.

## Deterministic randomized properties

`RandomizedInteractionPropertyTests` runs 32 cases:

```text
seeds 0-15  synchronous Ready/Advance
seeds 16-31 asynchronous append/apply workers
```

The PRNG is a checked-in xorshift64* implementation:

```text
x ^= x >> 12
x ^= x << 25
x ^= x >> 27
result = x * 2685821657736338717
```

Zero seed is initialized to
`0x9e3779b97f4a7c15`. Each seed performs exactly 200 actions. The low byte of
each generated value selects:

Every run starts with IDs 1, 2, and 3 as voters in a term-one snapshot at
index 10:

```text
ElectionTick=3
HeartbeatTick=1
MaxInflightMessages=4
MaxCommittedSizePerReady=256
PreVote=(seed bit 0)
CheckQuorum=(seed bit 1)
AsyncStorageWrites=(seed >= 16)
```

All nodes start with empty application data and `Applied=10`. Node selection
uses `1 + ((value >> 8) % 3)`. Peer selection uses the next different ID in
ring order plus `((value >> 16) & 1)`. When a queued message is needed, index
`(value >> 24) % queueCount` selects it; the action handles all queued messages
with that selected message's target and type, preserving harness semantics.
Bit 32 selects deliver versus drop, but local targets and self messages always
deliver. Bit 40 selects all-node versus selected-node stabilization.

Proposal data is exactly 16 bytes:

```text
little-endian seed (8 bytes)
little-endian action number (8 bytes)
```

Proposal actions use the sole current leader view, if any; otherwise they are
no-ops. Report-unreachable uses the generated node and generated distinct peer.
Tick actions call exactly one `RawNode.Tick`.

- campaign;
- role-dispatched ticks;
- leader proposal;
- Ready processing;
- message delivery or loss;
- unreachable reporting;
- append/apply worker processing; and
- selected/all-node stabilization.

Action ranges and preconditions are:

```text
0-15    campaign a generated node
16-63   tick a generated node
64-95   propose only when a leader view exists; otherwise no-op
96-143  process one Ready only when available
144-191 select one queued nonlocal message by the defined queue index, then
        handle the full queued batch with its target/type; bit 32 selects
        deliver/drop
192-207 report a generated peer unreachable from a generated node
208-223 process one append item when async and available
224-239 process one apply item when async and available
240-255 stabilize either all nodes or one generated node
```

Local storage messages are never placed in the droppable network path.
Expected proposal drops are handled explicitly.

Every step checks the common cursor chain:

```text
Applied <= Applying
Applying <= Committed <= LastIndex
Unstable.Offset <= Unstable.OffsetInProgress
Progress.Next > Progress.Match
at most one leader per term across local views
```

It also checks:

```text
synchronous: physicalApplied <= Applied
asynchronous: Applied <= physicalApplied <= Committed
Unstable.OffsetInProgress <= LastIndex + 1
```

Failures report mode, seed, action number, generated value, action, compact
node cursors/roles, queued message targets, and worker counts so the run is
exactly reproducible.

No wall clock, random system seed, sleep, or background thread participates.

## Parity matrix

`docs/parity-matrix.md` maps each declared feature to:

- pinned implementation/test sources;
- C# design node;
- focused C# tests;
- interaction files; and
- parity status.

The matrix covers protocol/storage, quorum, unstable/log, progress/flow,
configuration, election, replication, snapshots, reads, availability,
leadership transfer, RawNode, bootstrap/status/diagnostics, concurrent Node,
and async storage.

Intentional deviations are explicit:

- C# `CancellationToken`/`ValueTask` replaces Go `context` and channels;
- deterministic sorted status JSON replaces nondeterministic Go map order;
- malformed diagnostic payloads use stable tokens, not protobuf exception
  text;
- bootstrap has a stricter never-used lifecycle;
- fault/stop errors are explicit exceptions;
- deprecated `TickQuiesced` is not ported;
- transport, gRPC, disk engines, and application state remain outside scope;
  and
- test-only network helpers are not runtime API.

No known behavioral parity gap may remain undocumented.

## Public documentation

`docs/public-api.md` documents:

- architecture and host/library boundary;
- choosing `RawNode` versus `RaftNode`;
- start/restart/bootstrap;
- synchronous Ready persistence/application order;
- asynchronous append/apply worker protocol;
- configuration acceptance/rejection;
- physical versus logical applied indexes;
- snapshots, compaction, recovery, and commit repair;
- message transport and snapshot reporting;
- read-index barriers;
- tracing, status, diagnostics, cancellation, stop, and faults; and
- a minimal end-to-end host loop for both storage modes.

README becomes the NuGet readme and links to the public guide, parity matrix,
designs, license, and notices.

## Public API manifest

`tests/DotnetRaft.Tests/Release/PublicApi.approved.txt` records a deterministic
reflection view of every exported type and every declared public:

- constructor;
- method;
- property;
- event;
- enum value; and
- constant/field.

The manifest grammar includes:

- type visibility, kind, static/abstract/sealed modifiers, base type, and
  interfaces;
- generic parameter variance, attributes, and class/struct/interface/new()
  constraints;
- nullable annotations through `NullabilityInfoContext`;
- constructor and method visibility, static/abstract/virtual/final modifiers,
  generic constraints, return type, required/optional custom modifiers,
  parameter names, ref/in/out/params modifiers, nullable types, and invariant
  optional default values;
- property type, index parameters, required custom modifiers, getter/setter
  visibility, and `init` versus `set`;
- event type and accessor visibility;
- field/enum visibility, const/static/readonly modifiers, type, and invariant
  constant value; and
- compatibility-relevant attributes:
  `Obsolete`, `Flags`, `Extension`, `ParamArray`, `RequiredMember`, and
  `SetsRequiredMembers`.

Members and attributes sort ordinally. Attribute values use a fixed invariant
grammar.

`ProtocolDescriptorApprovalTests` separately records protobuf package/message/
enum names, field numbers, wire types, labels, and enum numeric values from
the generated descriptor. This is the wire-compatibility baseline from ADR
0002.

`PublicApiApprovalTests` regenerates the surface in memory and compares exact
text. Tests have no rewrite path. Any baseline update is a separately reviewed
source change generated outside the test process.

The manifest is the ADR 0002 compatibility baseline, not a substitute for
behavioral tests.

## NuGet package

`src/DotnetRaft/DotnetRaft.csproj` declares:

```text
IsPackable = true
PackageId = DotnetRaft
Version = 1.0.0
Authors = algorithm-apprentice
Description = An educational, behavior-oriented C# implementation of the etcd Raft consensus state machine for .NET.
TargetFramework = net10.0
PackageLicenseFile = LICENSE
PackageReadmeFile = README.md
RepositoryType = git
RepositoryUrl = https://github.com/algorithm-apprentice/dotnet-raft
IncludeSymbols = true
SymbolPackageFormat = snupkg
PackageOutputPath = ../../artifacts/package
```

Explicit `Pack=true`, `PackagePath=""` items add:

```text
LICENSE
README.md
THIRD-PARTY-NOTICES.md
```

The runtime dependency list contains `Google.Protobuf 3.36.2`.
`Grpc.Tools` stays private and must not appear in the nuspec dependency list.

Every hyperlink in the packaged README is an absolute HTTPS URL. Repository
documentation, license, notices, and source links use
`https://github.com/algorithm-apprentice/dotnet-raft/...`; no relative link is
permitted.

`RepositoryCommit` is passed explicitly from `git rev-parse HEAD` locally and
`${{ github.sha }}` in CI.

## Package verification

`tools/DotnetRaft.ReleaseVerifier` is a dependency-free `net10.0` console
project. `eng/verify-package.sh <nupkg> <snupkg> <repository-root> <commit>` invokes it
and then performs the isolated consumer restore.

The verifier:

1. inspects the ZIP paths;
2. verifies the required root files and `lib/net10.0/DotnetRaft.dll`;
3. reads the nuspec and checks ID, version, authors, description, repository,
   license/readme metadata, and dependencies;
4. rejects `Grpc.Tools` as a consumer dependency;
5. byte-compares packaged legal/readme files to repository files;
6. verifies the symbol nuspec has matching ID/version and
   `SymbolsPackage` type;
7. verifies `lib/net10.0/DotnetRaft.pdb` is a portable PDB (`BSJB`);
8. reads the DLL CodeView record and portable-PDB ID with
   `System.Reflection.Metadata` and verifies their debug identity; and
9. requires the exact expected commit in nupkg/snupkg repository metadata and
   portable-PDB Source Link;
10. rejects every unexpected symbol-package path, every nonportable PDB, and
    any entry outside NuGet.org's allowed `.snupkg` layout; and
11. emits one deterministic success line containing package ID/version and
    repository commit.

The shell wrapper creates temporary:

```text
NUGET_PACKAGES
NUGET_HTTP_CACHE_PATH
consumer project
NuGet.Config
```

The config starts with `<clear/>` and adds only:

- the candidate package directory for pattern `DotnetRaft`; and
- `https://api.nuget.org/v3/index.json` for `Google.*`, `System.*`, and
  `Microsoft.*`.

Restore uses `--no-cache` and the isolated directories. The wrapper verifies
the restored
`$NUGET_PACKAGES/dotnetraft/1.0.0/.nupkg.metadata` source resolves to the
candidate directory, then compiles a small public `RawNode`/`RaftNode`
program. Temporary state is removed by a trap.

The script is noninteractive, uses resolved paths, and never publishes.

## Reproducible package content

Reproducibility means identical canonical extracted path sets and bytes, not
identical ZIP container timestamps. NuGet generates a random OPC
core-properties part name and matching relationship ID on every pack. The
comparison canonicalizes only:

```text
package/services/metadata/core-properties/<random>.psmdcp
-> package/services/metadata/core-properties/core-properties.psmdcp

the matching _rels/.rels relationship Target and Id
-> fixed canonical values
```

The core-properties XML payload itself and every other package path/byte are
compared exactly.

`eng/pack-release.sh` is the only release pack recipe. It requires a clean
working tree at the supplied HEAD commit and always passes the same
`ContinuousIntegrationBuild`, `Deterministic`, `RepositoryCommit`, and
canonical `PathMap` properties.

`eng/verify-reproducible-pack.sh`:

1. requires the original repository and candidate artifacts to be clean and
   at the supplied commit;
2. creates two detached Git worktrees at that exact commit in distinct
   absolute paths;
3. restores each worktree and invokes its checked-in `pack-release.sh`;
4. compares the actual candidate `.nupkg` and `.snupkg` against both rebuilds;
   and
5. requires every canonical extracted non-signature path and byte sequence to
   match.

The shared pack recipe passes:

```text
Configuration=Release
ContinuousIntegrationBuild=true
Deterministic=true
RepositoryCommit=<same HEAD>
PathMap=<physical worktree root>=/_/
SOURCE_DATE_EPOCH=0
```

NuGet signature files are absent because D26 does not sign packages.

## CI

`.github/workflows/ci.yml` runs on pushes and pull requests:

1. checkout;
2. install the SDK declared by `global.json`;
3. `dotnet restore`;
4. `dotnet format --verify-no-changes`;
5. `dotnet test DotnetRaft.sln -c Debug --no-restore`;
6. `dotnet test DotnetRaft.sln -c Release --no-restore`;
7. `dotnet pack src/DotnetRaft/DotnetRaft.csproj -c Release --no-restore -p:RepositoryCommit=${{ github.sha }}`;
8. `./eng/verify-package.sh artifacts/package/DotnetRaft.1.0.0.nupkg artifacts/package/DotnetRaft.1.0.0.snupkg "$GITHUB_WORKSPACE"`;
9. `./eng/verify-reproducible-pack.sh "$GITHUB_WORKSPACE" "${{ github.sha }}"`;
10. `dotnet run --project benchmarks/DotnetRaft.Benchmarks/DotnetRaft.Benchmarks.csproj -c Release --no-restore -- --smoke`.

No credentials or publishing step is included.

The solution contains production, test, benchmark, and release-verifier
projects, so the initial restore/build covers every later `--no-restore`
command. Pack artifacts always use `artifacts/package`.

## Performance baselines

`benchmarks/DotnetRaft.Benchmarks` is a dependency-free `net10.0` console
project using `Stopwatch`.

It reports median nanoseconds/operation and operations/second for:

1. singleton synchronous proposal through append/commit/application Ready
   cycles;
2. leader `GetStatus` snapshot construction; and
3. deterministic message description formatting.

Each benchmark:

- builds/runs in `Release` with server GC enabled;
- performs one untimed warmup with the same operation count;
- forces `GC.Collect`, `WaitForPendingFinalizers`, and a second collection
  before each measured iteration;
- creates/elects fresh singleton state outside the timed region;
- runs five measured iterations by default; and
- consumes results into a checksum printed with the measurement.

Exact workloads are:

| Name | Default operations | Timed region |
|---|---:|---|
| `sync-proposal-cycle` | 10,000 | 32-byte `Propose`, append Ready construction, `MemoryStorage` snapshot/entry/hard-state persistence in host order, first `Advance`, committed Ready construction, normal-entry application acknowledgement, and second `Advance`; construction, term-one snapshot restore, singleton election, and initial no-op application are excluded. |
| `status-snapshot` | 100,000 | leader `GetStatus` plus checksum of term, commit, applied, and progress count. |
| `describe-message` | 100,000 | `RaftDescriptions.DescribeMessage` for the fixed message below plus checksum of output length. |

The proposal payload is bytes `0x00` through `0x1f`.

Each measured singleton starts from:

```text
node ID=1
term-one snapshot index=2
ConfState voters=[1]
empty snapshot data
RaftConfig.Applied=2
ElectionTick=10
HeartbeatTick=1
unlimited message/uncommitted limits
MaxInflightMessages=256
```

Before timing, the benchmark explicitly campaigns, persists/advances the
candidate Ready, persists/advances the leader no-op Ready, and
persists/applies/advances the committed no-op Ready. The timer starts only
after the node is idle as leader at applied index 3.

The description message is:

```text
From=1 To=2 Type=MsgApp Term=7 LogTerm=6 Index=41 Commit=40
Entry 1: Term=7 Index=42 Type=EntryNormal Data=bytes 0x00..0x1f
Entry 2: Term=7 Index=43 Type=EntryNormal Data=UTF-8 "payload-two"
```

Every benchmark checksum starts at FNV-1a offset
`14695981039346656037` and updates with:

```text
checksum = (checksum XOR value) * 1099511628211
```

using unchecked `ulong` arithmetic.

- proposal cycle feeds appended final index, committed final index, and stable
  storage last index for every operation;
- status feeds term, commit, applied, and progress count; and
- description feeds the returned string length.

Warmup initializes and computes a checksum but discards it. Every measured
iteration resets the checksum to the FNV offset and creates fresh state.
Measured iteration checksums must all be identical; otherwise the benchmark
fails. The JSON `checksum` is that common measured value, not an accumulation
across warmup or iterations.

`--smoke` uses one measured iteration and respectively 100, 1,000, and 1,000
operations.

Output is one invariant-culture JSON object per benchmark:

```json
{"name":"sync-proposal-cycle","operations":10000,"iterations":5,"medianNanosecondsPerOperation":123.4,"operationsPerSecond":8103727.7,"checksum":12345}
```

`docs/performance.md` records methodology, the smoke command, a reference
capture from the D26 environment, and the rule that results are comparative,
not CI pass/fail thresholds.

The reference capture records commit, UTC date, SDK, runtime, Release
configuration, OS, architecture, CPU model, GC mode, exact command, and all
JSON output lines.

## Release tests

New focused tests verify:

- all 28 pinned names, 558 cases, and command/input digests match the manifest;
- all 34 interaction files run twice;
- randomized seeds and invariant diagnostics;
- API and protobuf manifest exactness with no test-side rewrite path;
- package metadata values and required documentation paths;
- protobuf descriptor wire manifest exactness;
- every README hyperlink is absolute HTTPS, while relative links in
  repository-only documentation resolve; and
- release version and reference commit appear consistently.

Package ZIP and benchmark execution remain explicit validation commands rather
than nested unit-test processes.

## Acceptance criteria

D26 is complete when:

1. all 28 pinned command flows and 6 educational files pass exactly;
2. deterministic randomized properties pass for every fixed seed;
3. parity and intentional deviations are fully documented;
4. the approved public API manifest matches;
5. public host-integration documentation is complete;
6. `dotnet pack -c Release` produces valid `.nupkg` and `.snupkg`;
7. package verification and clean consumer compilation pass;
8. two-path extracted package-content reproducibility passes;
9. benchmark smoke and a recorded baseline complete;
10. full Release tests, Debug tests, and formatting pass;
11. an independent GPT-5.6 Sol review approves design and implementation; and
12. the completed node is committed and pushed before post-parity refactoring.
