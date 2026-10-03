# Behavioral Parity Matrix

Reference: `etcd-io/raft` commit
`1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`.

The matrix tracks behavior and invariants rather than Go syntax or goroutine
structure.

| Area | Reference sources | C# design/tests | Status |
|---|---|---|---|
| Protocol model and cloning | `raftpb/raft.proto`, protocol tests | D01-D02, `Protocol/*Tests` | Complete |
| Quorum math | `quorum/*` | D03, `QuorumTests` | Complete |
| Stable storage | `storage.go` | D04, `MemoryStorageTests` | Complete |
| Unstable and unified log | `log_unstable.go`, `log.go` | D05-D06, `UnstableLogTests`, `RaftLogTests` | Complete |
| Inflight and progress | `tracker/inflights.go`, `tracker/progress.go` | D07-D09, tracker tests | Complete |
| Membership/joint consensus | `confchange/*`, configuration interaction files | D10, D17, data-driven/property tests, pinned corpus | Complete |
| Read-only tracker | `read_only.go` | D11, `ReadOnlyTrackerTests` | Complete |
| Core initialization and clocks | `raft.go` | D12, core state/config tests | Complete |
| Election and pre-vote | paper/election tests, `prevote*.txt` | D13, D19, election tests, pinned corpus | Complete |
| Replication and commit | paper/raft tests, `campaign.txt`, `lagging_commit.txt` | D14, replication tests, pinned corpus | Complete |
| Flow control and rejection hints | `raft_flow_control_test.go`, `probe_and_replicate.txt`, `replicate_pause.txt` | D15, flow/rejection tests, pinned corpus | Complete |
| Snapshots and compaction recovery | `raft_snap_test.go`, snapshot/compaction interaction files | D16, snapshot tests, pinned corpus | Complete |
| Safe and lease reads | read-index raft tests, lease interaction files | D18-D19, read-index tests, pinned corpus | Complete |
| Check quorum / forget leader | availability interaction files | D19, availability tests, pinned corpus | Complete |
| Leadership transfer | leadership-transfer raft tests | D20, transfer tests, pinned corpus | Complete |
| RawNode / Ready / durability | `rawnode.go`, `rawnode_test.go` | D21, RawNode test suite | Complete |
| Bootstrap / restart / status / descriptions / trace | `bootstrap.go`, `status.go`, `util.go`, `state_trace*.go` | D22, bootstrap/status/diagnostic tests | Complete |
| Deterministic interaction harness | `rafttest/interaction_env*`, all 28 `testdata/*.txt` files | D23-D26, 28-file/558-case manifest and exact C# outputs | Complete |
| Concurrent Node | `node.go`, `node_test.go` | D24, Node concurrency/lifecycle tests | Complete |
| Async storage and ABA | async `rawnode.go`/`raft.go`, two async interaction files | D25, async tests and pinned corpus | Complete |
| Randomized invariant coverage | reference property/fuzz intent | D26 fixed xorshift seeds | Complete |
| Package/release contract | N/A | ADR 0002, package/API/wire verification | Complete |

## Intentional deviations

These are documented design choices, not unknown parity gaps:

1. .NET uses `CancellationToken`, `Task`, `ValueTask`, and
   `System.Threading.Channels` instead of Go contexts/channels.
2. `Status.ToString()` sorts progress IDs for deterministic JSON; the pinned Go
   map order is nondeterministic.
3. Malformed diagnostic configuration payloads use stable tokens rather than
   version-dependent protobuf exception text.
4. Bootstrap is restricted to a never-used facade and validates empty
   hard-state/membership in addition to the pinned last-index check.
5. Stop and terminal faults are explicit typed exceptions. Status after a
   `RawNode` fault remains diagnostic.
6. Deprecated `TickQuiesced` is not ported.
7. Internal Go testing adapters, stochastic network helpers, and panic-style
   logger methods are test/runtime-idiom details rather than package API.
8. Transport, gRPC, durable database engines, application state, retries, and
   timers remain outside the consensus library.
9. Package 1.0.0 targets `net10.0` only; no older-framework compatibility
   requirement exists.

## Corpus integrity

`tests/DotnetRaft.Tests/Interaction/PinnedTestData/manifest.json` records:

- the reference commit;
- all 28 upstream file names;
- all 558 command/input cases; and
- a per-file SHA-256 digest that excludes comments and expected output.

The checked-in expected output uses the accepted C# descriptions and logger
wording while preserving every upstream command and input.

