# Close the 3.0 serialization boundary review findings

Status: reviewed and implemented. This follows the
[serialization-boundary audit](serialization-boundary-audit.md) and
[ADR 0011](../adr/0011-complete-domain-codec-separation.md). It does not change
the consensus algorithm, durability ordering, public API, protobuf schema,
or either SQLite format.

## Confirmed evidence and origin

| Finding | Evidence and introduction |
|---|---|
| Rejected storage input terminates `RaftNode`. | On `f818865`, returning a foreign receipt, returning a consumed receipt, and passing a legacy storage response to `StepAsync` each reject the request and then leave `RaftNode` faulted. The equivalent foreign receipt does not fault `RawNode`. On `76fd690`, the two public `StepAsync` tests for invalid storage response type/sender still pass and the node remains usable. `f818865` introduced the typed receipts and changed the rejection path for legacy inputs. |
| Async regression tests can bypass receipt execution. | On `f818865`, the simulator and stale-append cases translate issued work into diagnostic messages and feed them into the fixture ingress. A temporary mutation admitting old terms through only the typed append path survived 146 focused tests. The equivalent scenario using typed receipts fails that mutation and passes the unmodified implementation. This is lost coverage, not evidence of a stale-append failure in the released code. |
| The KV transport copies the encoded snapshot twice into output buffers. | On the same macOS arm64/.NET 10 setup, the `76fd690` path allocated about 31 MiB to encode the 31 MiB snapshot into a `ByteString`. On `f818865`, `EncodeMessage` followed by `ByteString.CopyFrom` allocated about 62 MiB. That extra full-size allocation belongs to the new gRPC adapter path, not the Raft log or SQLite. |
| The storage implementer guide describes obsolete interfaces. | `186bd62` replaced `HardState`; `f818865` replaced generated entries and introduced `IStorage.LogFormat`. The guide was not migrated with either change. |

## Design and acceptance gates

### Reject before claim without faulting the node

`RawNode` owns receipt validation. Reuse the internal expected validation
exception (an `InvalidOperationException`) when a receipt is foreign or
already consumed, and when a legacy storage response or a reserved worker
sender is passed to peer ingress. Route those inputs from `RaftNode.StepAsync`
to `RawNode` on the ordinary command lane, before the existing ignore and
proposal routing rules. Do not special-case all `InvalidOperationException`s:
after a valid claim, failure during tracing or execution still faults the
node. Leave cancellation/stop precedence and unsupported-storage-mode behavior
unchanged. A wrong owner must not consume the receipt for its real owner.

Use the public facades to verify no state or Ready work is lost on rejection.
For the concurrent facade, show that subsequent commands and a valid
persist/apply cycle succeed, not just that the rejecting call throws. Exercise
legacy ingress with a worker sender and with an ordinary claimed peer ID,
and both storage modes for peer rejection. Also verify that throwing during
a valid receipt's trace remains terminal.

### Drive the typed work in deterministic tests

Use actual `StorageAppendWork` and `StorageApplyWork` in the existing cluster
simulator and send the original receipt back through `CompleteStorageWork`.
Preserve FIFO per worker and the common delivery queue's selectable order:
append responses are queued before the receipt, and a crash removes queued
work/deliveries for that instance. Apply still reports delivery before any
snapshot-covered entries are skipped. Never let packet loss or duplication
apply to a local receipt. Use diagnostic messages for labels and rendering
only; historical script commands and approved transcripts must remain exact.
Keep the synthetic ingress for explicit malformed-frame tests, not for work
emitted by a real node. Use the typed path in the focused ABA, snapshot
supersession and owner-loop tests as well.

The term-ABA negative control must fail when a stale receipt moves the
unstable offset on a matching entry ID. A separate control that drops the
accounting of an old apply receipt after a snapshot must fail while the
physical snapshot and logical cursor remain in place. Do not raise limits,
rerender approvals, or add another simulator as a substitute for these checks.

### Transfer the gRPC payload without copying it again

Keep `ProtobufRaftCodec.EncodeMessage` as the single encoder. The produced
array is new and owned by the caller; check its length against the same
payload limit and wrap the array in `ByteString` only for accepted messages.
The gRPC envelope owns it until serialization completes; do not mutate it,
pool it, or wrap caller-owned bytes. This preserves the payload bytes,
oversize logging and return status, and the transport contract. Keep the
conversion inside the example, not in the core or the adapter's public API.

Measure the real envelope builder with the existing allocation benchmark
runner, outside network I/O, using a representative 31 MiB snapshot. Verify
its payload matches the ordinary adapter encoding; verify oversize rejection
with the component test and exercise a real transfer in the KV smoke. Record
the before/after allocation on the same machine; CI need not enforce an
absolute latency or allocation threshold.

### Document the format and size contract where hosts implement it

The [storage guide](../integration/storage.md) describes detached domain
values and `RaftHardState?`. A stable `IStorage.LogFormat` determines both
the standalone entry sizes used by `GetEntries` and the encoding of the
replicated membership payloads. Stored and unstable entries share the same
size budget definition. Count neither RPC framing nor just the command bytes;
keep the first-entry exception. Every replica and recovery process must
interpret existing and newly written membership payloads consistently; a
codec swap does not negotiate a new log format. Use the existing protobuf
adapter for `raftpb` compatibility.

## Pre-implementation review

- A legacy response from an unknown peer would otherwise hit the facade's
  ignore path. A reserved sender on a proposal would otherwise join the
  proposal lane and could wait for leadership. Both must reach the same
  pre-dispatch validation without changing either lane's normal behavior.
- Keep one delivery queue in the simulator. Separate receipt execution
  from rendering, not from scheduling: a separate queue would silently
  change the order of the pinned scenarios. Route each receipt back to the
  node that issued the work, not to a field in the rendered frame. Explicit
  malformed messages can still use the dedicated fixture ingress in focused tests.
- The transport can adopt its own freshly encoded array without a pool,
  a new codec overload, or a new public API. Exercise the existing envelope
  builder directly from the example tests and microbenchmark; the benchmark
  excludes network, SQLite, and application allocation. The example exposes
  internals only to those test assemblies and compiles its existing linked
  classifier in its own namespace so it cannot be confused with the core copy.
- The public shape, wire descriptor, schema approvals, and historical
  transcripts are unchanged. Verify both full suites and the real-process
  smoke in addition to the targeted checks above.

## Verification results

- Before the fix, eight public-facade regression cases failed on the next
  status call after rejection; the case that throws during a valid receipt's
  trace already faulted as intended. After the fix, all nine pass.
- In isolated copies, admitting a stale typed append receipt advanced the
  unstable offset from 3 to 4 and failed the ABA check. Ignoring a typed
  apply receipt covered by a snapshot left outstanding bytes and failed both
  the focused and simulated accounting checks.
- The `large-snapshot-envelope` smoke workload on macOS arm64 with .NET SDK
  10.0.100 allocated **65,013,080** bytes with the original copy and
  **32,507,176** bytes after transferring the array (checksum
  `6873571993255534759` in both runs). This measures the envelope builder,
  not the network or total process memory.
- Debug and Release each passed 1,233 tests, including unchanged golden,
  managed API, wire, and schema approvals. Formatting, the async Counter
  trace, and the three-process KV smoke passed. A temporary copy of the
  smoke also passed with `76fd690` processes writing the original data and
  the candidate processes reopening the same stores one by one.
