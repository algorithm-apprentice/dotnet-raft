# Inside the node facades

Part of the [implementation map](README.md). The
[host guide](../integration/README.md) specifies calls and ordering; this
page explains why the facades enforce the boundaries they do.

## Delivery and acknowledgement

[`RawNode.CreateReady`](../../src/DotnetRaft/RawNode.cs) collects detached
outputs, updates the hard- and soft-state baselines, and marks exactly the
emitted unstable and committed work in flight. Re-reading Ready is not a
peek; accepting it claims work. `RaftLog.Applying` advances only through the
last entry delivered, not through the full commit, because the batch can be
paginated. Acknowledging work that the host never received would skip work
both in the live application and on recovery if that cursor were persisted.

In synchronous mode, one exact batch is outstanding. `RawNode` retains
separate completion metadata, so a later caller mutation or new work cannot
change what `Advance` confirms. The order in `AdvanceCore` matters: first
step deferred self-responses (including the leader's self-append), then
stabilize entries/snapshot and acknowledge the application slice. A self
append response can commit while the acknowledged term is still available
in the unstable view. Completion releases accounting for the acknowledged
batch, not whatever subsequently accumulated. See
[`RawNodeReadyTests`](../../tests/DotnetRaft.Tests/RawNode/RawNodeReadyTests.cs).

The log does not deliver a new committed-entry page while a snapshot is
unstable. This keeps entries after the snapshot from being applied before
the replacement application prefix is ready. Already delivered work is a
separate concern: it still needs its own completion or snapshot coverage.

## Why the asynchronous responses carry evidence

Append and apply have independent FIFO completion streams, so `RawNode`
emits storage requests that carry their own generated responses. Replies
that require persistence are nested with the append rather than exposed
early on the network. The core delivers committed apply work only through
the acknowledged stable prefix. A snapshot can cross the two streams and
supersede older application work; its completion and an old apply response
advance the cursor monotonically, but the old response must still release
its exact accounting.

`UnstableLog.StableTo` checks an entry tuple, but the same tuple can disappear
and return across term changes while writes are still queued. A delayed
confirmation for the former copy does not prove which bytes are currently
durable; entry stabilization also checks the Raft term of the append
response. FIFO order lets a later current-term response attest the earlier
writes in that stream. See the
[ABA walkthrough](../learning/03-ready-and-recovery.md#why-index-entryterm-is-not-enough-for-an-async-append-response)
and [response tests](../../tests/DotnetRaft.Tests/AsyncStorage/AsyncStorageResponseTests.cs).

## Serialize ownership, not client effects

[`RaftNode`](../../src/DotnetRaft/RaftNode.cs) wraps the same `RawNode` with
one owner for commands, proposals, ticks, Ready publication, and stop.
Separate scheduler lanes keep an eligible stream from indefinitely hiding
another. A request can be canceled while waiting; once claimed, the
operation's actual result must win. Otherwise the facade could report a
proposal as canceled after it had already been submitted. A blocked
proposal does not occupy the owner; ticks and peer messages can still
discover a leader and make it eligible.

A caller awaiting a queued result does not need a thread while it waits,
and the owner also awaits work when idle. Once dispatched, a core operation
runs synchronously. A slow stable log read holds up the owner, including
ticks and other queued commands. This remains true with asynchronous
storage writes: that protocol moves write and application work out to the
host, not stable reads out of the core. The
[scheduling tests](../../tests/DotnetRaft.Tests/Node/RaftNodeSchedulingTests.cs)
gate a stable `GetTerm` call to make this distinction observable.

Callback reentry into the owner is rejected, and continuations run outside
the owner to avoid synchronous self-deadlock. Use the
[deterministic race tests](../maintaining/testing.md#exercise-the-boundary-that-changed)
to observe claim, cancel, Ready, and terminal draining without guessing
timing.

## Validation and failure

The host owns peer identity and routing. `RawNode.Step` separates local
storage responses from peer traffic and checks owned message shapes before
core mutation. [`NetworkMessageValidation`](../../src/DotnetRaft/Core/NetworkMessageValidation.cs)
prepares a [`LogSlice`](../../src/DotnetRaft/Core/LogSlice.cs) from an owned
peer Append. `RaftCore.StepPeerMessage` prepares a
[`ReadAcknowledgement`](../../src/DotnetRaft/Read/ReadAcknowledgement.cs)
only when the current leader can consume it. Preparation and dispatch run
in the same serialized operation. The heartbeat handler receives the read
position, not bytes it needs to decode. An ignored message need not interpret
an unused commit or context; an already matched snapshot tuple does not
reinstall its configuration.

Structural configuration checks are shared between network and public
proposal paths. They do not predict whether a future membership change is
legal at its log position. A structural rejection can fail one API request;
a storage fault or a failure during committed configuration application
can leave a partially changed node and is terminal. See
[ADR 0008](../adr/0008-protocol-input-hardening.md) and the
[network validation regressions](../../tests/DotnetRaft.Tests/RawNode/RawNodeNetworkValidationTests.cs).
