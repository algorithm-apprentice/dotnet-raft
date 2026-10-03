# D23 Deterministic Interaction Harness Design

- **Status:** Accepted
- **Date:** 2026-10-03
- **DAG node:** D23
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D23 adds a deterministic, data-driven cluster harness around the completed
synchronous `RawNode` API. It reproduces the useful structure of the pinned
`rafttest.InteractionEnv`: multiple nodes, explicit `Ready` processing,
in-flight message queues, selective delivery or loss, storage/application
processing, compaction, snapshots, status inspection, and checked-in golden
scenarios.

The harness is parity-test infrastructure. It does not become part of the
runtime NuGet API.

## Goals

- Exercise multi-node behavior through public `RawNode` inputs and outputs.
- Make persistence, application, transport, and message loss explicit.
- Reach reproducible fixed points without wall clocks, tasks, sleeps, or
  background threads.
- Preserve message order and exact `Ready` ownership.
- Maintain physical application state and configuration snapshots according
  to D21.
- Run checked-in interaction scripts with deterministic output.
- Establish the reusable scenario infrastructure used by D26 parity closure.

## Non-goals

- The concurrent `Node` facade, cancellation, channels, or host lifecycle;
  D24 owns those features.
- Asynchronous storage-write messages or append/apply worker queues; D25 owns
  those features and the two pinned `async_storage_writes*.txt` scenarios.
- Random packet loss, random delays, goroutines, or real-time simulation.
- Shipping the harness in `DotnetRaft.dll`.
- Byte-for-byte reuse of pinned Go log messages. Consensus descriptions use
  the accepted D22 C# grammar, and internal logger wording remains C#-specific.
- Porting every pinned scenario in D23. D23 supplies representative golden
  coverage and the complete synchronous command surface; D26 imports the
  remaining relevant synchronous corpus.

## Location and visibility

The harness lives under:

```text
tests/DotnetRaft.Tests/Interaction/
```

All harness types are `internal`. The only production changes permitted by
D23 are deterministic test hooks that remain `internal` and are available to
`DotnetRaft.Tests` through the existing `InternalsVisibleTo` declaration.

No D23 type is exposed as a supported library API.

The hooks are:

```csharp
internal RawNode(
    RaftConfig config,
    Func<int, int> randomOffset);

internal void SetRandomizedElectionTimeoutForTesting(
    int timeout);

internal bool IsFaultedForTesting { get; }
```

The setter rejects nonpositive values and otherwise installs the exact test
value, matching the pinned exported test hook.

## Core model

```csharp
internal sealed class InteractionEnvironment
{
    internal IReadOnlyList<InteractionNode> Nodes { get; }
    internal IReadOnlyList<Message> QueuedMessages { get; }

    internal void AddNodes(
        int count,
        InteractionNodeOptions options);
    internal void Campaign(int nodeIndex);
    internal void TickElection(int nodeIndex);
    internal void TickHeartbeat(int nodeIndex);
    internal void Propose(int nodeIndex, ReadOnlySpan<byte> data);
    internal void ProposeConfiguration(
        int nodeIndex,
        ConfChange change);
    internal void ProposeConfiguration(
        int nodeIndex,
        ConfChangeV2 change);
    internal void ProcessReady(int nodeIndex);
    internal int DeliverMessages(
        MessageType? type,
        params InteractionRecipient[] recipients);
    internal void Stabilize(params int[] nodeIndexes);
    internal void Compact(int nodeIndex, ulong compactThrough);
    internal void TransferLeadership(ulong from, ulong to);
    internal void ForgetLeader(int nodeIndex);
    internal void ReportUnreachable(int nodeIndex, ulong peerId);
    internal void SendSnapshot(int fromIndex, int toIndex);
    internal void SetRandomizedElectionTimeout(
        int nodeIndex,
        int timeout);
}

internal sealed class InteractionNode
{
    internal RawNode RawNode { get; }
    internal MemoryStorage Storage { get; }
    internal RaftConfig Config { get; }
    internal Snapshot GetApplicationSnapshot();
}

internal readonly record struct InteractionRecipient(
    ulong Id,
    bool Drop = false);
```

Node indexes in methods and scripts are one-based at the script boundary and
zero-based internally. Raft IDs are consecutive and start at one.

`QueuedMessages` returns a read-only collection of message clones in send
order. `GetApplicationSnapshot` returns a clone. Mutating either inspection
result cannot affect the queue, storage, or application state. `Nodes` is a
read-only structural view for harness implementation/tests; node members
remain internal test infrastructure rather than an ownership boundary.

## Deterministic construction

Each node uses:

```text
ElectionTick = 3
HeartbeatTick = 1
MaxSizePerMessage = ulong.MaxValue
MaxInflightMessages = int.MaxValue
MaxInflightBytes = ulong.MaxValue
MaxUncommittedEntriesSize = ulong.MaxValue
```

The harness constructs `RawNode` through an internal constructor whose random
election offset always returns zero. A reset therefore chooses exactly
`ElectionTick`, eliminating process-global or cryptographic randomness.

`set-randomized-election-timeout` can override the current timeout with a
positive value. As in the pinned hook, a later role/term reset recomputes the
timeout from the deterministic random source.

### Initial state

`add-nodes` accepts:

- voter and learner IDs;
- an optional snapshot index and data;
- inflight capacity;
- pre-vote and quorum-check settings;
- committed-entry page size;
- configuration-validation and leader-removal behavior; and
- safe or lease-based reads.

With index zero, a node starts with empty storage and configuration.

With a nonzero index:

1. the index must be greater than one, matching the pinned harness
   restriction;
2. a term-one snapshot containing the requested `ConfState` and data is
   applied to `MemoryStorage`;
3. `RaftConfig.Applied` equals the snapshot index; and
4. the application snapshot starts as a detached copy of that snapshot.

`AsyncStorageWrites` is rejected in D23.

## Ready processing

`ProcessReady` performs exactly one synchronous batch:

1. call `Ready` and append `RaftDescriptions.DescribeReady` to the current
   command output;
2. persist the snapshot, entries, and hard state to `MemoryStorage`;
3. restore the application snapshot, if present;
4. apply every committed entry in log order;
5. enqueue detached outbound messages in batch order; and
6. call `Advance` with the exact returned instance.

Calling `ProcessReady` when `HasReady` is false produces and advances the
public `<empty Ready>` batch, matching the pinned harness. `Stabilize` avoids
such calls and only processes nodes for which `HasReady` is true.

The persistence operations are ordered as:

```text
snapshot -> entries -> hard state
```

The in-memory harness cannot simulate a torn generation; D21 remains
authoritative for real hosts' atomic publication and `MustSync` behavior.

### Application state

The deterministic application state machine is an appender:

- normal entry data is appended to `ApplicationSnapshot.Data`;
- V1 configuration context is appended after synchronously calling
  `ApplyConfChange`;
- V2 configuration context is appended after synchronously calling
  `ApplyConfChange`; and
- rejected configuration entries are not used by the D23 corpus.

After each entry:

- snapshot index and term equal the entry index and term;
- configuration equals the latest accepted returned `ConfState`, or the
  preceding configuration for a normal entry; and
- `MemoryStorage.CreateSnapshot` updates the snapshot available to Raft
  without compacting the log.

This snapshot is based on physical application, not the logical `Applied`
cursor. It satisfies D21's restart and snapshot-configuration invariants.

If persistence, decoding, configuration application, snapshot creation, or
`Advance` fails, processing stops and the exception is reported. The harness
does not emit a success-shaped result or retry a partially mutated node.

## Message queue and partitions

Outbound `Ready.Messages` are cloned into one FIFO queue.

`DeliverMessages` processes recipients in caller order. For each recipient it:

1. extracts matching messages without reordering them;
2. optionally filters by one `MessageType`;
3. either logs and drops them or logs and calls the destination node's
   `Step`; and
4. removes handled messages from the queue.

Messages not selected remain in their original relative order.

A recipient cannot be both delivered and dropped in the same command.
Dropping a message to a node that has not been instantiated is allowed.
Delivery to a missing node fails explicitly.

Self-directed or reserved local-storage messages are never droppable. They are
not emitted by synchronous D23, but the rule is retained for D25 composition.

Before stepping a response, the harness checks the exact public-filter order.
It takes the safe continuation path only when:

```text
destination.IsFaultedForTesting == false
&& !RaftMessageTargets.IsLocal(message.From)
&& !MessageClassifier.IsLocal(message.Type)
&& MessageClassifier.IsResponse(message.Type)
&& message.From is absent from destination progress
```

The progress IDs are materialized through `VisitProgress`. When all conditions
hold, the harness emits the same deterministic safe pre-dispatch rejection as
`RawNode`:

```text
Response sender <id> is not a known peer.
```

and continues with later messages. The rejected message is removed and counts
as one handled stabilization work item.

This is the only delivery rejection that does not abort the command. Reserved
senders, local message types, argument/filter errors, trace failures,
faulted-node errors, storage failures, and Raft invariant failures propagate
and stop delivery. The harness never broadly catches
`InvalidOperationException`.

Explicit selective delivery/drop is the partition model. D23 intentionally
does not introduce a stochastic network.

## Stabilization

`Stabilize` repeatedly executes these phases over either the supplied nodes or
all nodes:

1. process one `Ready` for each selected node that has work, in node order;
2. deliver all queued messages for each selected recipient, in node order;
3. repeat while either phase handled work.

The synchronous D23 fixed point has no append/apply worker phase.

The loop limit is `100_000` handled work items. Each processed `Ready` batch
and each delivered, dropped, or safely rejected unknown response consumes one
item; empty phase checks do not. A test-only constructor argument may lower
the limit but not disable it.
For selected-node stabilization, queued messages to unselected recipients are
not actionable and do not consume or appear in the remaining-message count.
Before handling work item `limit + 1`, the harness throws with the selected
node IDs, selected nodes currently reporting `HasReady`, and queued messages
addressed to selected recipients.

Output headers follow the pinned structure:

```text
> 1 handling Ready
  <indented Ready and logger output>
> 2 receiving messages
  <indented message and logger output>
```

## Storage, snapshot, and inspection controls

- `compact <node> <index>` compacts through the supplied index and then prints
  the retained stable log. It first requires:

  ```text
  compactThrough <= ApplicationSnapshot.Metadata.Index
  ```

  Failure leaves storage unchanged. This is D21's physical-application and
  durable-application-snapshot barrier.

- `raft-log <node>` prints stable entries from `FirstIndex` through
  `LastIndex` with `RaftDescriptions.DescribeEntries`. When
  `LastIndex < FirstIndex`, it emits exactly:

  ```text
  log is empty: first index=<first>, last index=<last>
  ```

- `send-snapshot <from> <to>` queues the sender's latest physically applied
  application snapshot as a detached message with `Type=MsgSnap`, sender,
  recipient, and `Term=sender.GetBasicStatus().Term`. It immediately emits
  `RaftDescriptions.DescribeMessage` without a trailing newline.

- `status <node>` prints leader progress in sorted ID order:

  ```text
  1: Replicate match=4 next=5
  2: Probe match=0 next=3 paused
  ```

  Pending snapshots and inflight counts/limits are appended when present.

  Each line has this exact suffix order:

  ```text
  <id>: <ReplicationState> match=<match> next=<next>
      [ learner]
      [ paused]
      [ pendingSnap=<index>]
      [ inactive]
      [ inflight=<count>[full]]
  ```

  The actual output is one line with no indentation or bracket characters.
  `paused` uses effective `IsPaused`. `inactive` appears when
  `RecentActive == false`. `[full]` appears when either inflight count equals
  capacity or the configured byte limit is nonzero and current bytes meet or
  exceed it. Followers produce empty status output, which the command maps to
  `ok`.

- `raft-state` prints every node's local view:

  ```text
  1: Leader (Voter) Term:2 Lead:1
  2: Follower (Voter) Term:2 Lead:1
  ```

## Logger

`InteractionLogger` implements `IRaftLogger` and writes deterministic lines to
the command output:

```text
DEBUG <message>
INFO <message>
WARN <message>
ERROR <message>
```

Levels are `debug`, `info`, `warn`, `error`, and `none`. The selected level
persists across commands. `none` suppresses both logger and normal harness
output, but returned errors remain visible.

The initial level is `debug`, matching the pinned environment.

The output buffer is reset before each command. If a successful command emits
nothing, its result is exactly:

```text
ok
```

## Script grammar

`InteractionScriptRunner` implements the subset of the pinned data-driven
grammar used by Raft interaction files:

```text
# comments and blank separators
command positional-arg key=value key=(v1,v2)
optional input lines
----
expected output
```

Rules:

- files are UTF-8 and line endings normalize to `\n`;
- comments and blank lines separate cases;
- command names, bare arguments, keys, and non-parenthesized values split on
  literal ASCII spaces; tabs are ordinary token bytes; quote characters have
  no lexical meaning and remain
  ordinary token bytes;
- a parenthesized value may nest parentheses and splits only at top-level
  commas; spaces inside a value are preserved except spaces immediately after
  a separating comma;
- input is every line between the command line and `----`;
- expected output continues to the next blank separator or end of file;
- one optional final newline is ignored on both actual and expected output;
- parsing errors include file name and one-based line number; and
- command validation errors identify the command and offending argument.

Consequently, `propose 1 "foo"` proposes the five UTF-8 bytes including both
quotes, while `propose 1 "foo bar"` has two payload-side tokens and fails
the exact-arity rule.

Lexical conversion follows the pinned Go helpers:

- unsigned values are base-ten `ulong` without signs;
- signed counts/timeouts are base-ten `int`;
- booleans accept exactly Go `strconv.ParseBool` spellings:
  `1`, `t`, `T`, `TRUE`, `true`, `True`, `0`, `f`, `F`, `FALSE`, `false`, and
  `False`;
- `MessageType` is a case-sensitive pinned protobuf symbol such as `MsgApp`;
  numeric enum values are rejected in scripts; and
- overflow, underflow, trailing characters, and unknown symbols fail with
  file/line and argument context.

### Exact command contracts

| Command | Contract |
|---|---|
| `add-nodes <count>` | `count` is positive. Options: `voters=(...)`, `learners=(...)`, `inflight=<positive-int>`, `index=<ulong>`, `content=<raw-token>`, `prevote=<bool>`, `checkquorum=<bool>`, `max-committed-size-per-ready=<ulong>`, `disable-conf-change-validation=<bool>`, `read-only=safe|lease-based`, and `step-down-on-removal=<bool>`. Defaults are the deterministic construction values, empty membership/data, index zero, safe reads, and false booleans. `async-storage-writes` is recognized but any true value is rejected until D25. |
| `campaign <node>` | Exactly one node. |
| `compact <node> <index>` | Exactly one node and compact-through index. |
| `deliver-msgs [node ...] [drop=(...)] [type=<MessageType>]` | Bare nodes are delivered. Drop IDs are dropped. At least one recipient is required. The same ID cannot appear in both sets. Type is optional and defaults to all messages. |
| `process-ready <node ...>` | At least one node, processed in argument order. |
| `log-level <level>` | Exactly one of `debug`, `info`, `warn`, `error`, or `none`, case-insensitive. |
| `raft-log <node>` / `status <node>` | Exactly one node. |
| `raft-state` | No arguments. |
| `set-randomized-election-timeout <node> timeout=<positive-int>` | Exactly one node and one timeout. |
| `stabilize [node ...] [log-level=<level>]` | No nodes means all nodes. The optional level applies only during this command and is restored in `finally`, including when stabilization fails. |
| `tick-election <node>` | Calls `RawNode.Tick()` exactly `Config.ElectionTick` times. |
| `tick-heartbeat <node>` | Calls `RawNode.Tick()` exactly `Config.HeartbeatTick` times. It does not require leader role; the node's normal role-dispatched tick behavior is authoritative. |
| `transfer-leadership from=<id> to=<id>` | Both IDs must name instantiated nodes. |
| `forget-leader <node>` | Exactly one node. |
| `send-snapshot <from> <to>` | Exactly two instantiated nodes. |
| `propose <node> <raw-token>` | Exactly one payload token. Quote characters are ordinary bytes and remain in the UTF-8 proposal data. |
| `propose-conf-change <node> [v1=<bool>] [transition=auto|implicit|explicit]` | Input is zero or more whitespace-separated compact changes `v<ID>`, `l<ID>`, `r<ID>`, or `u<ID>`. Defaults are `v1=false` and `transition=auto`. Empty V2 input is leave-joint. V1 requires exactly one operation and `transition=auto`. |
| `report-unreachable <node> <peer>` | Exactly two instantiated nodes; the second node's Raft ID is reported from the first. |
| `process-append-thread <node ...>` / `process-apply-thread <node ...>` | Known syntax, but execution throws the explicit D25 `NotSupportedException`. |

Supported synchronous commands are:

```text
add-nodes
campaign
compact
deliver-msgs
forget-leader
log-level
process-ready
propose
propose-conf-change
raft-log
raft-state
report-unreachable
send-snapshot
set-randomized-election-timeout
stabilize
status
tick-election
tick-heartbeat
transfer-leadership
```

`process-append-thread` and `process-apply-thread` parse as known deferred
commands and return a D25-specific `NotSupportedException` in D23.

## Ownership and thread safety

- The environment and nodes are synchronous and thread-unsafe.
- Node options, snapshots, entries, messages, and configuration changes are
  cloned on ingress.
- Queued messages and application snapshots never alias caller-owned mutable
  protobuf objects.
- Command output is deterministic and uses invariant culture and `\n`.
- Harness callbacks never re-enter a `RawNode`.

## Initial golden corpus

D23 checks in representative C# golden scenarios covering:

1. singleton election, proposal, persistence, and application;
2. three-node election, replication, selective message loss, and recovery;
3. compaction followed by snapshot catch-up; and
4. V1/V2 membership application and local configuration views.

The command surface is implemented completely for synchronous scenarios.
D26 imports and adapts the remaining pinned synchronous files after D24 and
D25 remove the final parity blockers.

## Test plan

### Parser and runner

- comments, blank separators, multiline input, parenthesized lists, quoted
  tokens, and line-ending normalization;
- malformed command, argument, separator, and expected blocks report exact
  file/line context;
- empty output becomes `ok`;
- logger level persists while command output resets; and
- golden mismatches identify file, case, expected, and actual output.

### Node and Ready processing

- deterministic election timeout construction and explicit override;
- snapshot/entries/hard-state persistence;
- exact-instance `Advance`;
- committed pagination across repeated `ProcessReady` calls;
- normal and configuration application update the physical snapshot;
- restored snapshots replace application state before later entries; and
- failures do not advance or report success.

### Network and fixed point

- message extraction preserves queue order;
- delivery/drop filters by recipient and type;
- unselected messages retain relative order;
- missing destinations and conflicting recipient directives fail;
- local messages cannot be dropped;
- selected-node stabilization leaves other nodes/messages untouched; and
- the operation cap detects a non-converging loop.

### Storage and inspection

- compaction exposes the expected retained log;
- a lagging follower receives the latest physical snapshot;
- status and raft-state ordering are stable; and
- returned harness collections and snapshots are detached/read-only.

### Golden scenarios

- all four initial files match exactly;
- every file is executed independently with a fresh environment; and
- repeated runs produce byte-identical output.

## Acceptance criteria

D23 is complete when:

1. the synchronous command surface above is implemented;
2. persistence, application, queue, drop, compaction, and snapshot behavior
   obey this design and D21;
3. representative golden scenarios pass deterministically;
4. D21 and D22 behavior remains unchanged;
5. focused and full tests pass;
6. formatting verification passes;
7. an independent GPT-5.6 Sol review approves the design and implementation;
   and
8. the completed node is committed and pushed before D24 begins.
