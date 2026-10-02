# D22 Bootstrap, Status, and Diagnostics Design

- **Status:** Accepted
- **Date:** 2026-10-04
- **DAG node:** D22
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D22 completes the synchronous public integration surface around `RawNode`.
It adds first-use bootstrap and named restart helpers, detached status
snapshots, deterministic human-readable descriptions, and optional structured
state-machine tracing.

The concurrent `Node` wrapper remains deferred to D24.

## Goals

- Bootstrap an empty `RawNode` with the pinned term-one V1 configuration
  entries.
- Preserve constructor-based restart behavior behind an explicit named helper.
- Expose immutable basic, configuration, and replication-progress status
  values without leaking tracker internals.
- Produce deterministic descriptions for protocol state, entries, messages,
  `Ready` batches, and configuration changes.
- Expose opt-in detached trace events at important deterministic state-machine
  boundaries.
- Make trace callback failures explicit and prevent reuse of a partially
  mutated facade.

## Non-goals

- Concurrent `Node`, `StartNode`, `RestartNode`, cancellation, or lifecycle;
  D24 owns those APIs.
- Automatic persistence, transport, application, snapshot creation, or
  compaction.
- Parsing description strings back into protocol values.
- A logging framework replacement; `IRaftLogger` remains the internal
  diagnostic log sink.
- Timestamps, wall-clock ordering, sleeps, or nondeterministic trace fields.
- Final public API compatibility guarantees; D26 owns release hardening.

## Bootstrap API

```csharp
public sealed record Peer
{
    public Peer(ulong id);
    public Peer(ulong id, ByteString context);

    public ulong Id { get; }
    public ByteString Context { get; }
}

public sealed partial class RawNode
{
    public static RawNode Start(
        RaftConfig config,
        IEnumerable<Peer> peers);

    public static RawNode Restart(RaftConfig config);

    public void Bootstrap(IEnumerable<Peer> peers);
}
```

`Start` constructs a `RawNode`, calls `Bootstrap`, and returns it only after
bootstrap succeeds. `Restart` is a named constructor for existing storage and
is behaviorally identical to `new RawNode(config)`.

D24 later composes these synchronous helpers into the concurrent
`StartNode`/`RestartNode` surface.

`Start` materializes and validates the peer sequence before constructing the
`RawNode`. Invalid peer input therefore cannot emit an `Initialized` trace for
an object that is never returned.

### Validation

`Bootstrap` materializes and validates all input before core mutation:

- the peer sequence is non-null and nonempty;
- no element is null;
- every ID is nonzero and is not a reserved local storage target;
- IDs are unique;
- the facade is still in its never-used lifecycle state;
- the storage/log last index is zero;
- hard state is empty;
- restored membership and progress are empty;
- the facade is not faulted.

The membership and hard-state checks deliberately strengthen the pinned
last-index-only guard. They reject accidentally bootstrapping storage that was
manually initialized at index zero with membership or term/vote state.

The facade starts in a private `Fresh` lifecycle state. Status inspection does
not consume freshness. After argument validation, any state-machine input,
reporting call, configuration application, `Ready` acceptance, or bootstrap
mutation changes it permanently to `Active`, even if that operation is later
dropped without state change. This includes:

- `Tick`, `Campaign`, proposal, read-index, transfer, forget-leader, and report
  methods;
- accepted public `Step`;
- `ApplyConfChange`;
- `Ready`; and
- `Advance`.

`HasReady`, `GetBasicStatus`, `GetStatus`, and description helpers are
read-only and do not consume freshness.

Peer-sequence validation failures on a fresh facade leave it fresh and permit
a corrected retry. A lifecycle/storage/hard-state/membership bootstrap
precondition failure terminally faults that facade; the caller must discard
it. Any failure after bootstrap mutation begins also faults the facade.

This prevents queued messages, read states, previous soft-state baselines, or
other effects from pre-bootstrap use from leaking into the promised first
bootstrap `Ready`.

### Bootstrap entries

For peers in caller order, bootstrap creates:

```text
Entry.Type  = EntryConfChange
Entry.Term  = 1
Entry.Index = position + 1
Entry.Data  = serialized ConfChangeAddNode
```

Each `ConfChange.Context` is the peer's immutable `ByteString` context.

The core then:

1. becomes a term-one follower with no leader;
2. appends all bootstrap entries;
3. commits the final bootstrap index;
4. applies every add-node change to the internal tracker after all entries
   have been appended; and
5. leaves `Log.Applied` unchanged so the host still observes and applies every
   configuration entry through `Ready.CommittedEntries`.

Applying the entries internally makes the new node immediately campaignable.
The host later calling `ApplyConfChange` for the committed entries repeats the
idempotent add operations, matching the pinned bootstrap path.

Bootstrap configuration entries are initialization records, not rejectable
application proposals. For every bootstrap entry exposed in
`Ready.CommittedEntries`, the host must, in log order:

1. durably publish the entry and hard state as required by D21;
2. synchronously call the matching `ApplyConfChange`; and
3. only then `Advance` that page.

The D21 rejected-configuration no-op path does not apply to bootstrap entries.
If the host cannot accept one, it must not advance that page and must
reconstruct from the last complete durable/application generation. Ordinary
campaign, proposal, and message input begins only after all bootstrap pages
have been applied and advanced.

The first `Ready` contains:

- term-one hard state with commit equal to the peer count;
- every unstable bootstrap entry;
- the longest committed prefix permitted by D21
  `MaxCommittedSizePerReady` pagination;
- no synthetic outbound messages; and
- `MustSync == true`.

The unstable entry list is not paginated. If the committed list is shorter
than the bootstrap list, each `Advance` exposes the next committed prefix in a
later `Ready` until all bootstrap entries are applied. Those later
application-only pages contain no unstable entries and normally have
`MustSync == false`.

Each initialized `Progress.Next` equals:

```text
max(final bootstrap index, 1)
```

For a nonempty peer list this is the peer count, matching the pinned changer.
Campaign/reset later moves replication progress to the usual
`LastIndex + 1`.

## Restart contract

`RawNode.Restart(config)` does not inspect or modify application state. The
host remains responsible for the D21 recovery invariants:

```text
RaftConfig.Applied = physically recovered application index
HardState.Commit >= RaftConfig.Applied
initial ConfState = membership at that recovered point
```

No bootstrap entries are generated during restart. Existing committed but
unapplied entries begin in the first `Ready` and continue across successive
`Ready`/`Advance` pages under D21 `MaxCommittedSizePerReady` pagination.

## Status model

```csharp
public readonly record struct BasicStatus(
    ulong Id,
    ulong Term,
    ulong Vote,
    ulong Commit,
    ulong LeaderId,
    RaftRole Role,
    ulong Applied,
    ulong LeaderTransferee);

public enum ReplicationState
{
    Probe,
    Replicate,
    Snapshot,
}

public readonly record struct ProgressStatus(
    ulong Match,
    ulong Next,
    ulong LastSentCommit,
    ReplicationState State,
    ulong PendingSnapshot,
    bool RecentActive,
    bool AppendFlowPaused,
    bool IsPaused,
    bool IsLearner,
    int InflightCount,
    ulong InflightBytes,
    int InflightCapacity,
    ulong MaxInflightBytes);

public sealed class ConfigurationStatus
{
    public IReadOnlyList<ulong> Voters { get; }
    public IReadOnlyList<ulong> VotersOutgoing { get; }
    public IReadOnlyList<ulong> Learners { get; }
    public IReadOnlyList<ulong> LearnersNext { get; }
    public bool AutoLeave { get; }
}

public sealed class Status
{
    public BasicStatus Basic { get; }
    public ConfigurationStatus Configuration { get; }
    public IReadOnlyDictionary<ulong, ProgressStatus> Progress { get; }
}

public sealed partial class RawNode
{
    public BasicStatus GetBasicStatus();
    public Status GetStatus();
    public void VisitProgress(
        Action<ulong, ProgressStatus> visitor);
}
```

`BasicStatus` contains scalar snapshots only and does not expose mutable
protobuf state.

`ConfigurationStatus` copies sorted membership IDs into read-only
collections.

`ProgressStatus` copies every externally useful replication field, including
inflight counts and byte accounting, without exposing `Progress` or
`InflightWindow`.

`GetStatus` always includes configuration. Its progress dictionary is a
read-only wrapper over a `SortedDictionary<ulong, ProgressStatus>` and:

- contains sorted, detached values while the local node is leader; and
- is empty for followers, candidates, and pre-candidates.

Unlike the pinned nullable Go map, the C# collection is never null.

`VisitProgress` visits all tracked progress records in ascending ID order,
regardless of role, and passes detached value snapshots. It materializes the
complete ordered `(id, ProgressStatus)` array before invoking the first
callback, so callback activity cannot change the point-in-time result. A null
visitor throws.

Status acquisition is read-only, may occur while a `Ready` is outstanding, and
does not consume work.

`BasicStatus.Applied` is the D21 logical acknowledged/admitted cursor. After an
early `Advance`, it can be ahead of the host's physical-applied cursor.

`AppendFlowPaused` exposes the stored progress flag. `IsPaused` exposes the
effective state-machine result and is always true in snapshot state.

`Status.ToString()` returns this exact compact JSON schema and field order:

```text
{"id":"<hex>","term":<decimal>,"vote":"<hex>","commit":<decimal>,
"lead":"<hex>","raftState":"<RaftRole>","applied":<decimal>,
"progress":{"<hex>":{"match":<decimal>,"next":<decimal>,
"state":"<ReplicationState>"}},"leadtransferee":"<hex>"}
```

The actual string contains no whitespace or line breaks. Empty progress is
`"progress":{}`. Progress keys are sorted numerically before hexadecimal
formatting. Configuration is intentionally omitted from JSON, matching the
pinned status string; callers use the typed `Configuration` property. IDs and
votes use lowercase hexadecimal without a prefix.

For example:

```json
{"id":"1","term":2,"vote":"1","commit":3,"lead":"1","raftState":"Leader","applied":3,"progress":{"1":{"match":3,"next":4,"state":"Replicate"}},"leadtransferee":"0"}
```

Mutating returned collections is impossible and cannot affect the core.

## Deterministic descriptions

```csharp
public delegate string EntryFormatter(
    ReadOnlySpan<byte> data);

public static class RaftDescriptions
{
    public static string DescribeHardState(HardState state);
    public static string DescribeSoftState(SoftState state);
    public static string DescribeConfState(ConfState state);
    public static string DescribeSnapshot(Snapshot snapshot);
    public static string DescribeReady(
        Ready ready,
        EntryFormatter? formatter = null);
    public static string DescribeMessage(
        Message message,
        EntryFormatter? formatter = null);
    public static string DescribeEntry(
        Entry entry,
        EntryFormatter? formatter = null);
    public static string DescribeEntries(
        IEnumerable<Entry> entries,
        EntryFormatter? formatter = null);
    public static string DescribeConfChange(ConfChange change);
    public static string DescribeConfChange(ConfChangeV2 change);
}
```

Formatting uses invariant culture and `\n`, never current culture or platform
line endings.

The default entry formatter emits quoted bytes:

- printable ASCII is preserved;
- quotes and backslashes are escaped;
- common controls use `\n`, `\r`, and `\t`; and
- all other bytes use lowercase `\xNN`.

Configuration entry payloads decode to the pinned compact change notation:

```text
v<ID>  add voter
l<ID>  add learner
r<ID>  remove
u<ID>  update
```

Malformed configuration payloads produce an explicit
`invalid ConfChange payload:<lowercase-hex>` or
`invalid ConfChangeV2 payload:<lowercase-hex>` token instead of including a
version-dependent protobuf exception message. Direct method arguments and
entry collections still reject nulls.

Message descriptions:

- render ordinary IDs as lowercase hexadecimal;
- render zero as `None`;
- render reserved targets as `AppendThread` or `ApplyThread`;
- include rejection hints, commit, vote, entries, snapshots, and nested
  responses; and
- indent nested messages deterministically.

`DescribeReady` follows the pinned category order and returns
`<empty Ready>` when all categories are empty.

### Exact description grammar

Golden output uses the following forms:

```text
HardState:      Term:2 Vote:1 Commit:3
zero vote:      Term:2 Commit:3
SoftState:      Lead:1 State:Leader
ConfState:      Voters:[1 2] VotersOutgoing:[] Learners:[3] LearnersNext:[] AutoLeave:false
Snapshot:       Index:5 Term:2 ConfState:<ConfState>
Normal entry:   1/2 EntryNormal "hello\x00world"
V1 entry:       1/2 EntryConfChange v3
V2 entry:       1/2 EntryConfChangeV2 v3 l4
Unknown change: 1/2 EntryConfChangeV2 unknown9
```

`DescribeHardState` always emits term and commit and emits
` Vote:<decimal>` only when vote is nonzero. `DescribeSoftState` always emits
leader ID in decimal. `DescribeConfState` preserves each protobuf list's order,
uses decimal IDs separated by one space, and uses lowercase `true`/`false`.
`DescribeSnapshot` uses only metadata and emits no snapshot data.

An entry is:

```text
<term>/<index> <EntryType>[ <formatted-data>]
```

Term and index are decimal. Known protocol enum values use their symbol names
as declared in `raft.proto`; unknown enum values use invariant decimal. A
custom `EntryFormatter` result is inserted verbatim as `<formatted-data>`.

The default formatter encloses data in ASCII quotes. Bytes `0x20` through
`0x7e` are literal except `"` and `\`, which become `\"` and `\\`.
Line-feed, carriage-return, and tab become `\n`, `\r`, and `\t`; every other
byte becomes `\xhh` with exactly two lowercase hexadecimal digits.

`DescribeEntries` appends `\n` after every entry, including the final entry.
Multiple compact changes are separated by one ASCII space.

Direct V1 and V2 configuration descriptions first normalize to V2 and use:

```text
transition:ConfChangeTransitionAuto changes:{type:ConfChangeAddNode node_id:2} context:"ctx"
```

Each later change adds one
` changes:{type:<enum> node_id:<decimal>}` segment. Empty context is omitted.
Context uses the default byte-quoting grammar. Unknown enum values use their
invariant decimal representation.

Protocol enum rendering deliberately uses pinned protobuf symbols, not
generated C# member names. In particular:

```text
ConfChangeTransition.Auto          -> ConfChangeTransitionAuto
ConfChangeTransition.JointImplicit -> ConfChangeTransitionJointImplicit
ConfChangeTransition.JointExplicit -> ConfChangeTransitionJointExplicit
```

`EntryType`, `MessageType`, and `ConfChangeType` use the corresponding
`raft.proto` symbol in the same way.

Messages use:

```text
<from>-><to> <MessageType> Term:<term> Log:<logTerm>/<index>
```

Term, log term, and index are always decimal and always present. Optional
segments are appended in exactly this order:

```text
 Rejected (Hint: <rejectHint>)            when Reject is true
 Commit:<commit>                          when Commit is nonzero
 Vote:<vote>                              when Vote is nonzero
 Entries:[<entry>]                        for one entry
 Entries:[
<indent>  <entry-1>
<indent>  <entry-N>
<indent>]                                 for multiple entries
\n<indent>  Snapshot: <snapshot>          for a nonempty snapshot
 Responses:[
<nested-message at indent + two spaces>
<indent>]                                 for one or more responses
```

A snapshot is nonempty exactly when its metadata index is nonzero. For
multiple entries, each entry starts after `\n`; the closing bracket also starts
after `\n`. Each response starts after `\n`, recursively uses the same grammar
with two more indentation spaces, and the closing bracket starts after `\n`.
`DescribeMessage` itself has no trailing newline.

`DescribeReady` uses this exact category order and labels:

```text
Ready MustSync=<true|false>:
<SoftState>
HardState <HardState>
ReadStates:[<index>/<quoted-context> ...]
Entries:
<entries>
Snapshot <Snapshot>
CommittedEntries:
<entries>
Messages:
<messages>
```

Absent categories emit nothing. Read states are separated by one ASCII space.
Every emitted category, including the final one, ends in `\n`; entry rendering
already supplies that newline, and each message is followed by one. A nonempty
result therefore has a trailing newline. The empty result is exactly
`<empty Ready>` with no newline.

Booleans are lowercase. Numeric values use invariant decimal except message
targets, which use the target rules above. Protocol enums use their
`raft.proto` symbols; public `RaftRole` and `ReplicationState` values use their
C# member names. Unknown enum values use invariant decimal.

## Structured tracing

```csharp
public interface IRaftTraceSink
{
    void Trace(RaftTraceEvent traceEvent);
}

public enum RaftTraceEventType
{
    Initialized,
    BecameFollower,
    BecamePreCandidate,
    BecameCandidate,
    BecameLeader,
    CommitAdvanced,
    EntriesAppended,
    ConfigurationProposed,
    ConfigurationApplied,
    ReadyAccepted,
    MessageSent,
    MessageReceived,
}

public readonly record struct RaftTraceMessage(
    MessageType Type,
    ulong From,
    ulong To,
    ulong Term,
    ulong LogTerm,
    ulong Index,
    ulong Commit,
    ulong Vote,
    bool Reject,
    ulong RejectHint,
    int EntryCount,
    ulong SnapshotIndex);

public sealed class RaftTraceEvent
{
    public RaftTraceEventType Type { get; }
    public BasicStatus Status { get; }
    public ulong LastLogIndex { get; }
    public ConfigurationStatus Configuration { get; }
    public RaftTraceMessage? Message { get; }
    public string? Detail { get; }
}
```

`RaftConfig` adds:

```csharp
public IRaftTraceSink? TraceSink { get; init; }
```

When no sink is configured, tracing adds one null check and no event
allocation.

When configured, events are emitted synchronously after the represented state
mutation, except `MessageReceived`, which is emitted after public/core input
validation and before term/role dispatch. Events contain only detached scalar
or immutable status values.

All message types, including local messages, pre-vote traffic, storage message
types, and ignored but valid messages, receive generic send/receive events.
This intentionally provides a broader diagnostic stream than the pinned TLA
build's selected message subset.

The exact event boundaries and order are:

| Operation | Events |
|---|---|
| Construction/restart | One `Initialized` event after storage, hard state, configuration, applied cursor, and initial follower role are fully restored. The constructor's initial follower setup does not emit `BecameFollower`. Startup restoration emits no `ConfigurationApplied`, `CommitAdvanced`, or `EntriesAppended` events. |
| Later follower transition | `BecameFollower` after reset and role/leader installation. |
| Pre-election | `BecamePreCandidate` after the volatile role transition, then one `MessageSent` per queued pre-vote response/request. |
| Election | `BecameCandidate` after term/vote/reset mutation, then one `MessageSent` per queued vote response/request. |
| Leader transition | `BecameLeader` after role/progress mutation, then `EntriesAppended` for the no-op batch, followed by queued `MessageSent` events. |
| Accepted inbound/local step | `MessageReceived` before term/role dispatch. Ignored valid messages emit no later event unless they queue output. |
| Accepted leader proposal | One `ConfigurationProposed` per configuration entry that remains a configuration entry, in entry order; one `EntriesAppended` for the complete appended batch; then queued `MessageSent` events. Entries converted to normal no-ops do not emit `ConfigurationProposed`. |
| Accepted follower append | `EntriesAppended` once only when at least one new/replacement entry is appended, then `CommitAdvanced` if commit increases, then queued `MessageSent`. Empty append/heartbeat requests emit no append event. |
| Successful commit | `CommitAdvanced` only when `Log.Committed` increases, after the new value is installed. No event is emitted for a no-op commit check. |
| Configuration application | `ConfigurationApplied` after tracker/configuration installation. `Detail` is the resulting `DescribeConfState` text. |
| Accepted full snapshot restore | `ConfigurationApplied`, then `CommitAdvanced`, then queued `MessageSent`. Obsolete or fast-forward-only snapshots omit `ConfigurationApplied`; fast-forward emits `CommitAdvanced` only if commit increases. |
| Bootstrap | `BecameFollower`, one `EntriesAppended` for the full bootstrap batch, one `CommitAdvanced`, then one `ConfigurationApplied` per peer in caller order. |
| Ready acceptance | `ReadyAccepted` after baselines, queues, unstable/applying cursors, acknowledgement metadata, and the outstanding instance are installed, immediately before returning the batch. |

`EntriesAppended` is once per nonempty append batch, including no-op,
configuration, follower-replication, and bootstrap batches. Its `Detail` is:

```text
count:<N> first:<first-index> last:<last-index>
```

`ConfigurationProposed.Detail` is the exact direct configuration description
defined above. Events without defined detail use `null`.

No wall-clock timestamp is included. Event order is therefore the serialized
Raft operation order.

Trace callbacks must not re-enter the same `RawNode`.

### Trace failures

An exception from `IRaftTraceSink.Trace` is wrapped in
`RaftTracingException` and propagated.

Construction failure returns no node. During later operations, the exception
may occur after a state mutation, so `RawNode` enters the same terminal faulted
state used for partial configuration-application and `Advance` failures.
Any outstanding `Ready` and its acknowledgement metadata become invalid.
Subsequent mutating, readiness, bootstrap, and reporting operations reject use
of that instance.

`GetBasicStatus`, `GetStatus`, and `VisitProgress` remain available after a
fault for diagnostics. Their values describe the possibly partial in-memory
state and must never be treated as persisted recovery metadata.

The host must discard and reconstruct the node from the actually durable
generation, using D21's physical-applied cursor and matching `ConfState`.

No trace exception is swallowed or converted to a successful result.

### Facade operation and reentrancy guard

Every public `RawNode` entry point participates in one same-instance operation
guard. A second call on the same instance before the first returns throws
`InvalidOperationException` before core dispatch. This includes callback
reentry from trace sinks and progress visitors.

All trace-capable boundaries catch `RaftTracingException`, set the terminal
fault, invalidate any outstanding `Ready`, and rethrow. Expected argument,
lifecycle, peer-filtering, and `ProposalDroppedException` failures do not fault
ordinary operations. `ApplyConfChange`, `Advance`, and bootstrap retain their
stronger D21/D22 partial-mutation fault rules.

If a trace callback attempts reentry and does not catch the rejection, the
outer trace call is considered failed, is wrapped in `RaftTracingException`,
and faults the facade. If the callback catches the reentrancy rejection and
returns normally, the outer operation may complete.

## Ownership

- `Peer.Context` is immutable.
- Bootstrap serializes detached configuration entries.
- Every status collection is copied and read-only.
- Progress and configuration mutation after status capture cannot alter the
  captured status.
- Trace events do not retain live tracker, progress, message, entry, or
  configuration objects.
- Description methods never mutate inputs.

## Test plan

### Bootstrap and restart

- empty peer input fails without mutation;
- null peers, invalid IDs, reserved IDs, and duplicate IDs fail atomically;
- nonempty log, hard state, or membership rejects bootstrap;
- contexts and caller order are preserved in exact V1 entry bytes;
- all entries are appended before progress is initialized, so every
  `Progress.Next` equals `max(final bootstrap index, 1)`;
- first `Ready` has the exact hard state, every unstable entry, paginated
  committed prefix, and `MustSync`;
- finite committed-size limits deliver bootstrap and restart application work
  across exact gap-free pages, always including at least the first pending
  entry;
- every bootstrap entry is accepted and passed to `ApplyConfChange` in order;
  attempting to use D21's rejection path is documented as invalid host usage;
- applying and advancing all bootstrap pages makes the node idle and ready for
  ordinary campaigning;
- `Start` returns an equivalent bootstrapped node;
- `Restart` emits only existing committed-but-unapplied work and creates no
  bootstrap entries.

### Status

- basic status covers follower, candidate, leader, applied, and transfer
  fields;
- full status includes configuration in every role;
- progress is present only for leaders;
- `VisitProgress` is sorted and works in every role;
- progress/configuration/status values remain unchanged after later core
  mutation;
- collection mutation is impossible;
- inflight and snapshot progress fields are exact;
- `Applied` is documented and tested as the logical cursor after early
  `Advance`;
- deterministic JSON is stable across insertion orders and cultures;
- status inspection does not consume or alter an outstanding `Ready`.

### Descriptions

- normal entries use default and custom formatters;
- binary escaping is exact;
- V1/V2 entries and direct configuration changes are deterministic;
- malformed configuration payloads are explicit;
- snapshots, hard state, soft state, and configuration state are exact;
- single/multiple entries, rejection hints, commit, vote, snapshot, and nested
  responses are covered;
- reserved and ordinary message targets are exact;
- empty and populated `Ready` output order is exact;
- formatting is invariant under non-English current culture;
- inputs remain unchanged.

### Tracing

- no sink produces no events or allocations attributable to trace objects;
- initialization and role transitions are ordered;
- send/receive events contain detached message scalars;
- commit, append, configuration proposal/application, and `Ready` events are
  emitted once at their successful boundaries;
- event status/configuration snapshots survive later core mutation;
- trace callbacks receive no wall-clock data;
- constructor callback failure propagates;
- throwing-sink cases cover receive-before-dispatch, send/transition/append
  after mutation, `ReadyAccepted`, `Advance`, and bootstrap;
- callback failure after mutation faults `RawNode`, invalidates an outstanding
  batch, and blocks later non-status use;
- same-instance reentry is rejected before dispatch, both when caught and when
  allowed to escape from the callback;
- a non-reentrant recording sink does not change consensus output.

## Acceptance criteria

D22 is complete when:

1. bootstrap and named restart behavior match the documented synchronous
   contract;
2. status and progress snapshots expose no mutable core state;
3. deterministic descriptions cover every public protocol category;
4. opt-in traces are detached, ordered, and fail explicitly;
5. D21 `Ready`/`Advance` behavior remains unchanged;
6. focused and full tests pass;
7. formatting verification passes;
8. an independent review approves the implementation; and
9. the completed node is committed and pushed before D23 begins.
