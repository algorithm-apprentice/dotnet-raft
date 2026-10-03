# D24 Concurrent Node Wrapper Design

- **Status:** Accepted
- **Date:** 2026-10-03
- **DAG node:** D24
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D24 adds the host-facing concurrent facade around the synchronous,
thread-unsafe `RawNode`. A single background loop owns the `RawNode` and
serializes ticks, protocol inputs, proposals, configuration application,
status inspection, `Ready` delivery, advancement, and lifecycle transitions.

The wrapper follows the pinned `node.go` behavior while using .NET
`Task`, `ValueTask`, `CancellationToken`, and
`System.Threading.Channels` instead of goroutines and Go channels.

## Goals

- Make one `RawNode` safely usable by concurrent callers.
- Preserve deterministic single-owner state-machine execution.
- Provide cancellable asynchronous commands and one ordered Ready stream.
- Block proposals while no leader is known, matching the pinned proposal
  channel.
- Buffer a bounded number of nonblocking ticks.
- Make stop and terminal-fault behavior explicit and unblock every waiter.
- Preserve all D21 Ready/persistence/application ordering contracts.

## Non-goals

- Asynchronous storage-write messages or completion responses; D25 owns them.
- Persistence, transport, application state, timers, retries, or networking.
- Multiple Ready consumers.
- Automatically applying configuration changes.
- Automatic proposal retry or delivery guarantees.
- Structural refactoring of `RaftCore`.

## Public API

```csharp
public interface IRaftNode : IAsyncDisposable
{
    Task Completion { get; }
    Status? TerminalStatus { get; }

    void Tick();

    ValueTask CampaignAsync(
        CancellationToken cancellationToken = default);
    ValueTask ProposeAsync(
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default);
    ValueTask ProposeConfChangeAsync(
        ConfChange change,
        CancellationToken cancellationToken = default);
    ValueTask ProposeConfChangeAsync(
        ConfChangeV2 change,
        CancellationToken cancellationToken = default);
    ValueTask StepAsync(
        Message message,
        CancellationToken cancellationToken = default);
    ValueTask ForgetLeaderAsync(
        CancellationToken cancellationToken = default);
    ValueTask ReadIndexAsync(
        ReadOnlyMemory<byte> context,
        CancellationToken cancellationToken = default);
    ValueTask TransferLeadershipAsync(
        ulong transferee,
        CancellationToken cancellationToken = default);
    ValueTask ReportUnreachableAsync(
        ulong id,
        CancellationToken cancellationToken = default);
    ValueTask ReportSnapshotAsync(
        ulong id,
        SnapshotStatus status,
        CancellationToken cancellationToken = default);

    ValueTask<Ready> WaitForReadyAsync(
        CancellationToken cancellationToken = default);
    ValueTask AdvanceAsync(
        CancellationToken cancellationToken = default);

    ValueTask<ConfState> ApplyConfChangeAsync(
        ConfChange change,
        CancellationToken cancellationToken = default);
    ValueTask<ConfState> ApplyConfChangeAsync(
        ConfChangeV2 change,
        CancellationToken cancellationToken = default);

    ValueTask<Status> GetStatusAsync(
        CancellationToken cancellationToken = default);

    ValueTask StopAsync();
}

public sealed class RaftNode : IRaftNode
{
    public static RaftNode Start(
        RaftConfig config,
        IEnumerable<Peer> peers);

    public static RaftNode Restart(
        RaftConfig config);
}

public sealed class RaftNodeStoppedException
    : InvalidOperationException;

public sealed class RaftNodeFaultedException
    : InvalidOperationException;
```

`Start` validates peers before constructing the node by using D22
`RawNode.Start`. `Restart` uses `RawNode.Restart`. Construction starts the
background loop before returning.

`AsyncStorageWrites == true` remains rejected by `RawNode` until D25.

## Ownership

All mutable input is detached before enqueue:

- `Message`, `ConfChange`, and `ConfChangeV2` are cloned;
- proposal and read-index memory is copied;
- peer sequences are materialized by `RawNode.Start`; and
- caller cancellation tokens are never stored after their request completes.

`Ready`, status, and configuration outputs retain the D21/D22 detached
ownership guarantees.

Caller mutation after an async method returns its `ValueTask` cannot alter the
eventual state-machine input.

## Serialized command loop

The node owns:

```text
unbounded multi-writer/single-reader command Channel
bounded multi-writer/single-reader tick Channel (capacity 128)
one background loop Task
one optional pending Ready waiter
one optional outstanding Ready
FIFO blocked-proposal queue
```

The loop is the only code that calls instance methods on `RawNode`.

At each iteration it:

1. discards a canceled pending Ready waiter;
2. purges canceled blocked proposals regardless of leader/membership state;
3. if a Ready waiter exists, no batch is outstanding, and `HasReady` is true,
   claims the waiter, calls `Ready`, stores that exact instance, and completes
   the waiter;
4. services at most one ordinary command, one eligible blocked proposal, and
   one tick per scheduler round; and
5. rotates which nonempty lane starts each round.

FIFO order is preserved within the command lane and within the proposal lane.
The stop flag is checked before every claim, including between lanes in one
round.
No continuously nonempty lane can prevent another nonempty lane from
receiving one dispatch per round. `AdvanceAsync`, configuration application,
status, and D25 storage-completion `StepAsync` commands therefore cannot be
starved by proposal backlog, and proposals cannot indefinitely starve ticks.

The loop never runs user continuations inline. Every request completion source
uses `RunContinuationsAsynchronously`.

## Request claiming and cancellation

Every cancellable request has three states:

```text
waiting -> claimed -> completed
       \-> canceled
```

Cancellation is honored only while waiting. Claiming and cancellation are
serialized by a request-local lock:

- if cancellation wins before claim, the operation is never dispatched;
- if claim wins, cancellation registration is removed, the operation executes
  exactly once, and its result or exception wins;
- cancellation never rolls back a dispatched state-machine input.

The state transition is performed while holding the request lock.
Cancellation registration disposal and task completion always occur after
releasing it, avoiding deadlock with an already-running callback.

This rule applies to ordinary commands, blocked proposals, and Ready waits.

Pre-canceled tokens return canceled `ValueTask`s without enqueueing.

Every successful waiting-to-canceled transition writes a coalesced wake pulse
to the owner command channel. The loop can therefore unlink canceled Ready and
proposal requests even when no leader, tick, or other command arrives.

## Proposal gating

`ProposeAsync`, both configuration proposal overloads, and a `StepAsync`
carrying `MsgProp` enter one FIFO proposal queue.

A proposal can be claimed only when:

```text
BasicStatus.LeaderId != 0
&& local node has not been observed losing all local progress
```

While blocked, cancellation and stop remain effective. Once a leader becomes
known, blocked proposals retain FIFO order relative to later proposals while
the fair scheduler continues interleaving other lanes.

`ProposeAsync` completes with the exact result of `RawNode.Propose`, including
`ProposalDroppedException`. The configuration proposal overloads also wait
for core dispatch and expose explicit drops rather than returning
success-shaped results.

The wrapper tracks full local progress membership, not only voters:

- initialization after bootstrap/restart records whether local progress
  exists;
- after every operation that can replace/install the tracker, including
  `ApplyConfChangeAsync` and accepted snapshot `StepAsync`, the loop refreshes
  local progress through `VisitProgress`;
- if progress existed before and is absent afterward, proposals are disabled;
- learner progress counts as present, so voter-to-learner demotion does not
  disable forwarding;
- if local progress later appears, the gate is re-enabled; and
- a catching-up node that has never observed local progress is not treated as
  removed.

Leader knowledge remains the second independent gate.

## Network Step filtering

`StepAsync` mirrors pinned `Node.Step` before entering `RawNode`:

- `null` is rejected;
- classifier-local messages from ordinary network senders are ignored and
  complete successfully;
- messages from reserved storage-thread senders fail explicitly until D25;
- response messages from senders absent from progress are silently ignored;
  and
- every other message is stepped through the D21 network facade.

Filtering and dispatch use the owned clone. Unknown-response inspection and
`RawNode.Step` occur in the same serialized loop turn.

For `MsgProp`, the wrapper:

- routes the command through the proposal lane;
- overwrites `From` with the local node ID immediately before dispatch,
  matching pinned `node.go`;
- waits for core dispatch; and
- propagates `ProposalDroppedException` or any other dispatch exception.

Dedicated proposal methods use the same completion semantics.

## Ready and Advance

`WaitForReadyAsync` is a single-consumer rendezvous:

- at most one Ready waiter may exist;
- no second waiter is accepted while a Ready is outstanding;
- cancellation before claim leaves `RawNode` untouched;
- the loop does not call `Ready` until a live waiter exists; and
- commands and ticks may continue while no waiter exists, allowing larger
  batches.

The returned `Ready` remains outstanding until `AdvanceAsync`.

`AdvanceAsync`:

- requires one outstanding batch;
- calls `RawNode.Advance` with the exact stored instance;
- clears it only after successful advancement; and
- rejects missing, duplicate, or post-stop advancement.

Commands, ticks, status, and required `ApplyConfChangeAsync` calls remain
available while a batch is outstanding. New Ready delivery remains blocked,
and newer work becomes visible after advancement, preserving D21.

## Configuration application

Both `ApplyConfChangeAsync` overloads execute synchronously on the owner loop
and return the detached `ConfState`.

Hosts must retain the D21 rule:

- accepted committed changes are applied in log order before advancing across
  them;
- rejected changes are deterministic application no-ops and are not passed to
  `ApplyConfChangeAsync`; and
- bootstrap-generated configuration entries are never rejected.

If application faults the `RawNode` after dispatch, the concurrent node enters
terminal fault state.

## Tick behavior

`Tick()` is synchronous and nonblocking.

- the bounded channel uses `BoundedChannelFullMode.Wait`, so `TryWrite`
  reliably returns false rather than silently dropping;
- while running, `Tick` calls only `TryWrite`;
- if full, the tick is dropped and an atomic missed-tick flag is set;
- the owner loop exchanges that flag to zero and emits at most one warning per
  observed nonempty missed-tick batch:

  ```text
  <id-hex> missed one or more ticks because the Node loop was busy.
  ```

- logger execution therefore occurs only on the owner loop under the same
  reentrancy guard as Raft callbacks;
- a logger exception faults the concurrent node and appears through
  `Completion`, but never blocks or throws synchronously from `Tick`;
- after stop or terminal fault, it is a no-op; and
- accepted ticks are processed one at a time through `RawNode.Tick`.

Ticks are not cancellable and carry no completion result.

## Stop and terminal fault

`StopAsync` is idempotent:

1. the first caller atomically requests stop and cancels the loop wait;
2. the loop stops accepting dispatch;
3. pending Ready, proposals, and commands fail with
   `RaftNodeStoppedException`;
4. queued channels are drained and completed;
5. any outstanding Ready becomes invalid with the discarded node; and
6. all callers await the same `Completion`.

The first successful running-to-stop-requested transition is the stop
linearization point. After it, the scheduler claims no new request or Ready
waiter.

An operation already claimed before that point is allowed to finish:

- it receives its own result or original exception;
- a Ready waiter claimed before stop receives that Ready, which becomes
  invalid once shutdown discards the node;
- an expected per-request error such as `ProposalDroppedException` does not
  fault the node; and
- if the claimed operation terminally faults `RawNode`, fault wins over the
  concurrent normal-stop request.

`DisposeAsync` delegates to `StopAsync`.

After normal stop:

- async methods fail with `RaftNodeStoppedException`;
- `Tick` is a no-op; and
- repeated stop calls await the same successful completion.

An exception that faults `RawNode` or escapes the owner loop causes terminal
fault:

- the triggering request receives its original exception;
- all other pending/future requests receive `RaftNodeFaultedException` whose
  inner exception is the terminal cause;
- `Completion` faults with `RaftNodeFaultedException`;
- `Tick` becomes a no-op; and
- `StopAsync` observes the same fault after ensuring termination.

Before terminating, the loop attempts to capture detached `TerminalStatus`
using D22's fault-safe status path. It never masks the original fault if status
capture fails. `TerminalStatus` is null while running and can describe
partially mutated in-memory state; it is diagnostic only and never recovery
metadata.

`Completion` settles only after the currently claimed dispatch returns,
terminal status capture is attempted, every pending request is completed, and
both channels are drained/completed.

## Completion ordering and stop races

The command channel is unbounded so ordinary callers block only on their own
cancellation/result, not queue capacity.

An enqueue racing with stop has two valid outcomes:

- it is rejected immediately as stopped; or
- it enters the channel and is failed by terminal draining.

No request is left incomplete.

`StopAsync` itself has no cancellation token. Once requested, shutdown cannot
be abandoned halfway.

## Callback reentrancy

A static `AsyncLocal<RaftNode?>` marks the owner while it is executing any
`RawNode` or logger call. All public methods, including `Tick` and
`StopAsync`, reject when called for that same node while the marker is present.

This covers direct synchronous callback calls and execution-context-flowing
fire-and-forget work created inside trace/logger callbacks. It prevents:

- synchronously waiting on a command that the occupied owner loop would need
  to process; and
- observational callbacks scheduling same-node consensus mutations.

Request continuations always run asynchronously, and every owner-loop await
uses `ConfigureAwait(false)`; the loop does not capture a caller
`SynchronizationContext`.

## Status and reporting

`GetStatusAsync` is serialized with state-machine inputs and may run while a
Ready is outstanding.

Reporting, transfer, forget-leader, campaign, and read-index methods complete
after their matching `RawNode` method returns. They do not wait for
persistence, network delivery, commitment, or application.

## Internal structure

The production implementation adds:

```text
src/DotnetRaft/IRaftNode.cs
src/DotnetRaft/RaftNode.cs
src/DotnetRaft/RaftNodeStoppedException.cs
src/DotnetRaft/RaftNodeFaultedException.cs
```

`RawNode` adds a general internal `IsFaulted` property used by the owner loop.
The D23 test hook aliases it rather than defining separate state.

No new package dependency is required; `System.Threading.Channels` is part of
the target framework.

## Test plan

### Construction and ownership

- `Start` exposes the exact bootstrap Ready sequence;
- `Restart` exposes existing committed-but-unapplied work;
- invalid peers fail before a node/loop is returned;
- message, proposal, context, and configuration inputs are detached; and
- async storage configuration remains rejected.

### Serialization and proposals

- many concurrent commands execute without `RawNode` reentry;
- a proposal without a leader remains incomplete;
- campaign/Ready/Advance unblocks it after leadership is known;
- cancellation removes a blocked proposal without dispatch;
- proposals expose `ProposalDroppedException`;
- removal disables proposals and re-addition re-enables them; and
- sustained commands do not starve accepted ticks.

### Ready lifecycle

- waiting receives no empty batches;
- only one waiter/outstanding batch is allowed;
- canceled waits do not accept a Ready;
- no next batch appears before `AdvanceAsync`;
- inputs can accumulate while a batch is outstanding;
- advance uses the exact hidden instance;
- duplicate/missing advance fails; and
- committed pagination remains gap-free.

### Step, status, and configuration

- ordinary local messages are ignored;
- reserved storage-thread senders fail until D25;
- unknown responses are ignored before term handling;
- caller message mutation cannot affect dispatch;
- accepted configuration application returns detached state;
- rejected changes are skipped by the host; and
- status is serialized and detached.

### Cancellation and lifecycle

- pre-canceled commands never dispatch;
- cancellation wins before claim;
- claim wins over later cancellation;
- stop unblocks blocked proposals and Ready waits;
- stop is idempotent and rejects later commands;
- stop during an outstanding Ready terminates cleanly;
- tick is nonblocking, bounded, warns on overflow, and is a no-op after stop;
  and
- every enqueue/stop race completes exactly once.

### Faults

- trace callback failure faults the node;
- post-dispatch configuration/advance failure faults the node;
- the triggering request receives the original exception;
- pending/future requests and `Completion` expose terminal fault;
- terminal status remains detached and inspectable; and
- no command executes after terminal fault.

## Acceptance criteria

D24 is complete when:

1. one background loop exclusively owns each `RawNode`;
2. the public async API satisfies the cancellation, Ready, proposal, and
   lifecycle contracts above;
3. D21 persistence/application ordering remains host-controlled and
   unchanged;
4. D23 interaction scenarios continue to pass;
5. focused and full tests pass;
6. formatting verification passes;
7. an independent GPT-5.6 Sol review approves the design and implementation;
   and
8. the completed node is committed and pushed before D25 begins.
