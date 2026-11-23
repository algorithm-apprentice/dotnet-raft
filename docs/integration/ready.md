# Process Ready work

Part of [host integration](README.md). Ready describes work, not effects that
have already happened. Run one consumer and finish the required work before
telling Raft it is complete. Use the protocol you selected at construction.

## Synchronous Ready loop

For `AsyncStorageWrites == false`, use this order:

1. Obtain work with `HasReady()` and `Ready()` on the `RawNode` owner, or with
   `await node.WaitForReadyAsync(...)` on a `RaftNode` consumer.
2. [Publish](storage.md#publishing-a-storage-batch) `Ready.Snapshot`,
   `Ready.Entries`, and `Ready.HardState` as one batch. Force durability when
   `Ready.MustSync` is true. Complete any storage-specific snapshot write
   requirements before releasing effects that depend on it.
3. Send `Ready.Messages` after the required persistence succeeds.
4. Restore any received snapshot to the application, then apply
   `CommittedEntries` in index order. Make each
   [configuration-entry decision](application.md#configuration-entries) at
   its position, not as a second pass after the batch.
5. Pass `ReadStates` to the [read-completion path](application.md#read-index).
   Do not wait on a read barrier that needs later Ready work to finish.
6. Call `Advance(ready)` with the exact `Ready` returned by `RawNode`, or
   `AdvanceAsync()` on `RaftNode`, which retains the exact batch for you.

`Ready.HardState` is a `RaftHardState?`. A non-null value replaces the entire
term, vote, and commit tuple; `null` leaves the persisted state untouched.
The persistence adapter owns its encoding.

New entries or a term or vote change set `MustSync`. A commit-only update or
snapshot presence does not set it by itself. The flag does not waive the
backend's durability requirements for a snapshot. Do not advance a batch
after persistence or application processing fails.

Only one batch can await `Advance`. Ticks, proposals, peer input, and
configuration calls can still run while it is outstanding; work they make
ready will appear in a later batch. An incoming snapshot can update consensus
membership while an earlier batch is still being applied. The earlier
configuration entries still belong to their own log positions and return
that prefix's membership. Do not pause peer input just to apply them. An
older snapshot completion does not overwrite newer consensus membership.

### Early admission is optional

The simple loop applies physically before advancing. After persistence, a
host can instead acknowledge irrevocable ordered admission to an application
pipeline that cannot silently lose or reorder work. Wait if the host cannot
yet decide and apply a configuration entry at its position. Complete any
storage-specific snapshot reconciliation before advancing, such as the
[SQLite pending marker](../../src/DotnetRaft.Sqlite/README.md#persisting-ready-and-local-append-work).
Track [actual application progress](application.md#application-progress)
separately, and stop the host if admitted work fails.

## Asynchronous storage writes

With `AsyncStorageWrites = true`, queue `Ready.AppendWork` and
`Ready.ApplyWork` on separate FIFO workers when present. `Ready.Messages`
contains only peer traffic ready to send. Do not write the mirrored fields
`HardState`, `Entries`, `Snapshot`, or `CommittedEntries` a second time.
Return the work's completion instead of calling `Advance` or `AdvanceAsync`.
Still consume `Ready.ReadStates` for reads.

You can accept more Ready work while earlier requests are in flight.
Each local worker queue must be FIFO and lossless.
For bounded channels, use `BoundedChannelFullMode.Wait`; dropping modes can
report a successful `WriteAsync` for work that will never execute. Treat a
closed or faulted worker queue as a host failure, not as permission to drop
a request. For example, the Ready dispatcher can route messages this way:

```csharp
if (ready.AppendWork is { } append)
{
    await appendQueue.Writer.WriteAsync(append, cancellationToken);
}
if (ready.ApplyWork is { } apply)
{
    await applyQueue.Writer.WriteAsync(apply, cancellationToken);
}
foreach (Message message in ready.Messages)
{
    await transport.SendAsync(message, cancellationToken);
}
```

### Append worker

`StorageAppendWork` carries entries, an optional snapshot, and a
`RaftHardState?`. A null state leaves the stored state unchanged; a present
state includes all three values, including an explicit zero vote.

Atomically publish the [batch](storage.md#publishing-a-storage-batch). If
`MessagesAfterPersist` is nonempty or `Completion` is present, force the
required durability before releasing either. Backends can provide stronger
guarantees, including always forcing a commit-only write.

For a snapshot-bearing request, also restore application state, its physical
cursor, and configuration before responding. Serialize that restoration
against application work using the **same** application-state lock or
transaction barrier. An older apply that holds it finishes before the
restore. If an older apply runs after the restore, skip entries covered by
the snapshot, including their configuration changes, but return its
original receipt to release Raft's outstanding work accounting. The node
does not let an older snapshot completion replace newer consensus membership.

After the write, required durability, and any restoration and storage-specific
acknowledgement succeed, deliver `MessagesAfterPersist` in order, then return
`Completion` if present. Step self-addressed replies locally and send the
other replies to their peers. Do not release replies or the receipt when any
required step fails.

```csharp
foreach (Message response in appendWork.MessagesAfterPersist)
{
    if (response.To == localNodeId)
    {
        await node.StepAsync(response, cancellationToken);
    }
    else
    {
        await transport.SendAsync(response, cancellationToken);
    }
}
if (appendWork.Completion is { } completion)
{
    await node.CompleteStorageWorkAsync(completion, cancellationToken);
}
```

With `RawNode`, schedule local responses and the completion back to its
serialized owner. Call `Step` for self-addressed replies and `CompleteStorageWork`
for the receipt. Keep that path runnable even while a worker queue is full:
an owner waiting for queue space can deadlock if the worker is waiting for
the same owner to process its previous completion. The
[counter](../../examples/DotnetRaft.Counter/README.md#asynchronous-storage-writes)
demonstrates both queues. A
[SQLite integration test](../../tests/DotnetRaft.Sqlite.Tests/SqliteStorageRawNodeTests.cs)
checks an append that releases responses to both destinations.

### Apply worker

`StorageApplyWork` carries committed entries already acknowledged as stable.
Apply them in order under the application-state barrier, including the
[configuration rules](application.md#configuration-entries), then return its
receipt using `CompleteStorageWork` or `CompleteStorageWorkAsync`. Do not
acknowledge a failed apply. Append
and apply each maintain their own order; apply may finish while a separate
commit-only write is still outstanding. Recovery must therefore reconcile
the physical cursor and durable commit, not assume they were one write.

Return each receipt once, in worker order, to the instance that issued it.
Both facades reject another instance's or a previously consumed receipt
without applying it or faulting the node. This is an input rejection, not a
recovery mechanism: a failure while processing a valid receipt still faults
the instance. Use the [shutdown barrier](recovery.md#fault-handling) before
reading a new recovery point.
