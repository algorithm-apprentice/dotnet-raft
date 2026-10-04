# ADR 0009: Retry Auto-Leave After Transfer Timeout

- **Status:** Accepted
- **Implementation:** Complete
- **Date:** 2026-10-04
- **Decision owners:** dotnet-raft maintainers

## Context

Automatic joint-consensus exit is proposed when an applied configuration has
`AutoLeave == true`, the pending configuration index is applied, and the local
node is leader. The proposal uses the ordinary proposal path.

That path rejects every proposal while leadership transfer is pending. The
pinned `etcd-io/raft` behavior catches the rejection and retries only from a
later application acknowledgement. If the transfer times out while the cluster
is otherwise idle, the same leader clears the transfer target but receives no
later application callback. The cluster can therefore remain in joint
configuration indefinitely.

This is more than cosmetic. Joint consensus requires both the outgoing and
incoming quorums. A later failure can make that joint quorum unavailable even
when the intended final configuration would still have a quorum, preventing
the new committed entry that the pinned retry strategy depends on.

The overlap between an applied automatic joint change and a pending transfer
is uncommon, but the impact is loss of membership-change liveness and the fix
is small.

## Decision

Adopt one narrow intentional parity deviation.

1. Leader ticking continues to run the existing quorum check before transfer
   expiration.
2. When the election timeout expires, the node is still leader, and a transfer
   target remains pending, clear the transfer target exactly as before.
3. Immediately invoke the existing auto-leave check after clearing that target.
4. The check retains all existing guards:
   - `AutoLeave == true`;
   - the pending configuration index is applied; and
   - the node is still leader.
5. The retry uses the ordinary empty V2 configuration proposal path. A
   successful proposal advances the pending configuration index, preventing
   duplicate retries.
6. Do not add periodic retries or change transfer replacement, role-reset, or
   membership-installation ordering.

## Rationale

The timeout is the event that removes the only reason the internal proposal
was dropped. Retrying at that exact boundary restores autonomous progress
without introducing a timer, a second membership mechanism, or a special log
append path.

Running the existing check is safe:

- if the quorum check stepped the node down, the leader guard prevents a
  proposal;
- if an exit is already pending, its higher pending index prevents a duplicate;
- if auto-leave is no longer required, the configuration guard prevents work;
- if another proposal precondition fails, the existing typed drop handling
  leaves state unchanged and logs the pending condition.

The pinned and current upstream implementations instead wait for the next
application acknowledgement. This project intentionally retries earlier at
transfer timeout to avoid making joint-exit liveness depend on unrelated client
traffic.

## Consequences

### Positive

- An idle leader exits joint consensus after a failed transfer without waiting
  for another committed entry.
- Existing proposal validation, tracing, replication, and pending-index
  accounting remain authoritative.
- The change is local to the transfer-timeout boundary.

### Tradeoffs

- Message ordering on the timeout tick can include an automatic configuration
  proposal before the normal heartbeat.
- Behavior differs from the pinned reference and must remain listed in the
  parity matrix.

## Rejected alternatives

- **Preserve the pinned next-application retry:** can leave an idle cluster in
  joint consensus indefinitely and can make the required triggering entry
  impossible to commit after a failure.
- **Retry on every leader tick:** creates needless repeated checks and obscures
  the actual unblocking event.
- **Append the exit directly:** bypasses proposal admission, pending-index
  accounting, tracing, and replication behavior.
- **Retry on every transfer cancellation path:** broadens ordering changes
  beyond the autonomous timeout stall addressed here.

## Acceptance criteria

1. A transfer-time auto-leave proposal is still dropped atomically.
2. Transfer timeout clears the target and immediately appends exactly one empty
   V2 exit proposal without a later application acknowledgement.
3. Repeated ticks and repeated application of the current index do not append a
   duplicate exit.
4. Check-quorum step-down does not propose an exit from a non-leader.
5. Existing leadership-transfer, membership, Debug, and Release tests pass.

## Outcome

Implemented by reusing `MaybeAutoLeave` immediately after timeout clears the
transfer target in `TickLeader`.

- The focused leadership-transfer, membership, quorum, and safety set passes
  109 tests.
- Full Debug and Release validation each pass 1,023 core/example tests and 33
  SQLite tests.
- The regression test proves immediate single-proposal retry, duplicate
  suppression, and check-quorum step-down ordering.
