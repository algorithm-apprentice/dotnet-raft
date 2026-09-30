# D11 Read-Only Request Tracker Design

- **Status:** Accepted
- **Date:** 2026-09-30
- **DAG node:** D11
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D11 tracks safe `ReadIndex` requests while a leader confirms its authority with
the current voter quorum. It assigns pending reads cumulative sequence
positions, places the latest position in heartbeat context, records each
voter's highest acknowledged position, and releases every request confirmed by
the incoming and outgoing majorities.

D18 integrates this tracker with Raft messages and converts confirmed requests
into local read states or remote `MsgReadIndexResp` messages. Singleton voter
configurations bypass D11 and respond immediately at the current committed
index, matching the pinned core's fast path. D19 uses the option model for
lease-based reads.

## Goals

- Preserve the pinned sequence-based `readOnly` behavior.
- Confirm safe reads through D03 majority or joint-quorum algebra.
- Release confirmed reads in original FIFO order.
- Let one heartbeat acknowledgement cumulatively confirm multiple reads.
- Keep per-voter acknowledgement positions monotonic.
- Retain no confirmed request references.
- Encode heartbeat positions deterministically as little-endian `ulong`.
- Defensively own queued protobuf requests.
- Reject malformed or impossible internal heartbeat positions explicitly.

## Non-goals

- Handling `MsgReadIndex`, heartbeat, or heartbeat-response messages; D18 owns
  core integration.
- Verifying that a current-term entry is committed before accepting reads; D18
  gates non-singleton requests before D11. The D18 singleton fast path precedes
  that gate, matching the pinned implementation.
- Producing public `ReadState` values or `Ready` output; D18 and D21 own those
  surfaces.
- Implementing leader leases; D19 adds the safety prerequisites and behavior.
- Synchronizing concurrent callers. One serialized Raft event loop owns the
  tracker.

## Read option

```csharp
namespace DotnetRaft.Read;

public enum ReadOnlyOption
{
    Safe,
    LeaseBased,
}
```

`Safe` confirms leader authority by communicating with a quorum and is the
default mode. `LeaseBased` relies on the leader lease and requires
`CheckQuorum`; D12 validates that dependency and D19 implements the behavior.

D11 stores the selected option so D12 can recreate the tracker during role
reset.

## Request representation

```csharp
internal sealed record ReadIndexRequest(
    Message Request,
    ulong Index);
```

`Index` is the Raft commit index captured when the request arrived.
`Request` is a clone of the original protobuf message. Mutating the caller's
message, entry collection, context, or entries after enqueueing cannot change
the queued request.

The request's user-supplied context remains in its entry payload for D18. It is
not used as the quorum acknowledgement key.

## Tracker API

```csharp
internal sealed class ReadOnlyTracker : IAckedIndexer
{
    internal ReadOnlyTracker(ReadOnlyOption option);

    internal ReadOnlyOption Option { get; }
    internal int PendingCount { get; }
    internal ulong ConfirmedCount { get; }

    internal void AddRequest(
        ulong commitIndex,
        Message request);

    internal void ReceiveAcknowledgement(
        ulong from,
        ByteString context);

    internal ReadIndexRequest[] Advance(
        JointConfig voters);

    internal ByteString GetHeartbeatContext();
}
```

Construction creates an empty request queue and acknowledgement map.

## Sequence positions

The tracker maintains:

```text
confirmedCount     total requests released since construction
pendingCount       requests currently queued
```

Pending requests occupy logical one-based positions:

```text
confirmedCount + 1
...
confirmedCount + pendingCount
```

The sequence is internal and cumulative across queue drains. It is unrelated
to the user request context or Raft log index.

Adding a request appends it without deduplication. Two requests with identical
user context remain two distinct FIFO requests, matching the pinned
implementation.

Before cloning or enqueueing, `AddRequest` computes the new cumulative
position with checked arithmetic. An unrepresentable position throws
`RaftInvariantException` without changing the queue or counters.

## Heartbeat context

`GetHeartbeatContext` returns:

- `ByteString.Empty` when no reads are pending; or
- exactly eight bytes containing
  `confirmedCount + pendingCount` as a little-endian `ulong`.

That value represents the newest read position the heartbeat can confirm. A
single response to this heartbeat cumulatively acknowledges every earlier
position.

Sequence addition is checked. An unrepresentable position throws
`RaftInvariantException` without changing tracker state.

## Recording acknowledgements

`ReceiveAcknowledgement(from, context)`:

- ignores an empty context;
- requires every non-empty context to contain exactly eight bytes;
- decodes the little-endian acknowledged position; and
- requires the position to be non-zero and no greater than the tracker's
  current cumulative position; and
- stores:

```text
acks[from] = max(acks[from], decodedPosition)
```

Late or duplicated heartbeat responses cannot regress a node's acknowledged
position.

The tracker itself can record acknowledgements from IDs outside the voter
configuration, and D03 ignores them for confirmation. D18 must call
`ReceiveAcknowledgement` only after successfully finding the sender in the
progress map, matching the pinned core's earlier message filter. Therefore a
tracked learner may be recorded, while an unknown or already-removed sender
never reaches D11.

Malformed context throws `RaftInvariantException`. D18 only passes heartbeat
contexts generated by this tracker, so malformed input indicates corrupted or
invalid protocol state rather than a retryable condition. Rejecting an
impossible future position before updating the acknowledgement map prevents a
minority acknowledgement from becoming quorum-valid after later requests are
added.

## Quorum confirmation

The tracker implements `IAckedIndexer` by returning each node's highest
acknowledged read position.

`Advance(voters)` computes:

```text
newConfirmedCount = voters.CommittedIndex(this)
```

D03 therefore applies:

- one majority for a simple configuration; or
- both incoming and outgoing majorities for joint consensus.

If `newConfirmedCount <= confirmedCount`, no request is released.

Otherwise:

```text
releaseCount = newConfirmedCount - confirmedCount
```

The tracker removes and returns exactly that many requests from the queue
front, advances `confirmedCount`, and preserves FIFO order.

A quorum position beyond:

```text
confirmedCount + pendingCount
```

could only come from corrupted tracker state because acknowledgement input is
already range-checked. `Advance` still throws `RaftInvariantException` before
removing requests or changing `confirmedCount` as defense in depth.

Confirmed request references are removed from the queue immediately, avoiding
the historical read-only memory retention issue covered by the pinned Raft
test.

## Ownership

- `AddRequest` clones its input message.
- `Advance` transfers ownership of the removed `ReadIndexRequest` objects to
  the caller in a new array.
- The pending queue retains no returned request.
- Heartbeat contexts are immutable `ByteString` values.
- D03 voter configuration is read but never mutated.

## Invariants

```text
confirmedCount is monotonic
pending positions are contiguous after confirmedCount
acks[id] is monotonic for each ID
queue order equals request arrival order
GetHeartbeatContext position == confirmedCount + pendingCount
Advance never releases a later request before an earlier request
```

An empty acknowledgement does not alter any invariant or acknowledgement
state.

## Test plan

The pinned source has no dedicated `read_only_test.go`; D11 derives unit tests
from `read_only.go`, `TestRaftFreesReadOnlyMem`, safe-read Raft tests, and D03
quorum behavior:

1. construction exposes the selected option and empty state;
2. empty queues produce empty heartbeat context;
3. heartbeat context is exactly eight-byte little-endian cumulative position;
4. queue draining keeps positions cumulative across later requests;
5. queued requests are defensively cloned;
6. identical user contexts remain distinct FIFO requests;
7. empty acknowledgements are ignored;
8. malformed non-eight-byte acknowledgements fail explicitly;
9. zero and future acknowledgement positions fail before changing ack state;
10. a rejected future minority acknowledgement cannot confirm a later request;
11. per-node acknowledgement positions never regress;
12. a simple majority releases only the confirmed FIFO prefix;
13. a later quorum acknowledgement releases all newly confirmed requests;
14. batched requests preserve their distinct captured commit indexes;
15. joint consensus requires both majority positions;
16. learner acknowledgements do not affect voter confirmation;
17. duplicate and stale acknowledgements do not release requests twice;
18. confirmed requests are removed and eligible for collection;
19. an impossible quorum position throws without changing the queue or
    confirmed counter;
20. null request/context/config arguments fail explicitly;
21. D18 reserves integration coverage for the singleton fast path and the
    known-progress sender filter.

## Development sequence

D11 uses strict test-driven development:

1. review and accept this design;
2. add all read-tracker tests before production types;
3. run the focused target and record the expected compile failure;
4. implement the minimum sequence and quorum behavior that passes;
5. refactor while preserving the test matrix;
6. run formatting and the full suite before code review.

## Completion criteria

D11 is complete when:

- this design is reviewed and accepted;
- sequence encoding, acknowledgement, and FIFO quorum release are implemented;
- simple and joint quorum tests pass;
- malformed-state and ownership tests pass;
- formatting, build, and full tests pass;
- code review has no unresolved substantiated findings;
- the reviewed changes are committed before D12 starts.
