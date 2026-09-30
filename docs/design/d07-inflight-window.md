# D07 Inflight Append Window Design

- **Status:** Accepted
- **Date:** 2026-09-30
- **DAG node:** D07
- **Reference:** `etcd-io/raft` commit
  `1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`

## Purpose

D07 implements the sliding window that limits append messages sent to one
follower but not yet acknowledged. Each tracked item records:

- the highest log index contained in one `MsgApp`; and
- the total payload bytes represented by that message.

D08 uses the window in replicate state to prevent an active follower from
receiving an unbounded number or byte volume of append messages.

## Goals

- Preserve the pinned `tracker.Inflights` ring-buffer behavior.
- Enforce both message-count and soft byte limits.
- Release all messages acknowledged through an inclusive log index.
- Grow storage lazily instead of allocating the configured maximum eagerly.
- Support independent cloning for status snapshots.
- Keep every operation synchronous and allocation-free after growth.

## Non-goals

- Deciding when a follower is paused; D08 combines this window with progress
  state.
- Tracking heartbeat messages or snapshots.
- Synchronizing concurrent callers. One serialized Raft event loop owns each
  window.
- Dynamically changing configured limits.

## Type and API

```csharp
namespace DotnetRaft.Tracker;

internal sealed class InflightWindow
{
    internal InflightWindow(
        int capacity,
        ulong maxBytes);

    internal int Count { get; }

    internal ulong Bytes { get; }

    internal int Capacity { get; }

    internal ulong MaxBytes { get; }

    internal bool IsFull { get; }

    internal void Add(
        ulong lastIndex,
        ulong bytes);

    internal void FreeThrough(ulong acknowledgedIndex);

    internal void Reset();

    internal InflightWindow Clone();
}
```

The constructor rejects negative capacity. Capacity zero is valid and creates
an always-full window.

`maxBytes == 0` disables the byte limit, matching the reference constructor.
D12 may normalize configuration-level zero values differently before creating
the tracker.

## Representation

```text
start       physical slot containing the oldest inflight
count       number of active inflights
bytes       total active bytes
capacity    maximum active message count
maxBytes    soft active-byte limit, or zero for unlimited
buffer      lazily grown ring array
```

Each ring element contains:

```text
lastIndex   highest entry index in the append message
bytes       payload bytes in the append message
```

The active logical sequence occupies `count` slots beginning at `start`,
wrapping at `capacity`.

## Fullness

The window is full when either:

```text
count == capacity
```

or:

```text
maxBytes != 0 && bytes >= maxBytes
```

The byte limit is soft. `Add` is allowed whenever the current window is not
full, even when the new message raises `bytes` from below `maxBytes` to equal or
above it. No additional message may be added until acknowledgements reduce the
total below the threshold.

## Adding

`Add`:

1. throws `RaftInvariantException` when `IsFull` is already true;
2. requires a strictly increasing index relative to the newest retained
   inflight;
3. computes the next ring slot from `start + count` using a `long`
   intermediate before wrapping and casting;
4. grows the backing array on demand;
5. writes the inflight and increments count and bytes.

Byte addition uses checked arithmetic and leaves state unchanged on overflow.

The backing array grows by doubling:

- zero length grows to one;
- growth uses `(int)Math.Min(capacity, (long)buffer.Length * 2)` and never
  exceeds `capacity`;
- existing physical slots retain their positions, which is sufficient because
  growth can occur only before the ring has wrapped into an unallocated slot.

Every physical-slot calculation, including lookup of the newest retained
inflight for monotonicity checks, uses `long` intermediates so valid
`int`-sized capacities cannot overflow.

## Acknowledgement

`FreeThrough(acknowledgedIndex)` removes every active inflight whose
`lastIndex <= acknowledgedIndex`.

- An empty window is unchanged.
- An acknowledgement older than the oldest retained index is ignored.
- The scan advances through the ring and sums released bytes.
- `count`, `bytes`, and `start` are updated once after the scan.
- When the window becomes empty, `start` resets to zero to avoid unnecessary
  growth on the next use.

Acknowledgements may skip multiple append messages because one follower
response can cumulatively confirm all earlier indexes.

## Reset

`Reset` clears logical state:

```text
start = 0
count = 0
bytes = 0
```

The backing array remains allocated for reuse. Old slots are outside the active
range and are overwritten by later additions.

## Clone

`Clone` copies configuration and logical counters and allocates an independent
copy of the backing array. Mutating or resetting either instance cannot affect
the other.

## Invariants

```text
0 <= count <= capacity
0 <= start < capacity, unless capacity == 0 where start == 0
buffer.Length <= capacity
count == 0 implies start == 0
active lastIndex values are strictly increasing in logical order
bytes equals the sum of active element bytes
```

All invariant violations throw `RaftInvariantException`.

## Test plan

Tests port the complete `tracker/inflights_test.go` behavior:

1. adding without rotation;
2. adding after ring rotation;
3. inclusive freeing at the oldest, middle, and newest indexes;
4. freeing across the physical end of the ring;
5. count-based fullness;
6. soft byte-limit fullness for slight, exact, and larger overflow;
7. zero-capacity always-full behavior;
8. reset without byte-accounting leakage.

Additional C# tests verify:

- negative capacity rejection;
- non-monotonic index rejection without mutation;
- byte-sum overflow rejection without mutation;
- clone independence;
- backing storage grows lazily and never exceeds capacity.

## Development sequence

D07 uses strict test-driven development:

1. add all window tests before production code;
2. run the focused target and record the expected red compile state;
3. implement the smallest ring-buffer behavior that passes;
4. refactor while preserving the test matrix;
5. run formatting and the full suite before code review.

## Completion criteria

D07 is complete when:

- this design is reviewed and accepted;
- the inflight window is implemented;
- all translated and additional tests pass;
- formatting, build, and full tests pass;
- code review has no unresolved substantiated findings;
- the reviewed changes are committed before D08 starts.
