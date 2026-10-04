# Protocol Input Hardening

## Scope

This design implements ADR 0008 without changing elections, replication,
membership semantics, `Ready` ordering, leadership removal, automatic joint
exit, or read-index lifetime.

The implementation has three validation layers:

1. transport identity and routing validation owned by the host;
2. host-independent network-message structure validation in `RawNode.Step`;
3. deterministic core invariants retained in `RaftCore`.

Only the second layer is added here.

## Error contract

External structural errors throw `ArgumentException` before core mutation.

`RaftNode` already classifies `ArgumentException` as an expected request
failure, so the individual operation fails while the owner loop remains
usable. The gRPC example maps the same exception to `InvalidArgument`.

`RaftInvariantException`, storage failures, and invalid committed
configuration application remain terminal.

## Network-message validation

Validation runs on the clone owned by `RawNode.Step`, after storage-response
and local-message routing have been distinguished, but before `Activate` and
`RaftCore.Step`.

The validator rejects:

- an absent or unknown `MessageType`;
- vote and pre-vote messages without a nonzero term;
- proposal and read-index requests with a nonzero term;
- proposals with no entries;
- read-index requests without exactly one entry;
- append requests whose entries do not begin at `message.Index + 1`, are not
  contiguous, end at an index without a representable successor, move
  backward in term, or carry a term newer than the leader message;
- processed heartbeats whose commit index exceeds the local last index;
- current-leader successful append acknowledgements beyond the local leader's
  last index;
- current-leader nonempty read-only heartbeat contexts that are not valid for
  the current read tracker;
- snapshots whose index cannot advance or whose applicable `ConfState` cannot
  be restored; and
- proposal entries whose entry type or encoded configuration payload is
  structurally invalid.

The validator does not reject a zero `From`, require `To` to equal the local
node, or authenticate membership. Existing `RawNode` compatibility permits
some zero-origin forwarding behavior, while routing and identity belong to
the host transport.

Validation must not mutate clocks, term, vote, leader identity, log state,
progress, read tracking, or output queues.

State-dependent leader-message checks run only when term and role routing
would process the message. Snapshot configuration and successor validation
also skip a snapshot point that already matches the local log, because that
path advances only the commit index and deliberately preserves the current
configuration.

## Configuration validation

One shared structural validator covers V1 and V2 configuration proposals.

It requires:

- known `ConfChangeTransition` values;
- known `ConfChangeType` values;
- non-null change records; and
- every nonzero member ID to be a remote Raft ID rather than a reserved local
  storage target.

`NodeId == 0` remains a no-op.

Public proposal methods invoke the validator before encoding and dispatch.
Network proposal validation parses configuration entries and invokes the same
validator before the message enters the core.

`ConfigurationChanger` independently rejects reserved IDs. This protects
bootstrap, snapshot restoration, and callers that reach deterministic
configuration application without using a public proposal facade.

The existing application path still validates quorum and joint-consensus
semantics. Structural proposal validation does not attempt to predict future
tracker state or dry-run overlapping changes.

## Concurrent facade and gRPC behavior

For `RaftNode.StepAsync` and configuration proposal operations:

- the returned operation fails with the original `ArgumentException`;
- `Completion` remains live;
- later ticks, proposals, and valid peer messages continue to work.

`RaftTransportService` maps an `ArgumentException` raised by the receiver to
gRPC `InvalidArgument`. `RaftNodeFaultedException` continues to map to
`Unavailable`.

The example's `NetworkMessageValidator` remains responsible for:

- exact local destination;
- configured remote sender;
- nonzero peer IDs;
- local-control message rejection; and
- the loopback-only deployment boundary.

It does not become an authentication mechanism.

## Tests

### RawNode

- every rejected message leaves term, role, leader, log, tracker, and output
  unchanged;
- the same instance processes a later valid message;
- empty proposals, zero-term votes, malformed read-index requests,
  term-bearing requests, inconsistent append slices, oversized heartbeat
  commits, impossible append acknowledgements, malformed read contexts, and
  malformed configuration entries are rejected;
- lower-term and same-term leader-ignored messages retain their existing
  behavior; and
- matching snapshot points do not validate an unused snapshot configuration.

### Configuration

- unknown V1/V2 enum values are rejected before the log changes;
- reserved append/apply worker IDs are rejected by proposal, bootstrap, and
  restored `ConfState` paths;
- ID zero remains a no-op; and
- disabling configuration admission validation does not disable structural
  validation.

### RaftNode and example transport

- an invalid stepped message fails only its operation;
- the node remains usable afterward; and
- the gRPC service reports `InvalidArgument` rather than faulting the hosted
  node.

## Acceptance criteria

1. known malformed network inputs cannot terminally fault a healthy node;
2. internal invariant failures remain terminal;
3. no invalid configuration proposal mutates the leader log;
4. reserved local worker IDs cannot enter any restored or applied
   configuration;
5. targeted and full Debug/Release tests pass; and
6. public API approval remains unchanged.
