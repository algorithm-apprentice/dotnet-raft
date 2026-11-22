# Configure, start, and drive a node

Part of [host integration](README.md). Choose the facade and Ready protocol
before wiring startup, ticks, and peer input.

## Configuration

```csharp
var config = new RaftConfig
{
    Id = nodeId,
    ElectionTick = 10,
    HeartbeatTick = 1,
    Storage = storage,
};
```

Node IDs must be nonzero, must not be reserved local worker IDs from
`RaftLocalMessageTargets`, and must not be reused for a different member.
Ticks are logical: `ElectionTick > HeartbeatTick > 0`. The randomized
election timeout is at most `2 * ElectionTick - 1` ticks, which must fit in
an `int`. The host chooses the cadence and calls `Tick`; configure network
and client deadlines separately.

| Option | Meaning |
|---|---|
| `PreVote` | Check whether an election could win before disrupting the cluster by increasing the term. |
| `CheckQuorum` | A leader steps down when it no longer observes recent activity from a quorum. Required for `ReadOnlyOption.LeaseBased`; see [clock assumptions](application.md#read-index). |
| `MaxSizePerMessage` | Bounds encoded entries in a replication batch, not the whole transport frame; the first entry can exceed the budget. |
| `MaxInflightMessages`, `MaxInflightBytes` | Per-follower window. Count must be positive. A zero byte limit is unlimited; a nonzero limit cannot be lower than `MaxSizePerMessage`. |
| `MaxUncommittedEntriesSize` | Proposal payload budget; zero is unlimited. The [replication chapter](../learning/02-log-and-replication.md) distinguishes it from wire and apply limits. |
| `MaxCommittedSizePerReady` | Cumulative encoded bytes outstanding for application in either storage mode. Zero uses `MaxSizePerMessage`; allow the first entry of a nonempty page to exceed the budget so it can make progress. |
| `DisableProposalForwarding` | With this enabled, a follower does not forward proposals to the known leader. |
| `DisableConfChangeValidation` | Bypasses the overlap and joint-state admission policy, not structural validation or legal configuration transitions. Leave it off unless the host enforces that policy. |
| `StepDownOnRemoval` | Make a leader step down when an installed configuration removes or demotes it. |
| `Applied` | Recovery position, not the current commit. Set it from the actual [recovery point](recovery.md#restart). |

## Start a new cluster

Use fresh storage and the same ordered nonempty peer list on every starting
voter. Peer IDs must be unique. `Start` bootstraps the membership; it does not
persist or apply the bootstrap entries on your behalf.

```csharp
RawNode raw = RawNode.Start(config, peers);
// Or, for concurrent callers:
// await using RaftNode node = RaftNode.Start(config, peers);
```

Run the Ready consumer immediately. Persist, accept and apply, then
acknowledge every [bootstrap configuration entry](application.md#configuration-entries)
before serving client traffic, initiating campaigns, or accepting inbound
peer traffic. Local storage work and responses must keep running during this
stage; bootstrap does not need a live quorum. If storage already contains
Raft state, use [restart](recovery.md#restart), not a second `Start`.

## Run the node

### RawNode

The host owns the serialized event loop. Drive `Tick`, `Step`, `Propose`,
and Ready/acknowledgement on that owner; worker threads cannot call into the
node directly. Use the [Ready guide](ready.md) for the selected protocol.

### RaftNode

Run one independent Ready consumer to deliver election messages and make
progress while callers wait. `ProposeAsync` can wait while no leader is known
or after the node has observed its own removal from membership; a learner is
not removed. Use a cancellation token to bound the wait *before* dispatch.

`Tick()` is nonblocking and can drop ticks when its bounded queue is full.
Keep network input, timers, and client requests independent of the Ready
consumer. Supervise the consumer alongside `node.Completion`; a terminal
failure requires [fault recovery](recovery.md#fault-handling).

Only one Ready waiter/consumer is supported. Cancellation prevents dispatch
only while the request is waiting. Once claimed, the operation runs once and
its result wins; cancellation does not roll back a proposal or read already
submitted. An already stopping, stopped, or faulted node reports that state
before honoring an already-canceled token. Stop releases waiters;
`Completion` completes on normal stop or faults after a terminal owner-loop
failure. `TerminalStatus` is diagnostic and may show partial in-memory state.

## Network transport

Only nonlocal messages go to peer transport; in async storage mode, route
local work to its [worker](ready.md#asynchronous-storage-writes). For a peer
message, use `Message.To` and clone or serialize it before sharing across
threads. Do not modify a Ready payload or a worker request before carrying
out the effects it describes: detached values do not change the completion
the node expects.

Authenticate the peer at the connection boundary, limit access to authorized
peers, and validate that `Message.To` is the exact recipient. `Step` and
`StepAsync` do not authenticate senders. For messages sent directly or
forwarded once by the originating node, require the connection identity to
match `Message.From`.

If an old leader forwards a `MsgReadIndex` again after stepping down, `From`
remains the original requester's reply address. `MsgProp` can also retain
its origin. A strict one-hop transport can drop these and let clients retry.
If you support repeated forwarding, explicitly authorize the authenticated
forwarder to carry that origin; do not rewrite a ReadIndex reply address.

`RawNode.Step` rejects unknown-peer responses and local-only message types
on the ordinary network path; `RaftNode.StepAsync` ignores them. Neither
replaces the transport allowlist. Known malformed shapes fail before core
mutation; a structural `ArgumentException` rejects the request without
faulting the node. See the [input boundary](../internals/node.md#validation-and-failure)
for why it is separate from consensus transitions.

After an outbound snapshot transfer, call `ReportSnapshot` or
`ReportSnapshotAsync` with the transport outcome. Both outcomes can move a
peer still in snapshot mode back to paused probe, from which a later
heartbeat response can resume work. Transport success does not prove the
receiver applied the snapshot; omitting the report can keep replication
paused. The [loopback KV example](../../examples/DotnetRaft.KvCluster/README.md)
has a teaching transport, not authentication suitable for deployment.

### Leadership transfer

Use an authorized management path to call `TransferLeader` or
`TransferLeadershipAsync` on the current leader. `MsgTransferLeader.From`
identifies the desired transferee, not an authenticated caller. The original
request can be forwarded with that value intact. If you allow forwarded
requests, authenticate and authorize them on a separate control path. A
leader-local-only policy should reject inbound `MsgTransferLeader` on the
peer listener. See [why transfer uses an election](../learning/04-membership-reads-and-liveness.md#put-availability-features-in-their-place).

## Status, descriptions, and tracing

`RawNode.GetBasicStatus`, `GetStatus`, and `VisitProgress`, and the concurrent
facade's `GetStatusAsync`, report detached observations.
`Applied` is the [acknowledgement cursor](application.md#application-progress),
not a substitute for application progress. `RaftDescriptions` formats
deterministic invariant-culture text. Set `RaftConfig.TraceSink` for detached
structured events. A trace callback runs synchronously, must not re-enter the
same node, and faults the node if it throws. A core send trace reports queuing,
not delivery over the network.
