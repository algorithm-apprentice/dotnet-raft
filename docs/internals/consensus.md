# Inside the consensus state machine

Part of the [implementation map](README.md). This page follows the order of a
transition through `RaftCore` and explains which component may change which
state. The [learning path](../learning/README.md) works through concrete logs,
votes, and failure schedules.

## Coordinate transitions in one place

| Owner | State and decisions |
|---|---|
| [`RaftRoleState`](../../src/DotnetRaft/Core/RaftRoleState.cs), [`RaftClock`](../../src/DotnetRaft/Core/RaftClock.cs) | Term, vote, role, leader and transferee; elapsed logical ticks and randomized deadlines. |
| [`RaftOutput`](../../src/DotnetRaft/Core/RaftOutput.cs) | Sender/term normalization and detached immediate and after-append messages. |
| [`RaftLog`](../../src/DotnetRaft/Core/RaftLog.cs), [`UnstableLog`](../../src/DotnetRaft/Core/UnstableLog.cs) | One logical log over stable storage and pending work; commit, applying, and applied positions. |
| [`ProgressTracker`](../../src/DotnetRaft/Tracker/ProgressTracker.cs), [`ConfigurationChanger`](../../src/DotnetRaft/ConfChange/ConfigurationChanger.cs) | Installed voting sets, progress and votes; validate a candidate change before installing it. |
| [`ProposalAdmission`](../../src/DotnetRaft/Core/ProposalAdmission.cs), [`ReadIndexCoordinator`](../../src/DotnetRaft/Core/ReadIndexCoordinator.cs) | Proposal payload and pending configuration index; gated reads, quorum confirmation, and read results. |

[`RaftCore.Step`](../../src/DotnetRaft/Core/RaftCore.cs) handles term changes
and shared messages before calling `StepFollower`, `StepCampaigning`, or
`StepLeader`. It routes by the role *after* term handling: a higher-term
append can first turn a leader into a follower and must then be handled as a
follower append. Candidate and pre-candidate share a route, but only count
the response type for the election they are running.

The files below are parts of the same class. Commit then read release,
reset, and snapshot installation stay visible in the transition that orders
them. See [ADR 0003](../adr/0003-raftcore-structural-boundaries.md).

| To follow… | Start at |
|---|---|
| Term handling, role dispatch, reset, local worker responses | [`RaftCore.cs`](../../src/DotnetRaft/Core/RaftCore.cs), `Step` |
| Logical ticks, voting, becoming leader, transfer | [`RaftCore.Election.cs`](../../src/DotnetRaft/Core/RaftCore.Election.cs), `TickElection` or `Campaign` |
| Proposals, Append, follower progress, and commit | [`RaftCore.Replication.cs`](../../src/DotnetRaft/Core/RaftCore.Replication.cs), `HandleLeaderProposal` and `HandleAppendResponse` |
| Configuration installation, incoming snapshots, application overlap | [`RaftCore.Membership.cs`](../../src/DotnetRaft/Core/RaftCore.Membership.cs), `ApplyConfigurationChange` or `RestoreSnapshot` |
| The current-term read gate and quorum confirmation | [`RaftCore.ReadIndex.cs`](../../src/DotnetRaft/Core/RaftCore.ReadIndex.cs), `HandleLeaderReadIndex` |
| Detached state and trace events | [`RaftCore.Diagnostics.cs`](../../src/DotnetRaft/Core/RaftCore.Diagnostics.cs), `GetBasicStatus` or `Trace` |

## Elect without losing a durable vote

Election logic needs both an up-to-date candidate log and an intersecting
majority. Randomized follower deadlines separate competing campaigns;
pre-vote checks whether the campaign can win without raising the term.
A real vote is only evidence once its term and vote are durable. `Campaign`
queues even the real self-vote as a response, so the local candidate follows
the same acknowledgement path rather than counting its vote immediately.
`BecomeLeader` appends a no-op to establish a current-term commit even
without a client proposal. The [election chapter](../learning/01-safety-and-elections.md)
explains why an older entry cannot be committed just by counting its copies.

## Admit proposals without stalling progress

`HandleLeaderProposal` checks local membership and transfer state before it
assigns indexes. `ProposalAdmission` prepares quota and configuration
accounting without publishing it before a successful append. The budget can
admit one oversized proposal when no payload is currently charged, and does
not block empty control entries. Otherwise a large entry could never be
admitted or an internal no-op could be blocked. Acknowledging application
releases charged payload, so stalled application can hold up admission even
if commitment has advanced. See the
[quota tests](../../tests/DotnetRaft.Tests/Core/RaftCoreUncommittedQuotaTests.cs).

With the default admission checks, a configuration proposal that conflicts
with a pending change or joint-state policy becomes an empty normal entry.
Successful submission is not proof that a configuration change was chosen or
applied. Structural validation is a separate, earlier check; an invalid
payload never becomes a legal transition just because admission is disabled.

## Replicate without treating a prediction as evidence

`RaftLog` hides a stable tail covered by an unstable replacement. `UnstableLog`
distinguishes pending, in-flight, and acknowledged entries: making work
available to a worker is not proof that it reached storage. At the stable
seam, a size-limited slice has **one** byte budget and **one** first-entry
exception, not one per source.

[`Progress`](../../src/DotnetRaft/Tracker/Progress.cs) keeps acknowledged
`Match` and the predicted next probe `Next` distinct. Replicate mode can
advance `Next` optimistically; probe mode waits for evidence; snapshot mode
pauses appends while a missing prefix is being sent. An
[`InflightWindow`](../../src/DotnetRaft/Tracker/InflightWindow.cs) records the
last index and bytes of each batch so an acknowledgement frees the covered
prefix of the window. The byte threshold stops starting more batches after
it is reached, not partway through an individual batch.

`MaybeCommit` combines the quorum's match index with the current-term rule.
A learner may be fully caught up without affecting that result; during
joint consensus the lower of the two majority indexes constrains commit.
`MsgHeartbeat` does not prove a log match, so the commit it advertises to
a peer is bounded by that peer's known `Match`. See the
[divergent-log and heartbeat walkthroughs](../learning/02-log-and-replication.md)
and [flow-control tests](../../tests/DotnetRaft.Tests/Core/RaftCoreFlowControlTests.cs).

## Install a membership or snapshot as a coherent transition

A live membership change enters the log; it takes effect when applied in
order, not when proposed or appended. The changer computes on copies, then
`SwitchToConfiguration` installs the valid result and reevaluates affected
quorums, replication, and leadership transfer. A voter demoted in a joint
change stays an outgoing voter until the outgoing set is removed; it cannot
already be an active learner. `ConfigurationRestore` rebuilds a valid
tracker from `ConfState` using the same change invariants.

`RestoreSnapshot` does not replace a log that already matches the snapshot's
boundary tuple; it can advance commit through that prefix instead. When it
really installs a snapshot, it changes the live consensus membership but
can retain the configuration needed by earlier delivered application work.
`AcknowledgeSnapshot` checks the owned unstable snapshot's exact index
before reasserting configuration. Without that check, an obsolete
completion could install membership from the wrong prefix. See the
[snapshot overlap test](../../tests/DotnetRaft.Tests/RawNode/RawNodeReadyTests.cs)
and the [membership chapter](../learning/04-membership-reads-and-liveness.md).

## Separate a fresh read index from application state

`HandleLeaderReadIndex` gates reads until the leader has committed in its
term. A sole voter cannot have a competing election within its singleton
configuration, but it can still inherit entries whose commit it has not
learned. A current-term commit gathers that prefix before the shortcut.
A removed leader is not the sole voter just because one voter remains;
the shortcut tests local voter membership as well as quorum size.

For safe reads needing quorum confirmation, the
[`ReadOnlyTracker`](../../src/DotnetRaft/Read/ReadOnlyTracker.cs) assigns
monotonic heartbeat positions, separate from log indexes and user contexts.
An acknowledgement of position 8 covers 7 from that peer, not from other
peers. The same quorum calculation can release a range of pending requests
without storing a separate acknowledgement set per request. The node
returns an index, not application data. Read the
[host barrier](../integration/application.md#read-index) and
[single-voter regression](../../tests/DotnetRaft.Tests/RawNode/RawNodeReadIndexTests.cs).

## Preserve progress without inventing authority

`CheckQuorum` examines recent voter activity at the election interval and
yields leadership if the leader cannot see a quorum. A learner cannot
substitute for a voter. With quorum checking enabled, a node that recently
heard from its leader suppresses disruptive higher-term vote requests. A
transfer campaign bypasses that suppression but still needs real votes. In
lease-read mode, `ForgetLeader` does not clear a follower's leader hint:
otherwise a voter could abandon vote protection earlier than the lease reader
assumes.
The [availability walkthrough](../learning/04-membership-reads-and-liveness.md#put-availability-features-in-their-place)
separates these mechanisms from the read safety proof.

Transfer pauses proposals so the target can reach the leader's last index;
`MsgTimeoutNow` starts an election, not an assignment of authority. This can
temporarily block automatic joint exit, which is also a proposal. `TickLeader`
checks quorum first and re-evaluates the exit after clearing a timed-out
transfer. See [ADR 0009](../adr/0009-auto-leave-retry-after-transfer-timeout.md)
for the idle-cluster failure this ordering prevents.
