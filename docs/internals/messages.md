# Messages, log records, and diagnostics

Part of the [implementation map](README.md). Follow the
[transport contract](../integration/node.md#network-transport) for routing
and peer identity. This page explains the state carried by messages and the
distinction between producing a message and making its effect visible.

## Keep term, log position, and membership separate

[`raft.proto`](../../src/DotnetRaft/Protocol/raft.proto) defines the generated
message, log, and snapshot types. `Message.Term` normally identifies the
sender's consensus term, but an election message can refer to a prospective
term and some local messages use zero. `Entry.Term` identifies the term that
created that entry. `LogTerm` and `Index` name a particular log point in an
append or election request, not the sender's commit. `HardState` records
the term, vote, and commit needed after a process restart.

A snapshot keeps the term and membership at a particular application index,
not at the instant the snapshot is sent. The bytes are opaque to consensus.
`ConfState` records incoming/outgoing voters and learners so a restored node
does not reconstruct voting rights from the set of peers it can currently
contact. [Configuration and recovery](../integration/recovery.md#restart)
must agree with what the application restored.

## Route by the kind of work

| Kind | Examples | Why it is distinct |
|---|---|---|
| Peer consensus traffic | vote, append, heartbeat, snapshot and their replies | Carries term, quorum, or log evidence between nodes. Shape validation is separate from the rule that interprets the evidence. |
| Client work that can be forwarded | `MsgProp`, `MsgReadIndex` | The request does not claim leadership in a Raft term. It can be sent on to the current leader; its origin may still be needed for a reply. |
| Local control | campaign, heartbeat tick, reachability/snapshot reports | An input to this node's owner, not something a peer is allowed to inject as a local event. |
| Local storage work | `MsgStorageAppend`, `MsgStorageApply` and their responses | Carries effects and evidence across workers, not across the trust boundary of peer transport. An append request may contain remote replies to release only after its write. |

[`MessageClassifier`](../../src/DotnetRaft/Core/MessageClassifier.cs) and
[`RaftOutput`](../../src/DotnetRaft/Core/RaftOutput.cs) keep these roles
separate. `RaftOutput` fills an absent sender and uses the current term for
ordinary term-bearing output. Vote messages carry their explicit election
term, since a pre-vote can ask about the next term without changing the
sender's term now. Vote and append responses enter the after-append queue.
See the [Ready boundary](node.md#delivery-and-acknowledgement) for how a local
and a remote response finally leave that queue.

## Preserve presence and ownership

An absent field and an explicitly present zero are different. For example,
a term/vote/commit update can contain a zero vote; absence of all three
fields on a local append means no hard-state update. Do not infer presence
from the numeric value. Generated protobuf types handle serialization and
encoded sizes; [protocol tests](../../tests/DotnetRaft.Tests/Protocol/ProtocolModelTests.cs)
check these details without requiring a second wire model.

Protobuf objects are mutable. Clone when an operation needs to retain input
or hand output to another owner. [`ProtocolDefaults`](../../src/DotnetRaft/Protocol/ProtocolDefaults.cs)
normalizes nested snapshot/configuration metadata on an owned value. When
comparing membership, list order and an omitted default flag do not by
themselves describe a different set of voters or learners.

## Observe a transition without driving it

Status captures detached state; a trace event records an internal boundary
such as entry append, commit advancement, or Ready acceptance. A send trace
is evidence that the core queued output, not that a peer received it or a
worker flushed it. [`RaftDescriptions`](../../src/DotnetRaft/Diagnostics/RaftDescriptions.cs)
uses invariant formatting so two runs can be compared without culture or
enumeration noise. See the [diagnostic tests](../../tests/DotnetRaft.Tests/Diagnostics/RaftDescriptionsTests.cs)
and the [host callback rules](../integration/node.md#status-descriptions-and-tracing).
