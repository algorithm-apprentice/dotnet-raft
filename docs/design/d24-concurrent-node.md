# D24 Concurrent Node Wrapper Design

Historical [port milestone](README.md#completed-reference-port). Use the
[current RaftNode contract](../public-api.md#raftnode)
and the [owner-loop walkthrough](../learning/05-concurrency-and-testing.md#the-net-owner-loop-contract).

`RaftNode` serializes calls into `RawNode`; it does not implement a separate
consensus state machine or run a host's Ready consumer. A request is cancelable
while waiting. After the owner claims it, the operation result wins, so
cancellation cannot report that an operation was prevented when it ran.

D24 initially supported only synchronous Ready/Advance. [D25](d25-asynchronous-storage-writes.md)
enabled async storage writes without changing command ownership. The
[concurrency verification notes](raftnode-concurrency-hardening.md) explain
how the claim, Ready, stop, and fault boundaries are tested without sleeps.
