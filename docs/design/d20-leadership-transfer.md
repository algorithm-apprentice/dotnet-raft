# D20 Leadership Transfer Design

Historical [port milestone](README.md#completed-reference-port). Use the
[leadership transfer contract](../public-api.md#leadership-transfer)
for caller authorization and routing.

A leader stops admitting new proposals during transfer to let an eligible
voter catch up. When it is caught up, a `TimeoutNow` triggers an election
instead of assigning a new leader directly. Errors, timeout, and intervening
term changes remain ordinary Raft transitions.

A transfer can temporarily block the automatic proposal to exit joint
consensus. [ADR 0009](../adr/0009-auto-leave-retry-after-transfer-timeout.md)
explains why timeout retries that proposal immediately, without depending on
another application callback. See the
[leadership transfer tests](../../tests/DotnetRaft.Tests/Core/RaftCoreLeadershipTransferTests.cs)
and [membership-transfer tests](../../tests/DotnetRaft.Tests/Core/RaftCoreLeadershipTransferMembershipTests.cs).
