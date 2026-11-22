# D17 Membership Integration Design

Historical [port milestone](README.md#completed-reference-port). See the
[configuration-entry contract](../public-api.md#configuration-entries)
and [membership walkthrough](../learning/04-membership-reads-and-liveness.md).

Proposal admission and configuration installation are different operations.
By default, a leader does not admit a new configuration change while a prior
one is still unapplied.
The host still makes an ordered, deterministic decision for every committed
change, and only an accepted change is installed. Installation can change the
quorums for replication and reads immediately; it is not undone if a later
side effect fails. That is why an application failure after dispatch is
terminal rather than a cue to retry the change on the same instance.

Automatic joint exit is an ordinary configuration proposal to retain its
admission and replication rules. [ADR 0009](../adr/0009-auto-leave-retry-after-transfer-timeout.md)
explains the timeout retry added after the port. See the
[membership application tests](../../tests/DotnetRaft.Tests/Core/RaftCoreMembershipApplicationTests.cs).
