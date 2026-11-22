# D10 Configuration Changes Design

Historical [port milestone](README.md#completed-reference-port). See the
[membership walkthrough](../learning/04-membership-reads-and-liveness.md)
and [configuration-entry contract](../public-api.md#configuration-entries).

[`ConfigurationChanger`](../../src/DotnetRaft/ConfChange/ConfigurationChanger.cs)
builds and validates a candidate without partially installing it. An outgoing
voter becoming a learner in the new configuration cannot become an active
learner while it still votes in the old set. The demotion is staged until
joint consensus exits.

[`ConfigurationRestore`](../../src/DotnetRaft/ConfChange/ConfigurationRestore.cs)
reuses change rules rather than bypassing them to populate the tracker.
[Restore tests](../../tests/DotnetRaft.Tests/ConfChange/ConfigurationRestoreTests.cs)
check round trips and that invalid configurations do not mutate the supplied
tracker.
