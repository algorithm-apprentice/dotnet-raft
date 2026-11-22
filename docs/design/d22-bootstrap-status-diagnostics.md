# D22 Bootstrap, Status, and Diagnostics Design

Historical [port milestone](README.md#completed-reference-port). See the current
[bootstrap and restart](../public-api.md#rawnode)
and [diagnostics](../public-api.md#status-descriptions-and-tracing) contracts.

Bootstrap installs initial membership and emits its ordered add-node entries;
it is not a recovery shortcut. Restart loads boundary, commit, and membership
from storage. `Applied` selects the replay point, but cannot update the
membership that storage returns.

Status and tracing expose detached observations, not mutable core state or
proof of transport delivery. Deterministic descriptions make interaction
output useful as a regression oracle. See the
[bootstrap tests](../../tests/DotnetRaft.Tests/RawNode/RawNodeBootstrapTests.cs)
and [description tests](../../tests/DotnetRaft.Tests/Diagnostics/RaftDescriptionsTests.cs).
