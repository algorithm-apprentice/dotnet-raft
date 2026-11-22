# D26 Parity and Release Hardening Design

Historical [port milestone](README.md#completed-reference-port). Use the
[compatibility decision](../adr/0002-release-and-compatibility-contract.md)
and [current CI workflow](../../.github/workflows/ci.yml) for release checks.

D26 made parity and compatibility verifiable rather than dependent on an
informal list of translated features. The
[pinned corpus tests](../../tests/DotnetRaft.Tests/Release/PinnedInteractionCorpusTests.cs)
keep upstream inputs distinct from approved C# outputs, while
[deterministic property tests](../../tests/DotnetRaft.Tests/Release/RandomizedInteractionPropertyTests.cs)
exercise invariants over more schedules. The [parity matrix](../parity-matrix.md)
records deliberate differences from the pin.

[API approvals](../../tests/DotnetRaft.Tests/Release/PublicApiApprovalTests.cs)
and package checks protect the released boundary independently of behavior
tests. [Benchmark runs](../performance.md) provide comparative evidence;
their wall-clock timings are not a CI acceptance gate.
