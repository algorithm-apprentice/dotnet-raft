# D18 Safe Linearizable Reads Design

Historical [port milestone](README.md#completed-reference-port). Follow the
[current ReadIndex contract](../public-api.md#read-index)
and the [read walkthrough](../learning/04-membership-reads-and-liveness.md).

Read admission separates knowing the committed prefix from confirming that
the leader still has authority. Commitment in the leader's term addresses the
first question; a quorum round for safe reads addresses the second. Neither
establishes that application state is ready to read.

### Single-voter fast path

The original port skipped both checks for a leader that was itself the only
voter. This could miss an inherited commit after the cluster shrank. The
[upstream fix](../parity-matrix.md#intentional-deviations) now makes every
leader wait for a current-term commit. A leader that is still the sole voter
can then answer without a heartbeat round; a retained non-voter cannot use
that shortcut. See the [inherited-write regression](../../tests/DotnetRaft.Tests/RawNode/RawNodeReadIndexTests.cs).
