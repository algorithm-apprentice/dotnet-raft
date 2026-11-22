# D06 Unified Raft Log Design

Historical [port milestone](README.md#completed-reference-port). Follow the
[log walkthrough](../learning/02-log-and-replication.md)
for current matching, slicing, and compaction examples.

[`RaftLog`](../../src/DotnetRaft/Core/RaftLog.cs) makes the stable store and
unstable overlay look like one log. A replacement of an uncommitted suffix
can hide a longer stable tail before the new suffix reaches storage. Reads
must honor the logical range, not merge both physical tails.

A size-limited slice gets one encoded-byte budget across both sources.
Returning the first entry despite the limit ensures progress; applying that
exception once per source would overfill a page at the seam. Application
delivery and acknowledgement have their own cursors and are separate from
both commitment and stable publication. See the
[log tests](../../tests/DotnetRaft.Tests/Core/RaftLogTests.cs).
