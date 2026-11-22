# ADR 0009: Retry automatic joint exit when transfer times out

- **Status:** Accepted
- **Date:** 2026-10-04

## Context

The leader proposes automatic joint exit through ordinary proposal admission.
Leadership transfer blocks proposals so the target can catch up to a stable
tail. If joint exit is attempted during that window and retried only after
another apply, an idle cluster can stay joint indefinitely. A failure can
also leave the intended final voter set with a majority while the joint
configuration lacks one, preventing the entry that would trigger the retry.

## Decision

On the election timeout, check quorum first. If the node is still leader
and clears an expired transfer, immediately re-run the existing auto-leave
check. Keep its guards: automatic exit is requested, the pending change has
been applied, and the node is still leader. Propose the ordinary empty V2
entry. Its pending index prevents repeated ticks from appending duplicates.

## Trade-offs and evidence

The timeout is the event that removes the blocking condition. Retrying on
every tick wastes work; appending the exit directly bypasses admission and
accounting. The timeout tick may now emit a configuration proposal before
its ordinary heartbeat. The
[leadership-transfer membership tests](../../tests/DotnetRaft.Tests/Core/RaftCoreLeadershipTransferMembershipTests.cs)
check duplicate suppression and the ordering with quorum step-down.
