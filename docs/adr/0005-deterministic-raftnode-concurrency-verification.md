# ADR 0005: Verify concurrency at its ordering boundary

- **Status:** Accepted
- **Date:** 2026-10-04

## Context

An owner loop can accept, cancel, claim, or fail a queued request. A test
that uses a sleep to race those transitions can pass without exercising the
intended order, or fail just because the scheduler was slow.

## Decision

Pause the real owner in an existing synchronous trace callback, then explicitly
coordinate cancellation, stop, or a second request. Keep the timeout outside
the callback as a deadlock guard. Once the owner claims an operation, assert
that the operation's real result wins. Before a claim, assert cancellation
prevents dispatch. Test stop and terminal draining while there is pending
work, not just on an already idle owner.

Inspect whether a mutation test actually instruments the target path.
Classify survivors and timeouts: a timeout inside a harness is not evidence
that an assertion caught a behavior change. Coverage is a map, not a reason
to introduce production-only test switches or exercise impossible branches.

## Consequences

Tests observe the production transition without a new scheduler or public
testing API. Test callbacks must be released in `finally`, and it matters
which trace event indicates a claim. The
[test map](../maintaining/testing.md#exercise-the-boundary-that-changed)
shows the helper and appropriate suites.
