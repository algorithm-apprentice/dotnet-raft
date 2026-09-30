using DotnetRaft.Core;
using DotnetRaft.Tracker;

namespace DotnetRaft.Tests.Tracker;

public sealed class ProgressTests
{
    [Fact]
    public void ToStringIncludesStateAndActiveQualifiers()
    {
        var progress = new Progress(
            match: 1,
            next: 2,
            maxInflightMessages: 1,
            maxInflightBytes: 0,
            isLearner: true);
        progress.BecomeSnapshot(123);
        progress.Inflights.Add(123, 1);

        Assert.Equal(
            "StateSnapshot match=1 next=124 learner paused pendingSnap=123 inactive inflight=1[full]",
            progress.ToString());
    }

    [Theory]
    [InlineData((int)ProgressState.Probe, false, false)]
    [InlineData((int)ProgressState.Probe, true, true)]
    [InlineData((int)ProgressState.Replicate, false, false)]
    [InlineData((int)ProgressState.Replicate, true, true)]
    [InlineData((int)ProgressState.Snapshot, false, true)]
    [InlineData((int)ProgressState.Snapshot, true, true)]
    public void IsPausedDependsOnStateAndFlowFlag(
        int stateValue,
        bool appendFlowPaused,
        bool expected)
    {
        var state = (ProgressState)stateValue;
        Progress progress = CreateInState(state);
        progress.AppendFlowPaused = appendFlowPaused;

        Assert.Equal(expected, progress.IsPaused);
    }

    [Fact]
    public void AcknowledgementsAndProbeRejectionsResumeAppendFlow()
    {
        var progress = new Progress(0, 2, 4, 0)
        {
            AppendFlowPaused = true,
        };

        Assert.True(progress.MaybeDecrementTo(1, 1));
        Assert.False(progress.AppendFlowPaused);

        progress.AppendFlowPaused = true;
        Assert.True(progress.MaybeUpdate(2));
        Assert.False(progress.AppendFlowPaused);
    }

    [Fact]
    public void BecomeProbeFromReplicateUsesMatchAndResetsFlowState()
    {
        var progress = new Progress(1, 5, 4, 0);
        progress.BecomeReplicate();
        progress.SentEntries(3, 30);

        progress.BecomeProbe();

        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.Equal(1UL, progress.Match);
        Assert.Equal(2UL, progress.Next);
        Assert.False(progress.AppendFlowPaused);
        Assert.Equal(0, progress.Inflights.Count);
    }

    [Fact]
    public void BecomeProbeFromSnapshotUsesPendingSnapshot()
    {
        var progress = new Progress(1, 5, 4, 0);
        progress.BecomeSnapshot(10);

        progress.BecomeProbe();

        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.Equal(1UL, progress.Match);
        Assert.Equal(11UL, progress.Next);
        Assert.Equal(0UL, progress.PendingSnapshot);
    }

    [Fact]
    public void BecomeReplicateUsesMatchAndResetsInflights()
    {
        var progress = new Progress(1, 5, 1, 0);
        progress.BecomeReplicate();
        progress.SentEntries(1, 10);

        progress.BecomeReplicate();

        Assert.Equal(ProgressState.Replicate, progress.State);
        Assert.Equal(1UL, progress.Match);
        Assert.Equal(2UL, progress.Next);
        Assert.False(progress.AppendFlowPaused);
        Assert.Equal(0, progress.Inflights.Count);
    }

    [Fact]
    public void BecomeSnapshotSetsSnapshotProgressAndResetsInflights()
    {
        var progress = new Progress(1, 5, 1, 0);
        progress.BecomeReplicate();
        progress.SentEntries(1, 10);

        progress.BecomeSnapshot(10);

        Assert.Equal(ProgressState.Snapshot, progress.State);
        Assert.Equal(1UL, progress.Match);
        Assert.Equal(11UL, progress.Next);
        Assert.Equal(10UL, progress.PendingSnapshot);
        Assert.Equal(10UL, progress.LastSentCommit);
        Assert.True(progress.IsPaused);
        Assert.Equal(0, progress.Inflights.Count);
    }

    [Theory]
    [InlineData(2, 3, 5, false)]
    [InlineData(3, 3, 5, false)]
    [InlineData(4, 4, 5, true)]
    [InlineData(5, 5, 6, true)]
    public void MaybeUpdateIgnoresStaleIndexesAndAdvancesNewIndexes(
        ulong acknowledgedIndex,
        ulong expectedMatch,
        ulong expectedNext,
        bool expectedUpdated)
    {
        var progress = new Progress(3, 5, 4, 0);

        bool updated = progress.MaybeUpdate(acknowledgedIndex);

        Assert.Equal(expectedUpdated, updated);
        Assert.Equal(expectedMatch, progress.Match);
        Assert.Equal(expectedNext, progress.Next);
    }

    [Theory]
    [InlineData((int)ProgressState.Replicate, 5, 10, 5, 5, false, 10)]
    [InlineData((int)ProgressState.Replicate, 5, 10, 4, 4, false, 10)]
    [InlineData((int)ProgressState.Replicate, 5, 10, 9, 9, true, 6)]
    [InlineData((int)ProgressState.Probe, 0, 10, 5, 5, false, 10)]
    [InlineData((int)ProgressState.Probe, 0, 10, 9, 9, true, 9)]
    [InlineData((int)ProgressState.Probe, 0, 2, 1, 1, true, 1)]
    [InlineData((int)ProgressState.Probe, 0, 1, 0, 0, true, 1)]
    [InlineData((int)ProgressState.Probe, 0, 10, 9, 2, true, 3)]
    [InlineData((int)ProgressState.Probe, 0, 10, 9, 0, true, 1)]
    public void MaybeDecrementToHandlesStaleAndGenuineRejections(
        int stateValue,
        ulong match,
        ulong next,
        ulong rejectedIndex,
        ulong matchHint,
        bool expectedChanged,
        ulong expectedNext)
    {
        var state = (ProgressState)stateValue;
        var progress = new Progress(match, next, 256, 0);
        if (state == ProgressState.Replicate)
        {
            progress.BecomeReplicate();
            progress.SentEntries(
                checked((int)(next - progress.Next)),
                bytes: 0);
        }

        bool changed = progress.MaybeDecrementTo(
            rejectedIndex,
            matchHint);

        Assert.Equal(expectedChanged, changed);
        Assert.Equal(match, progress.Match);
        Assert.Equal(expectedNext, progress.Next);
    }

    [Fact]
    public void ReplicateSendsAdvanceNextTrackInflightsAndPauseAtLimits()
    {
        var progress = new Progress(0, 1, 2, 150);
        progress.BecomeReplicate();

        progress.SentEntries(1, 100);

        Assert.Equal(2UL, progress.Next);
        Assert.Equal(1, progress.Inflights.Count);
        Assert.Equal(100UL, progress.Inflights.Bytes);
        Assert.False(progress.AppendFlowPaused);

        progress.SentEntries(2, 60);

        Assert.Equal(4UL, progress.Next);
        Assert.Equal(2, progress.Inflights.Count);
        Assert.Equal(160UL, progress.Inflights.Bytes);
        Assert.True(progress.AppendFlowPaused);
    }

    [Fact]
    public void EmptyReplicateSendRepausesAStillFullWindow()
    {
        var progress = new Progress(0, 1, 1, 0);
        progress.BecomeReplicate();
        progress.SentEntries(1, 10);
        progress.AppendFlowPaused = false;

        progress.SentEntries(0, 0);

        Assert.Equal(2UL, progress.Next);
        Assert.Equal(1, progress.Inflights.Count);
        Assert.True(progress.AppendFlowPaused);
    }

    [Fact]
    public void ProbeSendPausesWithoutOptimisticAdvancement()
    {
        var progress = new Progress(1, 5, 4, 0);

        progress.SentEntries(2, 100);

        Assert.Equal(5UL, progress.Next);
        Assert.Equal(0, progress.Inflights.Count);
        Assert.True(progress.AppendFlowPaused);

        progress.AppendFlowPaused = false;
        progress.SentEntries(0, 0);
        Assert.False(progress.AppendFlowPaused);
    }

    [Fact]
    public void SnapshotStateRejectsAppendSendsWithoutMutation()
    {
        var progress = new Progress(1, 2, 4, 0);
        progress.BecomeSnapshot(10);

        Assert.Throws<RaftInvariantException>(
            () => progress.SentEntries(1, 100));

        Assert.Equal(ProgressState.Snapshot, progress.State);
        Assert.Equal(11UL, progress.Next);
        Assert.Equal(10UL, progress.PendingSnapshot);
        Assert.Equal(0, progress.Inflights.Count);
    }

    [Fact]
    public void SentCommitOverwriteCanRenewCommitBumpEligibility()
    {
        var progress = new Progress(3, 5, 4, 0);

        progress.RecordSentCommit(8);
        Assert.False(progress.CanBumpCommit(9));

        progress.RecordSentCommit(3);
        Assert.Equal(3UL, progress.LastSentCommit);
        Assert.True(progress.CanBumpCommit(8));
        Assert.False(progress.CanBumpCommit(3));

        progress.RecordSentCommit(8);
        progress.BecomeProbe();
        Assert.Equal(3UL, progress.LastSentCommit);
    }

    [Theory]
    [InlineData(true, 11, 10)]
    [InlineData(false, 2, 1)]
    public void SnapshotReportsEnterPausedProbeWithCorrectRetryBasis(
        bool succeeded,
        ulong expectedNext,
        ulong expectedSentCommit)
    {
        var progress = new Progress(1, 2, 4, 0);
        progress.BecomeSnapshot(10);

        progress.ReportSnapshot(succeeded);

        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.Equal(expectedNext, progress.Next);
        Assert.Equal(0UL, progress.PendingSnapshot);
        Assert.Equal(expectedSentCommit, progress.LastSentCommit);
        Assert.True(progress.AppendFlowPaused);
    }

    [Fact]
    public void AppendAcknowledgementBelowPendingSnapshotCanResumeReplication()
    {
        var progress = new Progress(1, 2, 4, 0);
        progress.BecomeSnapshot(10);

        Assert.True(progress.MaybeUpdate(5));
        Assert.True(progress.Match + 1 >= 6);

        progress.BecomeProbe();
        progress.BecomeReplicate();

        Assert.Equal(ProgressState.Replicate, progress.State);
        Assert.Equal(5UL, progress.Match);
        Assert.Equal(6UL, progress.Next);
        Assert.Equal(0UL, progress.PendingSnapshot);
    }

    [Fact]
    public void SnapshotRejectionUsesNonReplicateBackoffWithoutLeavingSnapshot()
    {
        var progress = new Progress(1, 2, 4, 0);
        progress.BecomeSnapshot(10);
        progress.AppendFlowPaused = true;

        bool changed = progress.MaybeDecrementTo(10, 2);

        Assert.True(changed);
        Assert.Equal(ProgressState.Snapshot, progress.State);
        Assert.Equal(1UL, progress.Match);
        Assert.Equal(3UL, progress.Next);
        Assert.Equal(10UL, progress.PendingSnapshot);
        Assert.Equal(2UL, progress.LastSentCommit);
        Assert.False(progress.AppendFlowPaused);
        Assert.True(progress.IsPaused);
    }

    [Fact]
    public void MaximumMatchHintUsesBoundedRejectedIndex()
    {
        var progress = new Progress(0, 10, 4, 0);

        Assert.True(progress.MaybeDecrementTo(9, ulong.MaxValue));
        Assert.Equal(9UL, progress.Next);
    }

    [Fact]
    public void FullInflightFailureDoesNotAdvanceProgress()
    {
        var progress = new Progress(0, 1, 1, 0);
        progress.BecomeReplicate();
        progress.SentEntries(1, 10);

        Assert.Throws<RaftInvariantException>(
            () => progress.SentEntries(1, 20));

        Assert.Equal(ProgressState.Replicate, progress.State);
        Assert.Equal(0UL, progress.Match);
        Assert.Equal(2UL, progress.Next);
        Assert.Equal(1, progress.Inflights.Count);
        Assert.Equal(10UL, progress.Inflights.Bytes);
        Assert.True(progress.AppendFlowPaused);
    }

    [Fact]
    public void InvalidInputsAndIndexOverflowFailWithoutMutation()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new Progress(0, 0, 4, 0));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new Progress(2, 2, 4, 0));

        var progress = new Progress(5, 6, 4, 0);
        Assert.Throws<ArgumentOutOfRangeException>(
            () => progress.BecomeSnapshot(0));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => progress.BecomeSnapshot(4));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => progress.SentEntries(-1, 0));
        Assert.Throws<RaftInvariantException>(
            () => progress.ReportSnapshot(succeeded: true));

        Assert.Equal(ProgressState.Probe, progress.State);
        Assert.Equal(5UL, progress.Match);
        Assert.Equal(6UL, progress.Next);
        Assert.Equal(0UL, progress.PendingSnapshot);
        Assert.Equal(0, progress.Inflights.Count);

        var atLimit = new Progress(
            ulong.MaxValue - 1,
            ulong.MaxValue,
            4,
            0);
        atLimit.BecomeReplicate();

        Assert.Throws<RaftInvariantException>(
            () => atLimit.SentEntries(1, 1));
        Assert.Throws<RaftInvariantException>(
            () => atLimit.MaybeUpdate(ulong.MaxValue));
        Assert.Throws<RaftInvariantException>(
            () => atLimit.BecomeSnapshot(ulong.MaxValue));

        Assert.Equal(ProgressState.Replicate, atLimit.State);
        Assert.Equal(ulong.MaxValue - 1, atLimit.Match);
        Assert.Equal(ulong.MaxValue, atLimit.Next);
        Assert.Equal(0UL, atLimit.PendingSnapshot);
        Assert.Equal(0, atLimit.Inflights.Count);
    }

    private static Progress CreateInState(ProgressState state)
    {
        var progress = new Progress(0, 1, 4, 0);
        switch (state)
        {
            case ProgressState.Probe:
                break;
            case ProgressState.Replicate:
                progress.BecomeReplicate();
                break;
            case ProgressState.Snapshot:
                progress.BecomeSnapshot(1);
                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(state),
                    state,
                    "Unknown progress state.");
        }

        return progress;
    }
}
