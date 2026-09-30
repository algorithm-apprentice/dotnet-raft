using System.Globalization;
using System.Text;

using DotnetRaft.Core;

namespace DotnetRaft.Tracker;

internal sealed class Progress
{
    internal Progress(
        ulong match,
        ulong next,
        int maxInflightMessages,
        ulong maxInflightBytes,
        bool isLearner = false,
        bool recentActive = false)
    {
        if (next == 0 || next <= match)
        {
            throw new ArgumentOutOfRangeException(
                nameof(next),
                next,
                $"Next index must be greater than match index {match}.");
        }

        Match = match;
        Next = next;
        State = ProgressState.Probe;
        Inflights = new InflightWindow(
            maxInflightMessages,
            maxInflightBytes);
        IsLearner = isLearner;
        RecentActive = recentActive;
    }

    internal ulong Match { get; private set; }

    internal ulong Next { get; private set; }

    internal ulong LastSentCommit { get; private set; }

    internal ProgressState State { get; private set; }

    internal ulong PendingSnapshot { get; private set; }

    internal bool RecentActive { get; set; }

    internal bool AppendFlowPaused { get; set; }

    internal InflightWindow Inflights { get; private set; }

    internal bool IsLearner { get; set; }

    internal bool IsPaused =>
        State switch
        {
            ProgressState.Probe => AppendFlowPaused,
            ProgressState.Replicate => AppendFlowPaused,
            ProgressState.Snapshot => true,
            _ => throw UnknownState(),
        };

    internal void Reset(ulong match, ulong next)
    {
        if (next == 0 || next <= match)
        {
            throw new ArgumentOutOfRangeException(
                nameof(next),
                next,
                $"Next index must be greater than match index {match}.");
        }

        Match = match;
        Next = next;
        LastSentCommit = 0;
        RecentActive = false;
        ResetState(ProgressState.Probe);
    }

    internal void BecomeProbe()
    {
        ulong matchNext = IncrementIndex(
            Match,
            "computing the next probe index");
        ulong next = State switch
        {
            ProgressState.Probe or ProgressState.Replicate => matchNext,
            ProgressState.Snapshot => Math.Max(
                matchNext,
                IncrementIndex(
                    PendingSnapshot,
                    "computing the post-snapshot probe index")),
            _ => throw UnknownState(),
        };
        ulong sentCommit = Math.Min(LastSentCommit, next - 1);

        ResetState(ProgressState.Probe);
        Next = next;
        LastSentCommit = sentCommit;
    }

    internal void BecomeReplicate()
    {
        ulong next = IncrementIndex(
            Match,
            "computing the replication index");

        ResetState(ProgressState.Replicate);
        Next = next;
    }

    internal void BecomeSnapshot(ulong snapshotIndex)
    {
        if (snapshotIndex == 0 || snapshotIndex < Match)
        {
            throw new ArgumentOutOfRangeException(
                nameof(snapshotIndex),
                snapshotIndex,
                $"Snapshot index must be nonzero and at least match index {Match}.");
        }

        ulong next = IncrementIndex(
            snapshotIndex,
            "computing the post-snapshot index");

        ResetState(ProgressState.Snapshot);
        PendingSnapshot = snapshotIndex;
        Next = next;
        LastSentCommit = snapshotIndex;
    }

    internal void ReportSnapshot(bool succeeded)
    {
        if (State != ProgressState.Snapshot)
        {
            throw new RaftInvariantException(
                $"Cannot report a snapshot while progress is in {GetStateName(State)}.");
        }

        if (succeeded)
        {
            BecomeProbe();
            AppendFlowPaused = true;
            return;
        }

        ulong next = IncrementIndex(
            Match,
            "computing the failed-snapshot retry index");
        ulong sentCommit = Math.Min(LastSentCommit, next - 1);

        ResetState(ProgressState.Probe);
        Next = next;
        LastSentCommit = sentCommit;
        AppendFlowPaused = true;
    }

    internal void SentEntries(int entryCount, ulong bytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(entryCount);

        switch (State)
        {
            case ProgressState.Replicate:
                if (entryCount > 0)
                {
                    ulong count = (ulong)entryCount;
                    if (count > ulong.MaxValue - Next)
                    {
                        throw new RaftInvariantException(
                            $"Sending {entryCount} entries from index {Next} would overflow progress.");
                    }

                    ulong next = Next + count;
                    Inflights.Add(next - 1, bytes);
                    Next = next;
                }

                AppendFlowPaused = Inflights.IsFull;
                break;
            case ProgressState.Probe:
                if (entryCount > 0)
                {
                    AppendFlowPaused = true;
                }

                break;
            case ProgressState.Snapshot:
                throw new RaftInvariantException(
                    "Cannot send append entries while a snapshot is pending.");
            default:
                throw UnknownState();
        }
    }

    internal bool CanBumpCommit(ulong index)
    {
        ulong previousIndex = PreviousIndex(
            Next,
            "checking commit-send progress");
        return index > LastSentCommit
            && LastSentCommit < previousIndex;
    }

    internal void RecordSentCommit(ulong commit)
    {
        LastSentCommit = commit;
    }

    internal Progress Clone()
    {
        var clone = new Progress(
            Match,
            Next,
            Inflights.Capacity,
            Inflights.MaxBytes,
            IsLearner,
            RecentActive)
        {
            LastSentCommit = LastSentCommit,
            State = State,
            PendingSnapshot = PendingSnapshot,
            AppendFlowPaused = AppendFlowPaused,
            Inflights = Inflights.Clone(),
        };

        return clone;
    }

    internal bool MaybeUpdate(ulong acknowledgedIndex)
    {
        if (acknowledgedIndex <= Match)
        {
            return false;
        }

        ulong acknowledgedNext = IncrementIndex(
            acknowledgedIndex,
            "advancing acknowledged progress");

        Match = acknowledgedIndex;
        Next = Math.Max(Next, acknowledgedNext);
        AppendFlowPaused = false;
        return true;
    }

    internal bool MaybeDecrementTo(
        ulong rejectedIndex,
        ulong matchHint)
    {
        if (State == ProgressState.Replicate)
        {
            if (rejectedIndex <= Match)
            {
                return false;
            }

            ulong replicateNext = IncrementIndex(
                Match,
                "resetting rejected replication progress");
            Next = replicateNext;
            LastSentCommit = Math.Min(
                LastSentCommit,
                replicateNext - 1);
            return true;
        }

        if (State is not ProgressState.Probe
            and not ProgressState.Snapshot)
        {
            throw UnknownState();
        }

        ulong previousIndex = PreviousIndex(
            Next,
            "matching a rejected probe");
        if (previousIndex != rejectedIndex)
        {
            return false;
        }

        ulong matchNext = IncrementIndex(
            Match,
            "bounding rejected progress by the matched index");
        ulong hintedNext = matchHint >= rejectedIndex
            ? rejectedIndex
            : matchHint + 1;
        ulong rejectedNext = Math.Max(hintedNext, matchNext);

        Next = rejectedNext;
        LastSentCommit = Math.Min(
            LastSentCommit,
            rejectedNext - 1);
        AppendFlowPaused = false;
        return true;
    }

    public override string ToString()
    {
        var builder = new StringBuilder();
        builder.Append(GetStateName(State))
            .Append(" match=")
            .Append(Match.ToString(CultureInfo.InvariantCulture))
            .Append(" next=")
            .Append(Next.ToString(CultureInfo.InvariantCulture));

        if (IsLearner)
        {
            builder.Append(" learner");
        }

        if (IsPaused)
        {
            builder.Append(" paused");
        }

        if (PendingSnapshot > 0)
        {
            builder.Append(" pendingSnap=")
                .Append(PendingSnapshot.ToString(CultureInfo.InvariantCulture));
        }

        if (!RecentActive)
        {
            builder.Append(" inactive");
        }

        if (Inflights.Count > 0)
        {
            builder.Append(" inflight=")
                .Append(Inflights.Count.ToString(CultureInfo.InvariantCulture));
            if (Inflights.IsFull)
            {
                builder.Append("[full]");
            }
        }

        return builder.ToString();
    }

    private void ResetState(ProgressState state)
    {
        AppendFlowPaused = false;
        PendingSnapshot = 0;
        State = state;
        Inflights.Reset();
    }

    private static ulong IncrementIndex(
        ulong index,
        string operation)
    {
        if (index == ulong.MaxValue)
        {
            throw new RaftInvariantException(
                $"Index overflow while {operation}.");
        }

        return index + 1;
    }

    private static ulong PreviousIndex(
        ulong index,
        string operation)
    {
        if (index == 0)
        {
            throw new RaftInvariantException(
                $"Index underflow while {operation}.");
        }

        return index - 1;
    }

    private static string GetStateName(ProgressState state)
    {
        return state switch
        {
            ProgressState.Probe => "StateProbe",
            ProgressState.Replicate => "StateReplicate",
            ProgressState.Snapshot => "StateSnapshot",
            _ => throw UnknownState(state),
        };
    }

    private RaftInvariantException UnknownState()
    {
        return UnknownState(State);
    }

    private static RaftInvariantException UnknownState(
        ProgressState state)
    {
        return new RaftInvariantException(
            $"Unknown progress state {(int)state}.");
    }
}
