using Google.Protobuf.Collections;

namespace DotnetRaft.Protocol;

public static class ProtocolDefaults
{
    public static ConfState EnsureConfState(ConfState? state)
    {
        state ??= new ConfState();
        if (!state.HasAutoLeave)
        {
            state.AutoLeave = false;
        }

        return state;
    }

    public static SnapshotMetadata EnsureSnapshotMetadata(SnapshotMetadata? metadata)
    {
        metadata ??= new SnapshotMetadata();
        metadata.ConfState = EnsureConfState(metadata.ConfState);
        if (!metadata.HasIndex)
        {
            metadata.Index = 0;
        }

        if (!metadata.HasTerm)
        {
            metadata.Term = 0;
        }

        return metadata;
    }

    public static Snapshot EnsureSnapshot(Snapshot? snapshot)
    {
        snapshot ??= new Snapshot();
        snapshot.Metadata = EnsureSnapshotMetadata(snapshot.Metadata);
        return snapshot;
    }

    public static bool IsEquivalentTo(this ConfState state, ConfState other)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(other);

        var left = Normalize(state);
        var right = Normalize(other);
        return left.Equals(right);
    }

    private static ConfState Normalize(ConfState state)
    {
        var normalized = state.Clone();

        Sort(normalized.Voters);
        Sort(normalized.Learners);
        Sort(normalized.VotersOutgoing);
        Sort(normalized.LearnersNext);

        if (!normalized.HasAutoLeave)
        {
            normalized.AutoLeave = false;
        }

        return normalized;
    }

    private static void Sort(RepeatedField<ulong> values)
    {
        var sorted = values.Order().ToArray();
        values.Clear();
        values.Add(sorted);
    }
}
