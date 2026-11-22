using DotnetRaft.Protocol;

namespace DotnetRaft.Tests.Interaction;

// Observe the application boundary, not the leader's view of commitment.
// These tests start at a shared snapshot and do not install another one.
internal sealed class AppliedLogOracle(ulong initialSnapshotIndex)
{
    private readonly Dictionary<ulong, Entry> _entries = [];
    private readonly Dictionary<ulong, ulong> _appliedThrough = [];

    internal ulong AppliedThrough(ulong nodeId)
    {
        return _appliedThrough.GetValueOrDefault(nodeId, initialSnapshotIndex);
    }

    internal void Record(ulong nodeId, Entry entry)
    {
        Assert.Equal(AppliedThrough(nodeId) + 1, entry.Index);
        if (_entries.TryGetValue(entry.Index, out Entry? previouslyApplied))
        {
            Assert.Equal(previouslyApplied, entry);
        }
        else
        {
            _entries.Add(entry.Index, entry);
        }

        _appliedThrough[nodeId] = entry.Index;
    }
}
