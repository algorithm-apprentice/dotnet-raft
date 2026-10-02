using System.Collections.ObjectModel;

namespace DotnetRaft;

public sealed class ConfigurationStatus
{
    private readonly ReadOnlyCollection<ulong> _learners;
    private readonly ReadOnlyCollection<ulong> _learnersNext;
    private readonly ReadOnlyCollection<ulong> _voters;
    private readonly ReadOnlyCollection<ulong> _votersOutgoing;

    internal ConfigurationStatus(
        IEnumerable<ulong> voters,
        IEnumerable<ulong> votersOutgoing,
        IEnumerable<ulong> learners,
        IEnumerable<ulong> learnersNext,
        bool autoLeave)
    {
        _voters = CopySorted(voters);
        _votersOutgoing = CopySorted(votersOutgoing);
        _learners = CopySorted(learners);
        _learnersNext = CopySorted(learnersNext);
        AutoLeave = autoLeave;
    }

    public IReadOnlyList<ulong> Voters => _voters;

    public IReadOnlyList<ulong> VotersOutgoing =>
        _votersOutgoing;

    public IReadOnlyList<ulong> Learners => _learners;

    public IReadOnlyList<ulong> LearnersNext =>
        _learnersNext;

    public bool AutoLeave { get; }

    private static ReadOnlyCollection<ulong> CopySorted(
        IEnumerable<ulong> values)
    {
        ArgumentNullException.ThrowIfNull(values);
        return Array.AsReadOnly(
            values.Order().ToArray());
    }
}
