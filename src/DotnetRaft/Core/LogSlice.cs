using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal sealed class LogSlice
{
    private readonly Entry[] entries;

    public LogSlice(ulong term, EntryId previous, IEnumerable<Entry>? entries = null)
    {
        Term = term;
        Previous = previous;
        this.entries = entries?.Select(RequireEntry).ToArray() ?? [];
    }

    public ulong Term { get; }

    public EntryId Previous { get; }

    public IReadOnlyList<Entry> Entries => entries;

    public ulong LastIndex => checked(Previous.Index + (ulong)entries.Length);

    public EntryId LastEntryId => entries.Length == 0
        ? Previous
        : EntryId.From(entries[^1]);

    public void Validate()
    {
        var previous = Previous;

        foreach (var entry in entries)
        {
            var current = EntryId.From(entry);
            if (current.Term < previous.Term || current.Index != checked(previous.Index + 1))
            {
                throw new RaftInvariantException(
                    $"Leader term {Term}: entries {previous} and {current} are not consistent.");
            }

            previous = current;
        }

        if (Term < previous.Term)
        {
            throw new RaftInvariantException(
                $"Leader term {Term}: entry {previous} has a newer term.");
        }
    }

    private static Entry RequireEntry(Entry? entry)
    {
        return entry ?? throw new ArgumentException("Log entries cannot contain null values.", nameof(entry));
    }
}
