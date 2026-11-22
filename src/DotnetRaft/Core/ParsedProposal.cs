using DotnetRaft.ConfChange;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Core;

// Borrows the owned message's entries for one transition. The parsed changes
// are for admission and diagnostics; the entries remain the wire representation.
internal sealed class ParsedProposal
{
    private readonly IReadOnlyList<Entry> entries;
    private readonly ConfChangeV2?[]? changes;

    private ParsedProposal(IReadOnlyList<Entry> entries, ConfChangeV2?[]? changes)
    {
        this.entries = entries;
        this.changes = changes;
    }

    internal Entry[] CopyEntries() => ProtocolCloning.CloneEntries(entries);

    internal bool HasConfigurations => changes is not null;

    internal bool? WantsLeaveJointAt(int offset) => changes?[offset] is { } change
        ? change.Changes.Count == 0
        : null;

    internal string? DescribeConfigurationAt(int offset) => changes?[offset] is { } change
        ? RaftDescriptions.DescribeConfChange(change)
        : null;

    internal static ParsedProposal FromLocalConfiguration(Entry entry, ConfChangeV2 change) =>
        new([entry], [change]);

    internal static ParsedProposal Parse(IReadOnlyList<Entry> entries, bool fromPeer)
    {
        if (entries.Count == 0)
        {
            const string message = "A proposal must contain at least one entry.";
            throw fromPeer ? NetworkMessageValidation.Invalid(message) : new RaftInvariantException(message);
        }

        ConfChangeV2?[]? changes = null;
        for (int index = 0; index < entries.Count; index++)
        {
            ConfChangeV2? change = ParseEntry(entries[index], fromPeer);
            if (change is not null)
            {
                changes ??= new ConfChangeV2?[entries.Count];
                changes[index] = change;
            }
        }

        return new ParsedProposal(entries, changes);
    }

    internal static void RequireEntriesForSyntheticProposal(Message message)
    {
        if (message.Entries.Count == 0)
        {
            throw new RaftInvariantException("A proposal must contain at least one entry.");
        }
    }

    internal static void CheckReplicatedEntry(Entry entry) =>
        _ = ParseEntry(entry, fromPeer: true, retainChange: false);

    private static ConfChangeV2? ParseEntry(Entry entry, bool fromPeer, bool retainChange = true)
    {
        if (fromPeer && !Enum.IsDefined(entry.Type))
        {
            throw NetworkMessageValidation.Invalid($"Entry type {(int)entry.Type} is unknown.");
        }

        try
        {
            switch (entry.Type)
            {
                case EntryType.EntryConfChange:
                    ProtocolConfChange legacy = ProtocolConfChange.Parser.ParseFrom(entry.Data);
                    if (fromPeer)
                    {
                        ConfigurationChangeValidation.ValidateProposal(legacy, nameof(entry));
                    }

                    return retainChange ? legacy.AsV2() : null;
                case EntryType.EntryConfChangeV2:
                    ConfChangeV2 change = ConfChangeV2.Parser.ParseFrom(entry.Data);
                    if (fromPeer)
                    {
                        ConfigurationChangeValidation.ValidateProposal(change, nameof(entry));
                    }

                    return retainChange ? change : null;
                default:
                    return null;
            }
        }
        catch (InvalidProtocolBufferException exception) when (fromPeer)
        {
            throw new ArgumentException(
                $"Configuration entry payload is malformed: {exception.Message}",
                nameof(entry),
                exception);
        }
    }
}
