using System.Globalization;
using System.Text;

using DotnetRaft.Core;
using DotnetRaft.Protocol;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Diagnostics;

public delegate string EntryFormatter(
    ReadOnlySpan<byte> data);

public static class RaftDescriptions
{
    public static string DescribeHardState(
        HardState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var builder = new StringBuilder();
        builder.Append("Term:");
        AppendDecimal(builder, state.Term);
        if (state.Vote != 0)
        {
            builder.Append(" Vote:");
            AppendDecimal(builder, state.Vote);
        }

        builder.Append(" Commit:");
        AppendDecimal(builder, state.Commit);
        return builder.ToString();
    }

    public static string DescribeSoftState(
        SoftState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        return string.Create(
            CultureInfo.InvariantCulture,
            $"Lead:{state.LeaderId} State:{FormatPublicEnum(state.Role)}");
    }

    public static string DescribeConfState(
        ConfState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var builder = new StringBuilder();
        builder.Append("Voters:");
        AppendIds(builder, state.Voters);
        builder.Append(" VotersOutgoing:");
        AppendIds(builder, state.VotersOutgoing);
        builder.Append(" Learners:");
        AppendIds(builder, state.Learners);
        builder.Append(" LearnersNext:");
        AppendIds(builder, state.LearnersNext);
        builder.Append(" AutoLeave:");
        builder.Append(state.AutoLeave ? "true" : "false");
        return builder.ToString();
    }

    public static string DescribeSnapshot(
        Snapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        Snapshot owned = ProtocolDefaults.EnsureSnapshot(
            snapshot.Clone());
        var builder = new StringBuilder();
        builder.Append("Index:");
        AppendDecimal(builder, owned.Metadata.Index);
        builder.Append(" Term:");
        AppendDecimal(builder, owned.Metadata.Term);
        builder.Append(" ConfState:");
        builder.Append(
            DescribeConfState(
                owned.Metadata.ConfState));
        return builder.ToString();
    }

    public static string DescribeReady(
        Ready ready,
        EntryFormatter? formatter = null)
    {
        ArgumentNullException.ThrowIfNull(ready);
        var body = new StringBuilder();
        if (ready.SoftState is not null)
        {
            body.Append(
                DescribeSoftState(ready.SoftState));
            body.Append('\n');
        }

        if (ready.HardState is not null)
        {
            body.Append("HardState ");
            body.Append(
                DescribeHardState(ready.HardState));
            body.Append('\n');
        }

        if (ready.ReadStates.Count > 0)
        {
            body.Append("ReadStates:[");
            for (var index = 0;
                 index < ready.ReadStates.Count;
                 index++)
            {
                if (index > 0)
                {
                    body.Append(' ');
                }

                ReadState state = ready.ReadStates[index];
                AppendDecimal(body, state.Index);
                body.Append('/');
                body.Append(
                    QuoteBytes(
                        state.RequestContext.Span));
            }

            body.Append("]\n");
        }

        AppendEntrySection(
            body,
            "Entries",
            ready.Entries,
            formatter);
        if (ready.Snapshot is not null
            && IsNonempty(ready.Snapshot))
        {
            body.Append("Snapshot ");
            body.Append(
                DescribeSnapshot(ready.Snapshot));
            body.Append('\n');
        }

        AppendEntrySection(
            body,
            "CommittedEntries",
            ready.CommittedEntries,
            formatter);
        if (ready.Messages.Count > 0)
        {
            body.Append("Messages:\n");
            foreach (Message message in ready.Messages)
            {
                body.Append(
                    DescribeMessage(
                        message,
                        formatter));
                body.Append('\n');
            }
        }

        if (body.Length == 0)
        {
            return "<empty Ready>";
        }

        var result = new StringBuilder();
        result.Append("Ready MustSync=");
        result.Append(ready.MustSync ? "true" : "false");
        result.Append(":\n");
        result.Append(body);
        return result.ToString();
    }

    public static string DescribeMessage(
        Message message,
        EntryFormatter? formatter = null)
    {
        ArgumentNullException.ThrowIfNull(message);
        return DescribeMessage(
            string.Empty,
            message,
            formatter);
    }

    public static string DescribeEntry(
        Entry entry,
        EntryFormatter? formatter = null)
    {
        ArgumentNullException.ThrowIfNull(entry);
        string? formatted = entry.Type switch
        {
            EntryType.EntryNormal =>
                formatter is null
                    ? QuoteBytes(entry.Data.Span)
                    : formatter(entry.Data.Span),
            EntryType.EntryConfChange =>
                DescribeConfigurationPayload(
                    entry.Data,
                    v2: false),
            EntryType.EntryConfChangeV2 =>
                DescribeConfigurationPayload(
                    entry.Data,
                    v2: true),
            _ => null,
        };

        var builder = new StringBuilder();
        AppendDecimal(builder, entry.Term);
        builder.Append('/');
        AppendDecimal(builder, entry.Index);
        builder.Append(' ');
        builder.Append(FormatEntryType(entry.Type));
        if (!string.IsNullOrEmpty(formatted))
        {
            builder.Append(' ');
            builder.Append(formatted);
        }

        return builder.ToString();
    }

    public static string DescribeEntries(
        IEnumerable<Entry> entries,
        EntryFormatter? formatter = null)
    {
        ArgumentNullException.ThrowIfNull(entries);
        var builder = new StringBuilder();
        foreach (Entry entry in entries)
        {
            if (entry is null)
            {
                throw new ArgumentException(
                    "Entries cannot contain null values.",
                    nameof(entries));
            }

            builder.Append(
                DescribeEntry(entry, formatter));
            builder.Append('\n');
        }

        return builder.ToString();
    }

    public static string DescribeConfChange(
        ProtocolConfChange change)
    {
        ArgumentNullException.ThrowIfNull(change);
        var converted = new ConfChangeV2
        {
            Context = change.Context,
        };
        converted.Changes.Add(
            new ConfChangeSingle
            {
                Type = change.Type,
                NodeId = change.NodeId,
            });
        return DescribeConfChange(converted);
    }

    public static string DescribeConfChange(
        ConfChangeV2 change)
    {
        ArgumentNullException.ThrowIfNull(change);
        var builder = new StringBuilder();
        builder.Append("transition:");
        builder.Append(
            FormatTransition(change.Transition));
        foreach (ConfChangeSingle single in
                 change.Changes)
        {
            builder.Append(" changes:{type:");
            builder.Append(
                FormatConfChangeType(single.Type));
            builder.Append(" node_id:");
            AppendDecimal(builder, single.NodeId);
            builder.Append('}');
        }

        if (change.Context.Length > 0)
        {
            builder.Append(" context:");
            builder.Append(
                QuoteBytes(change.Context.Span));
        }

        return builder.ToString();
    }

    private static string DescribeMessage(
        string indent,
        Message message,
        EntryFormatter? formatter)
    {
        var builder = new StringBuilder();
        builder.Append(indent);
        builder.Append(DescribeTarget(message.From));
        builder.Append("->");
        builder.Append(DescribeTarget(message.To));
        builder.Append(' ');
        builder.Append(FormatMessageType(message.Type));
        builder.Append(" Term:");
        AppendDecimal(builder, message.Term);
        builder.Append(" Log:");
        AppendDecimal(builder, message.LogTerm);
        builder.Append('/');
        AppendDecimal(builder, message.Index);

        if (message.Reject)
        {
            builder.Append(" Rejected (Hint: ");
            AppendDecimal(builder, message.RejectHint);
            builder.Append(')');
        }

        if (message.Commit != 0)
        {
            builder.Append(" Commit:");
            AppendDecimal(builder, message.Commit);
        }

        if (message.Vote != 0)
        {
            builder.Append(" Vote:");
            AppendDecimal(builder, message.Vote);
        }

        if (message.Entries.Count == 1)
        {
            builder.Append(" Entries:[");
            builder.Append(
                DescribeEntry(
                    message.Entries[0],
                    formatter));
            builder.Append(']');
        }
        else if (message.Entries.Count > 1)
        {
            builder.Append(" Entries:[");
            foreach (Entry entry in message.Entries)
            {
                builder.Append('\n');
                builder.Append(indent);
                builder.Append("  ");
                builder.Append(
                    DescribeEntry(entry, formatter));
            }

            builder.Append('\n');
            builder.Append(indent);
            builder.Append(']');
        }

        if (message.Snapshot is not null
            && IsNonempty(message.Snapshot))
        {
            builder.Append('\n');
            builder.Append(indent);
            builder.Append("  Snapshot: ");
            builder.Append(
                DescribeSnapshot(message.Snapshot));
        }

        if (message.Responses.Count > 0)
        {
            builder.Append(" Responses:[");
            foreach (Message response in
                     message.Responses)
            {
                builder.Append('\n');
                builder.Append(
                    DescribeMessage(
                        indent + "  ",
                        response,
                        formatter));
            }

            builder.Append('\n');
            builder.Append(indent);
            builder.Append(']');
        }

        return builder.ToString();
    }

    private static string DescribeConfigurationPayload(
        ByteString data,
        bool v2)
    {
        try
        {
            ConfChangeV2 change = v2
                ? ConfChangeV2.Parser.ParseFrom(data)
                : Convert(
                    ProtocolConfChange.Parser.ParseFrom(data));
            return CompactChanges(change.Changes);
        }
        catch (InvalidProtocolBufferException)
        {
            string type = v2
                ? "ConfChangeV2"
                : "ConfChange";
            return string.Create(
                CultureInfo.InvariantCulture,
                $"invalid {type} payload:{ToLowerHex(data.Span)}");
        }
    }

    private static ConfChangeV2 Convert(
        ProtocolConfChange change)
    {
        var converted = new ConfChangeV2
        {
            Context = change.Context,
        };
        converted.Changes.Add(
            new ConfChangeSingle
            {
                Type = change.Type,
                NodeId = change.NodeId,
            });
        return converted;
    }

    private static string CompactChanges(
        IEnumerable<ConfChangeSingle> changes)
    {
        var builder = new StringBuilder();
        foreach (ConfChangeSingle change in changes)
        {
            if (builder.Length > 0)
            {
                builder.Append(' ');
            }

            builder.Append(change.Type switch
            {
                ConfChangeType.ConfChangeAddNode => 'v',
                ConfChangeType.ConfChangeAddLearnerNode => 'l',
                ConfChangeType.ConfChangeRemoveNode => 'r',
                ConfChangeType.ConfChangeUpdateNode => 'u',
                _ => "unknown",
            });
            AppendDecimal(builder, change.NodeId);
        }

        return builder.ToString();
    }

    private static void AppendEntrySection(
        StringBuilder builder,
        string label,
        IReadOnlyList<Entry> entries,
        EntryFormatter? formatter)
    {
        if (entries.Count == 0)
        {
            return;
        }

        builder.Append(label);
        builder.Append(":\n");
        builder.Append(
            DescribeEntries(entries, formatter));
    }

    private static void AppendIds(
        StringBuilder builder,
        IEnumerable<ulong> ids)
    {
        builder.Append('[');
        var first = true;
        foreach (ulong id in ids)
        {
            if (!first)
            {
                builder.Append(' ');
            }

            first = false;
            AppendDecimal(builder, id);
        }

        builder.Append(']');
    }

    private static string QuoteBytes(
        ReadOnlySpan<byte> data)
    {
        var builder = new StringBuilder();
        builder.Append('"');
        foreach (byte value in data)
        {
            switch (value)
            {
                case (byte)'"':
                    builder.Append("\\\"");
                    break;
                case (byte)'\\':
                    builder.Append("\\\\");
                    break;
                case (byte)'\n':
                    builder.Append("\\n");
                    break;
                case (byte)'\r':
                    builder.Append("\\r");
                    break;
                case (byte)'\t':
                    builder.Append("\\t");
                    break;
                case >= 0x20 and <= 0x7e:
                    builder.Append((char)value);
                    break;
                default:
                    builder.Append("\\x");
                    builder.Append(
                        value.ToString(
                            "x2",
                            CultureInfo.InvariantCulture));
                    break;
            }
        }

        builder.Append('"');
        return builder.ToString();
    }

    private static bool IsNonempty(Snapshot snapshot)
    {
        return snapshot.Metadata?.Index > 0;
    }

    private static string ToLowerHex(
        ReadOnlySpan<byte> data)
    {
        return System.Convert.ToHexString(data)
            .ToLowerInvariant();
    }

    private static string DescribeTarget(ulong id)
    {
        return id switch
        {
            RaftMessageTargets.None => "None",
            RaftMessageTargets.LocalAppendThread =>
                "AppendThread",
            RaftMessageTargets.LocalApplyThread =>
                "ApplyThread",
            _ => id.ToString(
                "x",
                CultureInfo.InvariantCulture),
        };
    }

    private static string FormatEntryType(
        EntryType value)
    {
        return Enum.IsDefined(value)
            ? value.ToString()
            : ((int)value).ToString(
                CultureInfo.InvariantCulture);
    }

    private static string FormatMessageType(
        MessageType value)
    {
        return Enum.IsDefined(value)
            ? value.ToString()
            : ((int)value).ToString(
                CultureInfo.InvariantCulture);
    }

    private static string FormatConfChangeType(
        ConfChangeType value)
    {
        return Enum.IsDefined(value)
            ? value.ToString()
            : ((int)value).ToString(
                CultureInfo.InvariantCulture);
    }

    private static string FormatTransition(
        ConfChangeTransition value)
    {
        return value switch
        {
            ConfChangeTransition.Auto =>
                "ConfChangeTransitionAuto",
            ConfChangeTransition.JointImplicit =>
                "ConfChangeTransitionJointImplicit",
            ConfChangeTransition.JointExplicit =>
                "ConfChangeTransitionJointExplicit",
            _ => ((int)value).ToString(
                CultureInfo.InvariantCulture),
        };
    }

    private static string FormatPublicEnum<T>(T value)
        where T : struct, Enum
    {
        return Enum.IsDefined(value)
            ? value.ToString()
            : System.Convert.ToInt64(
                    value,
                    CultureInfo.InvariantCulture)
                .ToString(CultureInfo.InvariantCulture);
    }

    private static void AppendDecimal(
        StringBuilder builder,
        ulong value)
    {
        builder.Append(
            value.ToString(
                CultureInfo.InvariantCulture));
    }
}
