using System.Collections.ObjectModel;

namespace DotnetRaft.Tests.Interaction;

internal sealed record InteractionArgument
{
    internal InteractionArgument(
        string key,
        IEnumerable<string>? values = null)
    {
        Key = key;
        Values = Array.AsReadOnly(
            values?.ToArray() ?? []);
    }

    internal string Key { get; }

    internal IReadOnlyList<string> Values { get; }
}

internal sealed record InteractionDirective
{
    private InteractionDirective(
        string command,
        IEnumerable<InteractionArgument> arguments)
    {
        Command = command;
        Arguments = Array.AsReadOnly(
            arguments.ToArray());
    }

    internal string Command { get; }

    internal IReadOnlyList<InteractionArgument> Arguments
    {
        get;
    }

    internal static InteractionDirective Parse(
        string source,
        string file,
        int lineNumber)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentException.ThrowIfNullOrWhiteSpace(file);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(
            lineNumber);

        try
        {
            return ParseCore(source);
        }
        catch (FormatException exception)
        {
            throw new FormatException(
                $"{file}:{lineNumber}: {exception.Message}",
                exception);
        }
    }

    private static InteractionDirective ParseCore(
        string source)
    {
        string remaining = source.Trim();
        string command = TakeUntil(
            ref remaining,
            " ");
        if (command.Length == 0)
        {
            throw new FormatException(
                "Directive command is empty.");
        }

        remaining = remaining.Trim();
        var arguments =
            new List<InteractionArgument>();
        while (remaining.Length > 0)
        {
            string key = TakeUntil(
                ref remaining,
                " =");
            if (key.Length == 0)
            {
                throw new FormatException(
                    "Directive argument key is empty.");
            }

            string[] values = [];
            if (remaining.Length > 0
                && remaining[0] == '=')
            {
                remaining = remaining[1..];
                if (remaining.Length == 0
                    || remaining[0] == ' ')
                {
                    values = [string.Empty];
                }
                else if (remaining[0] != '(')
                {
                    values =
                    [
                        TakeUntil(
                            ref remaining,
                            " "),
                    ];
                }
                else
                {
                    values = ParseParenthesized(
                        ref remaining);
                }
            }

            arguments.Add(
                new InteractionArgument(
                    key,
                    values));
            remaining = remaining.Trim();
        }

        return new InteractionDirective(
            command,
            arguments);
    }

    private static string[] ParseParenthesized(
        ref string remaining)
    {
        var values = new List<string>();
        var nesting = 1;
        var position = 1;
        var valueStart = position;
        while (nesting > 0)
        {
            if (position >= remaining.Length)
            {
                throw new FormatException(
                    "Parenthesized argument is not closed.");
            }

            char current = remaining[position++];
            switch (current)
            {
                case ',' when nesting == 1:
                    values.Add(
                        remaining[
                            valueStart..(position - 1)]);
                    while (position < remaining.Length
                           && remaining[position] == ' ')
                    {
                        position++;
                    }

                    valueStart = position;
                    break;
                case '(':
                    nesting++;
                    break;
                case ')':
                    nesting--;
                    break;
            }
        }

        values.Add(
            remaining[
                valueStart..(position - 1)]);
        remaining = remaining[position..].Trim();
        return [.. values];
    }

    private static string TakeUntil(
        ref string source,
        string delimiters)
    {
        int index = source.IndexOfAny(
            delimiters.ToCharArray());
        if (index < 0)
        {
            string result = source;
            source = string.Empty;
            return result;
        }

        string prefix = source[..index];
        source = source[index..];
        return prefix;
    }
}

internal static class InteractionParsing
{
    internal static bool ParseBoolean(
        string value,
        string name)
    {
        return value switch
        {
            "1" or "t" or "T" or "TRUE"
                or "true" or "True" => true,
            "0" or "f" or "F" or "FALSE"
                or "false" or "False" => false,
            _ => throw new FormatException(
                $"{name} value {value} is not a valid boolean."),
        };
    }

    internal static int ParseInt(
        string value,
        string name)
    {
        if (!int.TryParse(
                value,
                System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture,
                out int parsed))
        {
            throw new FormatException(
                $"{name} value {value} is not a valid Int32.");
        }

        return parsed;
    }

    internal static ulong ParseUInt64(
        string value,
        string name)
    {
        if (!ulong.TryParse(
                value,
                System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture,
                out ulong parsed))
        {
            throw new FormatException(
                $"{name} value {value} is not a valid UInt64.");
        }

        return parsed;
    }
}
