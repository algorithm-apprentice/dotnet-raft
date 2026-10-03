namespace DotnetRaft.Tests.Interaction;

internal sealed record InteractionScriptCase(
    int Number,
    int LineNumber,
    string CommandLine,
    string Input,
    string Expected);

internal static class InteractionScriptRunner
{
    internal static IReadOnlyList<InteractionScriptCase>
        Parse(
            string source,
            string file)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentException.ThrowIfNullOrWhiteSpace(file);

        string normalized = source.Replace(
            "\r\n",
            "\n",
            StringComparison.Ordinal);
        string[] lines = normalized.Split('\n');
        var cases =
            new List<InteractionScriptCase>();
        var index = 0;
        while (index < lines.Length)
        {
            SkipSeparators(lines, ref index);
            if (index >= lines.Length)
            {
                break;
            }

            int commandLineNumber = index + 1;
            string command = lines[index++].Trim();
            var input = new List<string>();
            while (index < lines.Length
                   && !string.Equals(
                       lines[index],
                       "----",
                       StringComparison.Ordinal))
            {
                input.Add(lines[index++]);
            }

            if (index >= lines.Length)
            {
                throw new FormatException(
                    $"{file}:{commandLineNumber}: missing ---- separator.");
            }

            index++;
            var expected = new List<string>();
            while (index < lines.Length
                   && !string.IsNullOrWhiteSpace(
                       lines[index])
                   && !lines[index].StartsWith(
                       '#'))
            {
                expected.Add(lines[index++]);
            }

            cases.Add(
                new InteractionScriptCase(
                    cases.Count + 1,
                    commandLineNumber,
                    command,
                    string.Join('\n', input),
                    string.Join('\n', expected)));
        }

        return cases;
    }

    internal static void Run(
        string source,
        string file)
    {
        var environment =
            new InteractionEnvironment();
        foreach (InteractionScriptCase testCase
                 in Parse(source, file))
        {
            string actual = Normalize(
                environment.Handle(
                    testCase.CommandLine,
                    testCase.Input,
                    file,
                    testCase.LineNumber));
            string expected = Normalize(
                testCase.Expected);
            if (!string.Equals(
                    expected,
                    actual,
                    StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"{file} case {testCase.Number} at line {testCase.LineNumber}" +
                    $"\nExpected:\n{expected}" +
                    $"\nActual:\n{actual}");
            }
        }
    }

    internal static void RunFile(string file)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(file);
        Run(
            File.ReadAllText(file),
            Path.GetFileName(file));
    }

    private static void SkipSeparators(
        string[] lines,
        ref int index)
    {
        while (index < lines.Length
               && (string.IsNullOrWhiteSpace(lines[index])
                   || lines[index].StartsWith('#')))
        {
            index++;
        }
    }

    private static string Normalize(string value)
    {
        if (value.EndsWith(
                '\n'))
        {
            return value[..^1];
        }

        return value;
    }
}
