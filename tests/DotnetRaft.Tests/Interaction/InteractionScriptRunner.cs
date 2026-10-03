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

    internal static void RewriteFile(string file)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(file);
        string source = File.ReadAllText(file);
        string name = Path.GetFileName(file);
        IReadOnlyList<InteractionScriptCase> cases =
            Parse(source, name);
        var environment =
            new InteractionEnvironment();
        var builder = new System.Text.StringBuilder();
        foreach (InteractionScriptCase testCase in cases)
        {
            builder.AppendLine(testCase.CommandLine);
            if (testCase.Input.Length > 0)
            {
                builder.AppendLine(testCase.Input);
            }

            builder.AppendLine("----");
            builder.AppendLine(
                Normalize(
                    environment.Handle(
                        testCase.CommandLine,
                        testCase.Input,
                        name,
                        testCase.LineNumber)));
            builder.AppendLine();
        }

        File.WriteAllText(
            file,
            builder.ToString()
                .Replace(
                    "\r\n",
                    "\n",
                    StringComparison.Ordinal)
                .TrimEnd('\n')
                + "\n");
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
