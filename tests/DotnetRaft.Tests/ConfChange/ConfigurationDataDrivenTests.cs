using DotnetRaft.ConfChange;
using DotnetRaft.Protocol;
using DotnetRaft.Tracker;

namespace DotnetRaft.Tests.ConfChange;

public sealed class ConfigurationDataDrivenTests
{
    [Fact]
    public void PinnedConfigurationChangeCorpusMatchesExactly()
    {
        string directory = Path.Combine(
            AppContext.BaseDirectory,
            "ConfChange",
            "TestData");
        string[] files = Directory.GetFiles(directory, "*.txt");
        Array.Sort(files, StringComparer.Ordinal);

        Assert.Equal(9, files.Length);
        foreach (string file in files)
        {
            RunFile(file);
        }
    }

    private static void RunFile(string file)
    {
        var tracker = new ProgressTracker(10, 0);
        ulong lastIndex = 0;

        foreach (DataCase testCase in ParseFile(file))
        {
            string actual;
            try
            {
                var changer = new ConfigurationChanger(
                    tracker,
                    lastIndex);
                ConfChangeSingle[] changes = ParseChanges(
                    testCase.Input);

                ConfigurationChangeResult result =
                    testCase.Command switch
                    {
                        "simple" => changer.Simple(changes),
                        "enter-joint" => changer.EnterJoint(
                            testCase.AutoLeave,
                            changes),
                        "leave-joint" when changes.Length == 0 =>
                            changer.LeaveJoint(),
                        "leave-joint" => throw new InvalidOperationException(
                            "leave-joint does not accept changes."),
                        _ => throw new InvalidOperationException(
                            $"Unknown command {testCase.Command}."),
                    };

                tracker.Install(
                    result.Config,
                    result.Progress);
                actual = $"{tracker.Config}\n{tracker.Progress}"
                    .TrimEnd('\r', '\n');
            }
            catch (ConfigurationChangeException exception)
            {
                actual = exception.Message;
            }
            finally
            {
                lastIndex++;
            }

            Assert.True(
                string.Equals(
                    testCase.Expected,
                    actual,
                    StringComparison.Ordinal),
                $"{Path.GetFileName(file)} case {testCase.Number}" +
                $"\nExpected:\n{testCase.Expected}" +
                $"\nActual:\n{actual}");
        }
    }

    private static List<DataCase> ParseFile(string file)
    {
        string[] lines = File.ReadAllLines(file);
        var cases = new List<DataCase>();
        var index = 0;

        while (index < lines.Length)
        {
            SkipSeparators(lines, ref index);
            if (index >= lines.Length)
            {
                break;
            }

            string commandLine = lines[index++].Trim();
            string[] commandParts = commandLine.Split(
                ' ',
                StringSplitOptions.RemoveEmptyEntries);
            string command = commandParts[0];
            bool autoLeave = commandParts
                .Skip(1)
                .Any(part => string.Equals(
                    part,
                    "autoleave=true",
                    StringComparison.Ordinal));

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
                throw new InvalidOperationException(
                    $"{file} has no separator for {commandLine}.");
            }

            index++;
            var expected = new List<string>();
            while (index < lines.Length
                   && !string.IsNullOrWhiteSpace(lines[index])
                   && !lines[index].StartsWith('#')
                   && !IsCommand(lines[index]))
            {
                expected.Add(lines[index++]);
            }

            cases.Add(
                new DataCase(
                    cases.Count + 1,
                    command,
                    autoLeave,
                    string.Join(' ', input).Trim(),
                    string.Join('\n', expected)));
        }

        return cases;
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

    private static bool IsCommand(string line)
    {
        return line.StartsWith("simple", StringComparison.Ordinal)
            || line.StartsWith(
                "enter-joint",
                StringComparison.Ordinal)
            || line.StartsWith(
                "leave-joint",
                StringComparison.Ordinal);
    }

    private static ConfChangeSingle[] ParseChanges(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
        {
            return [];
        }

        return input.Split(
                ' ',
                StringSplitOptions.RemoveEmptyEntries)
            .Select(ParseChange)
            .ToArray();
    }

    private static ConfChangeSingle ParseChange(string token)
    {
        if (token.Length < 2
            || !ulong.TryParse(token.AsSpan(1), out ulong id))
        {
            throw new InvalidOperationException(
                $"Invalid configuration token {token}.");
        }

        ConfChangeType type = token[0] switch
        {
            'v' => ConfChangeType.ConfChangeAddNode,
            'l' => ConfChangeType.ConfChangeAddLearnerNode,
            'r' => ConfChangeType.ConfChangeRemoveNode,
            'u' => ConfChangeType.ConfChangeUpdateNode,
            _ => throw new InvalidOperationException(
                $"Unknown configuration token {token}."),
        };

        return new ConfChangeSingle
        {
            Type = type,
            NodeId = id,
        };
    }

    private sealed record DataCase(
        int Number,
        string Command,
        bool AutoLeave,
        string Input,
        string Expected);
}
