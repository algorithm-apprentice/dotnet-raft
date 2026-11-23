using DotnetRaft.Tests.Release;

namespace DotnetRaft.Tests.Interaction;

public sealed class InteractionGoldenTests
{
    private static readonly string SourceDirectory = Path.Combine(
        ReleaseTestPaths.RepositoryRoot, "tests", "DotnetRaft.Tests", "Interaction");
    private static readonly string OutputDirectory = Path.Combine(AppContext.BaseDirectory, "Interaction");
    private static readonly string[] FixtureDirectories = ["TestData", "PinnedTestData"];

    public static TheoryData<string> Scenarios => new(FindScenarios(SourceDirectory));

    [Theory]
    [MemberData(nameof(Scenarios))]
    public void GoldenScenarioMatchesExactly(string file)
    {
        string copied = File.ReadAllText(Path.Combine(OutputDirectory, file));
        string source = File.ReadAllText(Path.Combine(SourceDirectory, file));
        Assert.True(string.Equals(source, copied, StringComparison.Ordinal), $"Copied fixture {file} is stale; rebuild the tests.");
        InteractionScriptRunner.Run(copied, file);
    }

    [Fact]
    public void OutputContainsExactlyTheCheckoutScenarios()
    {
        Assert.Equal(FindScenarios(SourceDirectory), FindScenarios(OutputDirectory));
    }

    private static IEnumerable<string> FindScenarios(string root)
    {
        return FixtureDirectories
            .SelectMany(directory => Directory.EnumerateFiles(Path.Combine(root, directory), "*.txt", SearchOption.AllDirectories))
            .Select(file => Path.GetRelativePath(root, file).Replace(Path.DirectorySeparatorChar, '/'))
            .Order(StringComparer.Ordinal);
    }
}
