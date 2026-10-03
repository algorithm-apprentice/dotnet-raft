namespace DotnetRaft.Tests.Interaction;

public sealed class InteractionGoldenTests
{
    [Fact]
    public void InitialGoldenScenariosMatchExactly()
    {
        bool rewrite = string.Equals(
            Environment.GetEnvironmentVariable(
                "DOTNET_RAFT_REWRITE_INTERACTION"),
            "1",
            StringComparison.Ordinal);
        string directory = rewrite
            ? Path.GetFullPath(
                Path.Combine(
                    AppContext.BaseDirectory,
                    "..",
                    "..",
                    "..",
                    "Interaction",
                    "TestData"))
            : Path.Combine(
                AppContext.BaseDirectory,
                "Interaction",
                "TestData");
        string[] files = Directory.GetFiles(
            directory,
            "*.txt");
        Array.Sort(files, StringComparer.Ordinal);

        Assert.Equal(8, files.Length);
        foreach (string file in files)
        {
            if (rewrite)
            {
                InteractionScriptRunner.RewriteFile(file);
                continue;
            }

            InteractionScriptRunner.RunFile(file);
            InteractionScriptRunner.RunFile(file);
        }
    }
}
