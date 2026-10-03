namespace DotnetRaft.Tests.Interaction;

public sealed class InteractionGoldenTests
{
    [Fact]
    public void InitialGoldenScenariosMatchExactly()
    {
        string directory = Path.Combine(
            AppContext.BaseDirectory,
            "Interaction",
            "TestData");
        string[] files = Directory.GetFiles(
            directory,
            "*.txt");
        Array.Sort(files, StringComparer.Ordinal);

        Assert.Equal(4, files.Length);
        foreach (string file in files)
        {
            InteractionScriptRunner.RunFile(file);
            InteractionScriptRunner.RunFile(file);
        }
    }
}
