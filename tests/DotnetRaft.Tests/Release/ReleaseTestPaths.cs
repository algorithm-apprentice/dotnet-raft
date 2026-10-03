namespace DotnetRaft.Tests.Release;

internal static class ReleaseTestPaths
{
    internal static string RepositoryRoot { get; } =
        Path.GetFullPath(
            Path.Combine(
                AppContext.BaseDirectory,
                "..",
                "..",
                "..",
                "..",
                ".."));

    internal static string OutputReleaseDirectory =>
        Path.Combine(
            AppContext.BaseDirectory,
            "Release");
}
