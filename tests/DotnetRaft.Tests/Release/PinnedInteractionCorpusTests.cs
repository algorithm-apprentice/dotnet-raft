using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

using DotnetRaft.Tests.Interaction;

namespace DotnetRaft.Tests.Release;

public sealed class PinnedInteractionCorpusTests
{
    private const string ReferenceCommit =
        "1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e";
    private static readonly JsonSerializerOptions
        JsonOptions = new()
        {
            PropertyNameCaseInsensitive = true,
        };
    [Fact]
    public void PinnedCommandInputsMatchManifest()
    {
        string directory = Path.Combine(
            AppContext.BaseDirectory,
            "Interaction",
            "PinnedTestData");
        string manifestPath = Path.Combine(
            directory,
            "manifest.json");
        Assert.True(
            File.Exists(manifestPath),
            $"Missing pinned corpus manifest {manifestPath}.");

        CorpusManifest manifest =
            JsonSerializer.Deserialize<CorpusManifest>(
                File.ReadAllText(manifestPath),
                JsonOptions)
            ?? throw new InvalidOperationException(
                "Pinned corpus manifest is null.");
        Assert.Equal(
            ReferenceCommit,
            manifest.ReferenceCommit);
        Assert.Equal(558, manifest.TotalCases);
        Assert.Equal(28, manifest.Files.Count);

        string[] files = Directory.GetFiles(
            directory,
            "*.txt");
        Array.Sort(files, StringComparer.Ordinal);
        Assert.Equal(
            manifest.Files
                .Select(file => file.Name)
                .Order(StringComparer.Ordinal),
            files.Select(Path.GetFileName));

        var total = 0;
        foreach (CorpusFile expected in
                 manifest.Files.OrderBy(
                     file => file.Name,
                     StringComparer.Ordinal))
        {
            string path = Path.Combine(
                directory,
                expected.Name);
            IReadOnlyList<InteractionScriptCase> cases =
                InteractionScriptRunner.Parse(
                    File.ReadAllText(path),
                    expected.Name);
            total += cases.Count;
            Assert.Equal(
                expected.CaseCount,
                cases.Count);
            Assert.Equal(
                expected.CommandInputSha256,
                Digest(cases));
        }

        Assert.Equal(manifest.TotalCases, total);
    }

    private static string Digest(
        IEnumerable<InteractionScriptCase> cases)
    {
        var builder = new StringBuilder();
        foreach (InteractionScriptCase testCase in cases)
        {
            builder.Append(testCase.CommandLine);
            builder.Append('\n');
            builder.Append(
                testCase.Input.Replace(
                    "\r\n",
                    "\n",
                    StringComparison.Ordinal));
            builder.Append("\n----\n");
        }

        return Convert.ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(
                        builder.ToString())))
            .ToLowerInvariant();
    }

    private sealed record CorpusManifest(
        string ReferenceCommit,
        int TotalCases,
        IReadOnlyList<CorpusFile> Files);

    private sealed record CorpusFile(
        string Name,
        int CaseCount,
        string CommandInputSha256);
}
