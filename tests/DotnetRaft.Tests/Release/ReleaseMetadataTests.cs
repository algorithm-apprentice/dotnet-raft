using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace DotnetRaft.Tests.Release;

public sealed partial class ReleaseMetadataTests
{
    private const string ReferenceCommit =
        "1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e";

    [Fact]
    public void ProjectDeclaresReleasePackageMetadata()
    {
        string project = Path.Combine(
            ReleaseTestPaths.RepositoryRoot,
            "src",
            "DotnetRaft",
            "DotnetRaft.csproj");
        XDocument document = XDocument.Load(project);

        Assert.Equal("true", Value(document, "IsPackable"));
        Assert.Equal("DotnetRaft", Value(document, "PackageId"));
        Assert.Equal("3.0.0", Value(document, "Version"));
        Assert.Empty(document.Descendants("PackageReference"));
        Assert.Empty(document.Descendants("ProjectReference"));
        Assert.Equal(
            "algorithm-apprentice",
            Value(document, "Authors"));
        Assert.Equal(
            "A deterministic Raft consensus library for building replicated state machines in .NET.",
            Value(document, "Description"));
        Assert.Equal(
            "LICENSE",
            Value(document, "PackageLicenseFile"));
        Assert.Equal(
            "README.md",
            Value(document, "PackageReadmeFile"));
        Assert.Equal(
            "git",
            Value(document, "RepositoryType"));
        Assert.Equal(
            "https://github.com/algorithm-apprentice/dotnet-raft",
            Value(document, "RepositoryUrl"));
        Assert.Equal(
            "true",
            Value(document, "IncludeSymbols"));
        Assert.Equal(
            "snupkg",
            Value(document, "SymbolPackageFormat"));
        Assert.Equal(
            "../../artifacts/package",
            Value(document, "PackageOutputPath"));

        string[] packed = document
            .Descendants("None")
            .Where(element =>
                string.Equals(
                    (string?)element.Attribute("Pack"),
                    "true",
                    StringComparison.OrdinalIgnoreCase))
            .Select(element =>
                Path.GetFileName(
                    (string?)element.Attribute("Include"))
                ?? string.Empty)
            .Order(StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(
            [
                "LICENSE",
                "README.md",
                "THIRD-PARTY-NOTICES.md",
            ],
            packed);
    }

    [Fact]
    public void SqliteProjectDeclaresReleasePackageMetadata()
    {
        string project = Path.Combine(
            ReleaseTestPaths.RepositoryRoot,
            "src",
            "DotnetRaft.Sqlite",
            "DotnetRaft.Sqlite.csproj");
        XDocument document = XDocument.Load(project);

        Assert.Equal("true", Value(document, "IsPackable"));
        Assert.Equal(
            "DotnetRaft.Sqlite",
            Value(document, "PackageId"));
        Assert.Equal("3.0.0", Value(document, "Version"));
        Assert.Equal(
            "algorithm-apprentice",
            Value(document, "Authors"));
        Assert.Equal(
            "Durable SQLite consensus storage for DotnetRaft.",
            Value(document, "Description"));
        Assert.Equal(
            "LICENSE",
            Value(document, "PackageLicenseFile"));
        Assert.Equal(
            "README.md",
            Value(document, "PackageReadmeFile"));
        Assert.Equal(
            "true",
            Value(document, "IncludeSymbols"));
        Assert.Equal(
            "snupkg",
            Value(document, "SymbolPackageFormat"));

        XElement sqlite = document
            .Descendants("PackageReference")
            .Single(element =>
                string.Equals(
                    (string?)element.Attribute(
                        "Include"),
                    "Microsoft.Data.Sqlite",
                    StringComparison.Ordinal));
        Assert.Equal(
            "10.0.12",
            (string?)sqlite.Attribute("Version"));
        Assert.Contains(
            document.Descendants("ProjectReference"),
            element =>
                ((string?)element.Attribute("Include")
                 ?? string.Empty)
                .EndsWith(
                    "DotnetRaft.csproj",
                    StringComparison.Ordinal));
    }

    [Fact]
    public void ReleaseDocumentsAndScriptsExist()
    {
        string[] paths =
        [
            "LICENSE",
            "README.md",
            "THIRD-PARTY-NOTICES.md",
            "docs/README.md",
            "docs/integration/README.md",
            "docs/integration/recovery.md",
            "docs/internals/README.md",
            "docs/learning/README.md",
            "docs/maintaining/README.md",
            "docs/maintaining/benchmarks.md",
            "eng/pack-release.sh",
            "eng/verify-package.sh",
            "eng/verify-sqlite-package.sh",
            "eng/verify-reproducible-pack.sh",
            "eng/smoke-durable-kv-cluster.sh",
            ".github/workflows/ci.yml",
            "benchmarks/DotnetRaft.Benchmarks/DotnetRaft.Benchmarks.csproj",
            "tools/DotnetRaft.ReleaseVerifier/DotnetRaft.ReleaseVerifier.csproj",
            "src/DotnetRaft.Sqlite/DotnetRaft.Sqlite.csproj",
            "src/DotnetRaft.Sqlite/README.md",
            "src/DotnetRaft.Protobuf/DotnetRaft.Protobuf.csproj",
            "src/DotnetRaft.Protobuf/README.md",
        ];
        foreach (string relative in paths)
        {
            Assert.True(
                File.Exists(
                    Path.Combine(
                        ReleaseTestPaths.RepositoryRoot,
                        relative)),
                $"Missing release artifact {relative}.");
        }
    }

    [Fact]
    public void ReadmeUsesOnlyAbsoluteHttpsLinks()
    {
        string[] readmes =
        {
            Path.Combine(
                ReleaseTestPaths.RepositoryRoot,
                "README.md"),
            Path.Combine(
                ReleaseTestPaths.RepositoryRoot,
                "src",
                "DotnetRaft.Sqlite",
                "README.md"),
        };
        foreach (string path in readmes)
        {
            MatchCollection links =
                MarkdownLinkRegex().Matches(
                    File.ReadAllText(path));
            Assert.NotEmpty(links);
            foreach (Match link in links)
            {
                string target =
                    link.Groups["target"].Value;
                Assert.StartsWith(
                    "https://",
                    target,
                    StringComparison.Ordinal);
            }
        }
    }

    [Fact]
    public void RepositoryDocumentationLinksResolve()
    {
        string root = ReleaseTestPaths.RepositoryRoot;
        IEnumerable<string> documents = DocumentationPaths(root)
            .Append(Path.Combine(root, "AGENTS.md"));
        foreach (string document in documents)
        {
            string text = File.ReadAllText(document);
            foreach (Match link in
                     MarkdownLinkRegex().Matches(text))
            {
                string target =
                    link.Groups["target"].Value;
                string? resolved = ResolveDocumentationPath(
                    root,
                    document,
                    target);
                if (resolved is null)
                {
                    continue;
                }

                Assert.True(
                    PathExistsWithExactCase(root, resolved),
                    $"{Path.GetRelativePath(root, document)} links to missing {target}.");
            }
        }
    }

    [Fact]
    public void AllDocumentationIsReachableFromTheRepositoryReadme()
    {
        string root = ReleaseTestPaths.RepositoryRoot;
        var unseen = DocumentationPaths(root).ToHashSet(StringComparer.Ordinal);
        var queue = new Queue<string>();
        queue.Enqueue(Path.Combine(root, "README.md"));
        while (queue.TryDequeue(out string? document))
        {
            if (!unseen.Remove(document))
            {
                continue;
            }

            foreach (Match link in
                     MarkdownLinkRegex().Matches(File.ReadAllText(document)))
            {
                string? target = ResolveDocumentationPath(
                    root,
                    document,
                    link.Groups["target"].Value);
                if (target is null)
                {
                    continue;
                }

                if (Directory.Exists(target))
                {
                    target = Path.Combine(target, "README.md");
                }

                if (unseen.Contains(target))
                {
                    queue.Enqueue(target);
                }
            }
        }

        string missing = string.Join(
            ", ",
            unseen.Select(path => Path.GetRelativePath(root, path))
                .Order(StringComparer.Ordinal));
        Assert.True(unseen.Count == 0, $"Unreachable documentation: {missing}");
    }

    [Fact]
    public void ThirdPartyNoticesIdentifyTheFixtureSource()
    {
        string notices = File.ReadAllText(Path.Combine(
            ReleaseTestPaths.RepositoryRoot,
            "THIRD-PARTY-NOTICES.md"));
        Assert.Contains(ReferenceCommit, notices, StringComparison.Ordinal);
    }

    [Fact]
    public void PerformanceCommitContainsTheRecordedBenchmarkBlob()
    {
        string performance = File.ReadAllText(
            Path.Combine(
                ReleaseTestPaths.RepositoryRoot,
                "docs",
            "maintaining",
            "benchmarks.md"));
        Match match = Regex.Match(
            performance,
            @"^commit: (?<commit>[0-9a-f]{40})$",
            RegexOptions.Multiline
            | RegexOptions.CultureInvariant);
        Assert.True(
            match.Success,
            "Performance document has no commit provenance.");
        Match blob = Regex.Match(performance, @"^workloadBlob: (?<blob>[0-9a-f]{40})$", RegexOptions.Multiline);
        Assert.True(blob.Success, "Performance document has no workload provenance.");
        string relative =
            "benchmarks/DotnetRaft.Benchmarks/Program.cs";
        var startInfo = new ProcessStartInfo(
            "git",
            $"-C \"{ReleaseTestPaths.RepositoryRoot}\" rev-parse --verify \"{match.Groups["commit"].Value}:{relative}\"")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        using Process process =
            Process.Start(startInfo)
            ?? throw new InvalidOperationException(
                "Could not start git.");
        string committed =
            process.StandardOutput.ReadToEnd();
        string error =
            process.StandardError.ReadToEnd();
        process.WaitForExit();
        Assert.True(
            process.ExitCode == 0,
            error);
        Assert.Equal(blob.Groups["blob"].Value, committed.Trim());
    }

    private static IEnumerable<string> DocumentationPaths(string root)
    {
        return Directory.EnumerateFiles(
                Path.Combine(root, "docs"),
                "*.md",
                SearchOption.AllDirectories)
            .Concat(Directory.EnumerateFiles(
                Path.Combine(root, "examples"),
                "README.md",
                SearchOption.AllDirectories))
            .Concat([
                Path.Combine(root, "README.md"),
                Path.Combine(root, "src", "DotnetRaft.Sqlite", "README.md"),
                Path.Combine(root, "THIRD-PARTY-NOTICES.md"),
            ]);
    }

    private static string? ResolveDocumentationPath(
        string root,
        string document,
        string target)
    {
        string path = target.Split('#')[0];
        if (path.Length == 0)
        {
            return document;
        }

        Match repositoryLink = RepositoryMainLinkRegex().Match(path);
        if (repositoryLink.Success)
        {
            return Path.GetFullPath(
                Uri.UnescapeDataString(repositoryLink.Groups["path"].Value),
                root);
        }

        if (Uri.TryCreate(path, UriKind.Absolute, out _))
        {
            return null;
        }

        return Path.GetFullPath(
            Uri.UnescapeDataString(path),
            Path.GetDirectoryName(document)!);
    }

    private static bool PathExistsWithExactCase(string root, string fullPath)
    {
        string directory = root;
        foreach (string part in Path.GetRelativePath(root, fullPath)
                     .Split(Path.DirectorySeparatorChar, StringSplitOptions.RemoveEmptyEntries))
        {
            if (part == ".")
            {
                continue;
            }

            if (!Directory.Exists(directory)
                || !Directory.EnumerateFileSystemEntries(directory)
                    .Any(entry => Path.GetFileName(entry) == part))
            {
                return false;
            }

            directory = Path.Combine(directory, part);
        }

        return true;
    }

    private static string Value(
        XDocument document,
        string name)
    {
        return document.Descendants(name)
                   .Select(element => element.Value)
                   .LastOrDefault()
               ?? string.Empty;
    }

    [GeneratedRegex(
        @"\[[^\]]+\]\((?<target>[^)\s]+)(?:\s+""[^""]*"")?\)",
        RegexOptions.CultureInvariant)]
    private static partial Regex MarkdownLinkRegex();

    [GeneratedRegex(
        @"^https://github\.com/algorithm-apprentice/dotnet-raft/(?:blob|tree)/main/(?<path>.+)$",
        RegexOptions.CultureInvariant)]
    private static partial Regex RepositoryMainLinkRegex();
}
