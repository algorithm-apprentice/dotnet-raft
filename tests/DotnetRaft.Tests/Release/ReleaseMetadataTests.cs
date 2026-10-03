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
        Assert.Equal("1.0.0", Value(document, "Version"));
        Assert.Equal(
            "algorithm-apprentice",
            Value(document, "Authors"));
        Assert.Equal(
            "An educational, behavior-oriented C# implementation of the etcd Raft consensus state machine for .NET.",
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
    public void ReleaseDocumentsAndScriptsExist()
    {
        string[] paths =
        [
            "LICENSE",
            "README.md",
            "THIRD-PARTY-NOTICES.md",
            "docs/public-api.md",
            "docs/parity-matrix.md",
            "docs/performance.md",
            "eng/pack-release.sh",
            "eng/verify-package.sh",
            "eng/verify-reproducible-pack.sh",
            ".github/workflows/ci.yml",
            "benchmarks/DotnetRaft.Benchmarks/DotnetRaft.Benchmarks.csproj",
            "tools/DotnetRaft.ReleaseVerifier/DotnetRaft.ReleaseVerifier.csproj",
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
        string readme = File.ReadAllText(
            Path.Combine(
                ReleaseTestPaths.RepositoryRoot,
                "README.md"));
        MatchCollection links =
            MarkdownLinkRegex().Matches(readme);
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

    [Fact]
    public void RepositoryDocumentationLinksResolve()
    {
        string root = ReleaseTestPaths.RepositoryRoot;
        string[] documents = Directory.GetFiles(
            Path.Combine(root, "docs"),
            "*.md",
            SearchOption.AllDirectories);
        foreach (string document in documents)
        {
            string text = File.ReadAllText(document);
            foreach (Match link in
                     MarkdownLinkRegex().Matches(text))
            {
                string target =
                    link.Groups["target"].Value;
                if (target.StartsWith(
                        "https://",
                        StringComparison.Ordinal)
                    || target.StartsWith(
                        "http://",
                        StringComparison.Ordinal)
                    || target.StartsWith(
                        '#'))
                {
                    continue;
                }

                string path = target.Split('#')[0];
                if (path.Length == 0)
                {
                    continue;
                }

                string resolved = Path.GetFullPath(
                    Path.Combine(
                        Path.GetDirectoryName(document)!,
                        Uri.UnescapeDataString(path)));
                Assert.True(
                    File.Exists(resolved)
                    || Directory.Exists(resolved),
                    $"{Path.GetRelativePath(root, document)} links to missing {target}.");
            }
        }
    }

    [Fact]
    public void ReferenceCommitIsConsistent()
    {
        string[] documents =
        [
            "README.md",
            "THIRD-PARTY-NOTICES.md",
            "docs/parity-matrix.md",
            "docs/reference-architecture.md",
        ];
        foreach (string document in documents)
        {
            string path = Path.Combine(
                ReleaseTestPaths.RepositoryRoot,
                document);
            Assert.Contains(
                ReferenceCommit,
                File.ReadAllText(path),
                StringComparison.Ordinal);
        }
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
}
