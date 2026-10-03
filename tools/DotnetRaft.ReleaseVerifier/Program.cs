using System.IO.Compression;
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;
using System.Reflection.PortableExecutable;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Xml.Linq;

const string PackageId = "DotnetRaft";
const string PackageVersion = "1.0.0";
const string Description =
    "An educational, behavior-oriented C# implementation of the etcd Raft consensus state machine for .NET.";

if (args.Length == 0)
{
    return Fail(
        "Usage: verify <nupkg> <snupkg> <repository-root> <commit> | compare <left> <right>");
}

try
{
    switch (args[0])
    {
        case "verify" when args.Length == 5:
            VerifyPackage(
                Path.GetFullPath(args[1]),
                Path.GetFullPath(args[2]),
                Path.GetFullPath(args[3]),
                args[4]);
            return 0;
        case "compare" when args.Length == 3:
            ComparePackages(
                Path.GetFullPath(args[1]),
                Path.GetFullPath(args[2]));
            return 0;
        default:
            return Fail("Invalid release-verifier arguments.");
    }
}
catch (Exception exception)
{
    return Fail(exception.Message);
}

static void VerifyPackage(
    string packagePath,
    string symbolPath,
    string repositoryRoot,
    string expectedCommit)
{
    Require(
        expectedCommit.Length == 40
        && expectedCommit.All(Uri.IsHexDigit),
        "Expected commit is not a 40-character SHA.");
    RequireFile(packagePath);
    RequireFile(symbolPath);
    RequireFile(
        Path.Combine(repositoryRoot, "LICENSE"));
    RequireFile(
        Path.Combine(repositoryRoot, "README.md"));
    RequireFile(
        Path.Combine(
            repositoryRoot,
            "THIRD-PARTY-NOTICES.md"));

    using ZipArchive package =
        ZipFile.OpenRead(packagePath);
    RequireEntry(package, "lib/net10.0/DotnetRaft.dll");
    RequireEntry(package, "LICENSE");
    RequireEntry(package, "README.md");
    RequireEntry(package, "THIRD-PARTY-NOTICES.md");
    ZipArchiveEntry nuspecEntry =
        SingleNuspec(package);
    XDocument nuspec = ReadXml(nuspecEntry);
    XElement metadata = Metadata(nuspec);
    RequireValue(metadata, "id", PackageId);
    RequireValue(metadata, "version", PackageVersion);
    RequireValue(
        metadata,
        "authors",
        "algorithm-apprentice");
    RequireValue(
        metadata,
        "description",
        Description);
    RequireValue(metadata, "readme", "README.md");

    XNamespace ns = metadata.Name.Namespace;
    XElement license =
        metadata.Element(ns + "license")
        ?? throw new InvalidOperationException(
            "Package nuspec has no license metadata.");
    Require(
        string.Equals(
            (string?)license.Attribute("type"),
            "file",
            StringComparison.Ordinal),
        "Package license metadata is not file-based.");
    Require(
        string.Equals(
            license.Value,
            "LICENSE",
            StringComparison.Ordinal),
        "Package license file is not LICENSE.");

    XElement repository =
        metadata.Element(ns + "repository")
        ?? throw new InvalidOperationException(
            "Package nuspec has no repository metadata.");
    Require(
        string.Equals(
            (string?)repository.Attribute("type"),
            "git",
            StringComparison.Ordinal),
        "Repository type is not git.");
    Require(
        string.Equals(
            (string?)repository.Attribute("url"),
            "https://github.com/algorithm-apprentice/dotnet-raft",
            StringComparison.Ordinal),
        "Repository URL is incorrect.");
    string commit =
        (string?)repository.Attribute("commit")
        ?? string.Empty;
    Require(
        string.Equals(
            commit,
            expectedCommit,
            StringComparison.OrdinalIgnoreCase),
        $"Repository commit is {commit}, expected {expectedCommit}.");

    XElement[] dependencies = metadata
        .Descendants(ns + "dependency")
        .ToArray();
    XElement protobuf = dependencies.SingleOrDefault(
        dependency =>
            string.Equals(
                (string?)dependency.Attribute("id"),
                "Google.Protobuf",
                StringComparison.Ordinal))
        ?? throw new InvalidOperationException(
            "Google.Protobuf dependency is missing.");
    Require(
        ((string?)protobuf.Attribute("version")
         ?? string.Empty)
        .Contains(
            "3.36.2",
            StringComparison.Ordinal),
        "Google.Protobuf dependency version is not 3.36.2.");
    Require(
        dependencies.All(
            dependency =>
                !string.Equals(
                    (string?)dependency.Attribute("id"),
                    "Grpc.Tools",
                    StringComparison.Ordinal)),
        "Grpc.Tools leaked into runtime dependencies.");

    CompareEntry(
        package,
        "LICENSE",
        Path.Combine(repositoryRoot, "LICENSE"));
    CompareEntry(
        package,
        "README.md",
        Path.Combine(repositoryRoot, "README.md"));
    CompareEntry(
        package,
        "THIRD-PARTY-NOTICES.md",
        Path.Combine(
            repositoryRoot,
            "THIRD-PARTY-NOTICES.md"));

    byte[] dll = ReadEntry(
        package,
        "lib/net10.0/DotnetRaft.dll");
    using ZipArchive symbols =
        ZipFile.OpenRead(symbolPath);
    RequireEntry(
        symbols,
        "lib/net10.0/DotnetRaft.pdb");
    XElement symbolMetadata =
        Metadata(ReadXml(SingleNuspec(symbols)));
    RequireValue(symbolMetadata, "id", PackageId);
    RequireValue(
        symbolMetadata,
        "version",
        PackageVersion);
    XElement symbolRepository =
        symbolMetadata.Element(
            symbolMetadata.Name.Namespace
            + "repository")
        ?? throw new InvalidOperationException(
            "Symbol nuspec has no repository metadata.");
    Require(
        string.Equals(
            (string?)symbolRepository.Attribute("commit"),
            expectedCommit,
            StringComparison.OrdinalIgnoreCase),
        "Symbol package repository commit does not match.");
    XElement packageType = symbolMetadata
        .Descendants(
            symbolMetadata.Name.Namespace
            + "packageType")
        .SingleOrDefault()
        ?? throw new InvalidOperationException(
            "Symbol package type is missing.");
    Require(
        string.Equals(
            (string?)packageType.Attribute("name"),
            "SymbolsPackage",
            StringComparison.Ordinal),
        "Symbol package type is not SymbolsPackage.");

    ZipArchiveEntry[] symbolEntries =
    [
        .. symbols.Entries.Where(
            entry => !entry.FullName.EndsWith('/')),
    ];
    string[] duplicates = symbolEntries
        .GroupBy(
            entry => entry.FullName,
            StringComparer.Ordinal)
        .Where(group => group.Count() != 1)
        .Select(group => group.Key)
        .ToArray();
    Require(
        duplicates.Length == 0,
        $"Symbol package has duplicate entries: {string.Join(',', duplicates)}.");
    ZipArchiveEntry coreProperties =
        symbolEntries.Single(entry =>
            entry.FullName.StartsWith(
                "package/services/metadata/core-properties/",
                StringComparison.Ordinal)
            && entry.FullName.EndsWith(
                ".psmdcp",
                StringComparison.Ordinal));
    string[] expectedSymbolEntries =
    [
        "_rels/.rels",
        "DotnetRaft.nuspec",
        "lib/net10.0/DotnetRaft.pdb",
        "[Content_Types].xml",
        coreProperties.FullName,
    ];
    Require(
        symbolEntries
            .Select(entry => entry.FullName)
            .Order(StringComparer.Ordinal)
            .SequenceEqual(
                expectedSymbolEntries.Order(
                    StringComparer.Ordinal),
                StringComparer.Ordinal),
        "Symbol package entry set is not exact.");

    foreach (ZipArchiveEntry entry in symbolEntries)
    {
        Require(
            IsAllowedSymbolEntry(entry.FullName),
            $"Symbol package contains unsupported entry {entry.FullName}.");
    }

    ZipArchiveEntry pdbEntry =
        symbolEntries.Single(entry =>
            string.Equals(
                entry.FullName,
                "lib/net10.0/DotnetRaft.pdb",
                StringComparison.Ordinal));
    byte[] pdb = ReadEntryBytes(pdbEntry);
    Require(
        pdb.AsSpan(0, 4)
            .SequenceEqual("BSJB"u8),
        "Symbol package PDB is not portable.");
    VerifyDebugIdentity(
        dll,
        pdb,
        expectedCommit);

    Console.WriteLine(
        $"verified {PackageId} {PackageVersion} commit {commit}");
}

static void VerifyDebugIdentity(
    byte[] dll,
    byte[] pdb,
    string expectedCommit)
{
    using var peReader = new PEReader(
        new MemoryStream(dll, writable: false));
    DebugDirectoryEntry codeViewEntry =
        peReader.ReadDebugDirectory()
            .Single(entry =>
                entry.Type
                == DebugDirectoryEntryType.CodeView);
    CodeViewDebugDirectoryData codeView =
        peReader.ReadCodeViewDebugDirectoryData(
            codeViewEntry);
    using MetadataReaderProvider provider =
        MetadataReaderProvider.FromPortablePdbStream(
            new MemoryStream(pdb, writable: false));
    MetadataReader reader = provider.GetMetadataReader();
    DebugMetadataHeader header =
        reader.DebugMetadataHeader
        ?? throw new InvalidOperationException(
            "Portable PDB has no debug metadata header.");
    byte[] id = header.Id.ToArray();
    Require(id.Length >= 20, "Portable PDB ID is incomplete.");
    var pdbGuid = new Guid(id.AsSpan(0, 16));
    uint pdbStamp =
        BitConverter.ToUInt32(id, 16);
    Require(
        codeView.Guid == pdbGuid,
        "DLL and PDB GUIDs do not match.");
    Require(
        codeViewEntry.Stamp == pdbStamp,
        "DLL and PDB stamps do not match.");
    Require(
        codeView.Path.EndsWith(
            "DotnetRaft.pdb",
            StringComparison.Ordinal),
        "DLL CodeView path does not name DotnetRaft.pdb.");
    VerifySourceLink(reader, expectedCommit);
}

static void VerifySourceLink(
    MetadataReader reader,
    string expectedCommit)
{
    var sourceLinkKind = new Guid(
        "CC110556-A091-4D38-9FEC-25AB9A351A6A");
    CustomDebugInformation sourceLink =
        reader.GetCustomDebugInformation(
                MetadataTokens.EntityHandle(
                    TableIndex.Module,
                    1))
            .Select(reader.GetCustomDebugInformation)
            .Single(information =>
                reader.GetGuid(information.Kind)
                == sourceLinkKind);
    string json = Encoding.UTF8.GetString(
        reader.GetBlobBytes(sourceLink.Value));
    using JsonDocument document =
        JsonDocument.Parse(json);
    JsonElement documents =
        document.RootElement.GetProperty(
            "documents");
    JsonProperty[] mappings =
        documents.EnumerateObject().ToArray();
    string expectedUrl =
        "https://raw.githubusercontent.com/algorithm-apprentice/dotnet-raft/"
        + expectedCommit
        + "/*";
    Require(
        mappings.Length == 1
        && string.Equals(
            mappings[0].Name,
            "/_/*",
            StringComparison.Ordinal)
        && string.Equals(
            mappings[0].Value.GetString(),
            expectedUrl,
            StringComparison.Ordinal),
        "Source Link mapping is not the exact expected GitHub mapping.");
}

static bool IsAllowedSymbolEntry(string path)
{
    if (string.Equals(
            path,
            "[Content_Types].xml",
            StringComparison.Ordinal)
        || string.Equals(
            path,
            "_rels/.rels",
            StringComparison.Ordinal)
        || string.Equals(
            path,
            "DotnetRaft.nuspec",
            StringComparison.Ordinal)
        || string.Equals(
            path,
            "lib/net10.0/DotnetRaft.pdb",
            StringComparison.Ordinal))
    {
        return true;
    }

    return path.StartsWith(
               "package/services/metadata/core-properties/",
               StringComparison.Ordinal)
           && path.EndsWith(
               ".psmdcp",
               StringComparison.Ordinal);
}

static void ComparePackages(
    string leftPath,
    string rightPath)
{
    RequireFile(leftPath);
    RequireFile(rightPath);
    Dictionary<string, string> left =
        ExtractedHashes(leftPath);
    Dictionary<string, string> right =
        ExtractedHashes(rightPath);
    if (!left.Keys.SequenceEqual(
            right.Keys,
            StringComparer.Ordinal))
    {
        string leftOnly = string.Join(
            ",",
            left.Keys.Except(
                right.Keys,
                StringComparer.Ordinal));
        string rightOnly = string.Join(
            ",",
            right.Keys.Except(
                left.Keys,
                StringComparer.Ordinal));
        throw new InvalidOperationException(
            $"Package path sets differ; left-only=[{leftOnly}] right-only=[{rightOnly}].");
    }
    foreach (string path in left.Keys)
    {
        Require(
            string.Equals(
                left[path],
                right[path],
                StringComparison.Ordinal),
            $"Package entry differs: {path}");
    }

    Console.WriteLine(
        $"reproducible {Path.GetFileName(leftPath)}");
}

static Dictionary<string, string> ExtractedHashes(
    string path)
{
    using ZipArchive archive = ZipFile.OpenRead(path);
    if (archive.Entries.Any(
            entry => string.Equals(
                entry.FullName,
                ".signature.p7s",
                StringComparison.OrdinalIgnoreCase)))
    {
        throw new InvalidOperationException(
            "D26 packages must be unsigned.");
    }

    return archive.Entries
        .Where(entry =>
            !entry.FullName.EndsWith('/'))
        .OrderBy(
            entry => entry.FullName,
            StringComparer.Ordinal)
        .ToDictionary(
            entry => CanonicalPath(entry.FullName),
            entry =>
                Convert.ToHexString(
                    SHA256.HashData(
                        CanonicalBytes(entry))),
            StringComparer.Ordinal);
}

static string CanonicalPath(string path)
{
    return path.StartsWith(
            "package/services/metadata/core-properties/",
            StringComparison.Ordinal)
        && path.EndsWith(
            ".psmdcp",
            StringComparison.Ordinal)
            ? "package/services/metadata/core-properties/core-properties.psmdcp"
            : path;
}

static byte[] CanonicalBytes(ZipArchiveEntry entry)
{
    byte[] bytes = ReadEntryBytes(entry);
    if (!string.Equals(
            entry.FullName,
            "_rels/.rels",
            StringComparison.Ordinal))
    {
        return bytes;
    }

    XDocument document;
    using (var stream = new MemoryStream(bytes))
    {
        document = XDocument.Load(stream);
    }

    XNamespace ns = document.Root?.Name.Namespace
        ?? throw new InvalidOperationException(
            "Relationships XML has no root.");
    XElement relationship = document
        .Descendants(ns + "Relationship")
        .Single(element =>
            ((string?)element.Attribute("Type")
             ?? string.Empty)
            .EndsWith(
                "/metadata/core-properties",
                StringComparison.Ordinal));
    relationship.SetAttributeValue(
        "Target",
        "/package/services/metadata/core-properties/core-properties.psmdcp");
    relationship.SetAttributeValue("Id", "RCORE");
    using var output = new MemoryStream();
    document.Save(
        output,
        SaveOptions.DisableFormatting);
    return output.ToArray();
}

static XElement Metadata(XDocument document)
{
    XElement root = document.Root
        ?? throw new InvalidOperationException(
            "Nuspec has no root.");
    return root.Element(
               root.Name.Namespace + "metadata")
           ?? throw new InvalidOperationException(
               "Nuspec has no metadata.");
}

static void RequireValue(
    XElement metadata,
    string name,
    string expected)
{
    string actual =
        metadata.Element(
                metadata.Name.Namespace + name)
            ?.Value
        ?? string.Empty;
    Require(
        string.Equals(
            actual,
            expected,
            StringComparison.Ordinal),
        $"Nuspec {name} is '{actual}', expected '{expected}'.");
}

static ZipArchiveEntry SingleNuspec(
    ZipArchive archive)
{
    return archive.Entries.Single(
        entry => entry.FullName.EndsWith(
            ".nuspec",
            StringComparison.OrdinalIgnoreCase));
}

static XDocument ReadXml(ZipArchiveEntry entry)
{
    using Stream stream = entry.Open();
    return XDocument.Load(stream);
}

static void CompareEntry(
    ZipArchive archive,
    string entryPath,
    string filePath)
{
    byte[] packaged = ReadEntry(archive, entryPath);
    byte[] repository = File.ReadAllBytes(filePath);
    Require(
        packaged.AsSpan().SequenceEqual(repository),
        $"Packaged {entryPath} differs from repository bytes.");
}

static byte[] ReadEntry(
    ZipArchive archive,
    string path)
{
    return ReadEntryBytes(
        archive.GetEntry(path)
        ?? throw new InvalidOperationException(
            $"Package is missing {path}."));
}

static byte[] ReadEntryBytes(
    ZipArchiveEntry entry)
{
    using Stream stream = entry.Open();
    using var memory = new MemoryStream();
    stream.CopyTo(memory);
    return memory.ToArray();
}

static void RequireEntry(
    ZipArchive archive,
    string path)
{
    Require(
        archive.GetEntry(path) is not null,
        $"Package is missing {path}.");
}

static void RequireFile(string path)
{
    Require(
        File.Exists(path),
        $"File does not exist: {path}");
}

static void Require(bool condition, string message)
{
    if (!condition)
    {
        throw new InvalidOperationException(message);
    }
}

static int Fail(string message)
{
    Console.Error.WriteLine(message);
    return 1;
}
