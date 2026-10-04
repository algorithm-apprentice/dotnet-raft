#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: verify-sqlite-package.sh <nupkg> <snupkg> <repository-root> <commit>" >&2
  exit 2
fi

resolve_file() {
  local directory
  directory="$(cd "$(dirname "$1")" && pwd)"
  printf '%s/%s\n' "$directory" "$(basename "$1")"
}

nupkg="$(resolve_file "$1")"
snupkg="$(resolve_file "$2")"
repo="$(cd "$3" && pwd)"
commit="$4"

dotnet run \
  --project "$repo/tools/DotnetRaft.ReleaseVerifier/DotnetRaft.ReleaseVerifier.csproj" \
  -c Release \
  --no-restore \
  -- verify-sqlite "$nupkg" "$snupkg" "$repo" "$commit"

temp="$(mktemp -d)"
trap 'rm -rf "$temp"' EXIT

packages="$temp/packages"
http_cache="$temp/http-cache"
consumer="$temp/consumer"
mkdir -p "$packages" "$http_cache" "$consumer"

package_dir="$(cd "$(dirname "$nupkg")" && pwd)"
cat > "$temp/NuGet.Config" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="candidate" value="$package_dir" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
  <packageSourceMapping>
    <packageSource key="candidate">
      <package pattern="DotnetRaft" />
      <package pattern="DotnetRaft.Sqlite" />
    </packageSource>
    <packageSource key="nuget.org">
      <package pattern="Google.*" />
      <package pattern="Microsoft.*" />
      <package pattern="SQLitePCLRaw.*" />
      <package pattern="System.*" />
    </packageSource>
  </packageSourceMapping>
</configuration>
EOF

cat > "$consumer/Consumer.csproj" <<'EOF'
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="DotnetRaft.Sqlite" Version="1.0.0" />
  </ItemGroup>
</Project>
EOF

cat > "$consumer/Program.cs" <<'EOF'
using DotnetRaft.Protocol;
using DotnetRaft.Storage.Sqlite;

string directory = Path.Combine(
    Path.GetTempPath(),
    "dotnet-raft-sqlite-consumer",
    Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(directory);
string database = Path.Combine(directory, "raft.db");
try
{
    using (var storage = new SqliteStorage(database))
    {
        storage.SetHardState(
            new HardState
            {
                Term = 1,
                Vote = 1,
            });
        storage.Append(
        [
            new Entry
            {
                Index = 1,
                Term = 1,
            },
        ]);
        storage.SetHardState(
            new HardState
            {
                Term = 1,
                Vote = 1,
                Commit = 1,
            });
    }

    using var reopened = new SqliteStorage(database);
    if (reopened.GetLastIndex() != 1
        || reopened.GetTerm(1) != 1
        || reopened.GetHardState()?.Commit != 1)
    {
        throw new InvalidOperationException(
            "SQLite package consumer recovery failed.");
    }
}
finally
{
    Directory.Delete(directory, recursive: true);
}
EOF

NUGET_PACKAGES="$packages" \
NUGET_HTTP_CACHE_PATH="$http_cache" \
dotnet restore "$consumer/Consumer.csproj" \
  --configfile "$temp/NuGet.Config" \
  --no-cache

for package in dotnetraft dotnetraft.sqlite; do
  metadata="$packages/$package/1.0.0/.nupkg.metadata"
  test -f "$metadata"
  resolved_source="$(
    sed -n 's/.*"source"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$metadata"
  )"
  resolved_source="$(cd "$resolved_source" && pwd)"
  test "$resolved_source" = "$package_dir"
done

NUGET_PACKAGES="$packages" \
NUGET_HTTP_CACHE_PATH="$http_cache" \
dotnet run --project "$consumer/Consumer.csproj" \
  -c Release \
  --no-restore

echo "consumer verified DotnetRaft.Sqlite 1.0.0"
