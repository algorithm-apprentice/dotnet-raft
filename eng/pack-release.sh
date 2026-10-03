#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: pack-release.sh <repository-root> <commit>" >&2
  exit 2
fi

repo="$(cd "$1" && pwd -P)"
commit="$2"

test "$(git -C "$repo" rev-parse HEAD)" = "$commit"
if [[ -n "$(git -C "$repo" status --porcelain)" ]]; then
  echo "release packing requires a clean working tree" >&2
  exit 1
fi

SOURCE_DATE_EPOCH=315532800 \
dotnet pack "$repo/src/DotnetRaft/DotnetRaft.csproj" \
  -c Release \
  --no-restore \
  -p:ContinuousIntegrationBuild=true \
  -p:Deterministic=true \
  -p:RepositoryCommit="$commit" \
  -p:PathMap="$repo=/_/"
