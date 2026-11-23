#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: verify-reproducible-pack.sh <repository-root> <commit>" >&2
  exit 2
fi

repo="$(cd "$1" && pwd -P)"
commit="$2"
test "$(git -C "$repo" rev-parse HEAD)" = "$commit"
if [[ -n "$(git -C "$repo" status --porcelain)" ]]; then
  echo "reproducibility verification requires a clean working tree" >&2
  exit 1
fi

candidate="$repo/artifacts/package"
packages=(
  DotnetRaft.2.0.0.nupkg
  DotnetRaft.2.0.0.snupkg
  DotnetRaft.Sqlite.2.0.0.nupkg
  DotnetRaft.Sqlite.2.0.0.snupkg
)
for package in "${packages[@]}"; do
  test -f "$candidate/$package"
done

temp="$(cd "$(mktemp -d)" && pwd -P)"
left="$temp/left"
right="$temp/right"

cleanup() {
  git -C "$repo" worktree remove --force "$left" >/dev/null 2>&1 || true
  git -C "$repo" worktree remove --force "$right" >/dev/null 2>&1 || true
  rm -rf "$temp"
}
trap cleanup EXIT

git -C "$repo" worktree add --detach "$left" "$commit" >/dev/null
git -C "$repo" worktree add --detach "$right" "$commit" >/dev/null

pack_worktree() {
  local source="$1"
  dotnet restore "$source/DotnetRaft.sln"
  "$source/eng/pack-release.sh" "$source" "$commit"
}

pack_worktree "$left"
pack_worktree "$right"

verifier="$repo/tools/DotnetRaft.ReleaseVerifier/DotnetRaft.ReleaseVerifier.csproj"
compare() {
  dotnet run --project "$verifier" -c Release --no-restore -- \
    compare "$1" "$2"
}

for package in "${packages[@]}"; do
  compare "$candidate/$package" "$left/artifacts/package/$package"
  compare "$candidate/$package" "$right/artifacts/package/$package"
done
