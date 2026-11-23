#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: bash eng/verify-raft-reference.sh <repository-root>" >&2
  exit 2
fi

repo="$(cd "$1" && pwd -P)"
harness="$repo/tests/DotnetRaft.Tests/bin/Release/net10.0/DotnetRaft.Tests.dll"
schedules="$repo/tests/DotnetRaft.Tests/Interaction/DifferentialTestData"
if [[ ! -f "$harness" ]]; then
  echo "Build the Release test project before running the reference comparison." >&2
  exit 1
fi

temp="$(mktemp -d)"
trap 'rm -rf -- "$temp"' EXIT
(cd "$repo/tools/raft-reference" && go build -mod=readonly -o "$temp/raft-reference" .)
shopt -s nullglob
fixtures=("$schedules"/*.json)
if [[ ${#fixtures[@]} -eq 0 ]]; then
  echo "No reference schedules found at $schedules." >&2
  exit 1
fi
for schedule in "${fixtures[@]}"; do
  modes=(sync async)
  if [[ "${schedule##*/}" == async-* ]]; then
    modes=(async)
  fi
  for mode in "${modes[@]}"; do
    if ! "$temp/raft-reference" "$schedule" "$mode" >"$temp/reference.json"; then
      echo "Go reference failed for ${schedule##*/}/$mode." >&2
      exit 1
    fi
    dotnet "$harness" compare-raft "$schedule" "$mode" "$temp/reference.json"
  done
done
