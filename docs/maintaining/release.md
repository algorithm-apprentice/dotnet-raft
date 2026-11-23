# Compatibility and release checks

Part of the [maintainer guide](README.md). Use the
[compatibility decision](../adr/0002-release-and-compatibility-contract.md)
to classify a change before editing an approval. Package verification does
not publish anything to NuGet.org.

## Check the boundary, not just the build

| Contract | Mechanically checked by |
|---|---|
| Core and SQLite managed API | [`PublicApi.approved.txt`](../../tests/DotnetRaft.Tests/Release/PublicApi.approved.txt), [`SqlitePublicApi.approved.txt`](../../tests/DotnetRaft.Tests/Release/SqlitePublicApi.approved.txt) |
| Protobuf wire numbers, types, cardinality, and enums | [`ProtocolDescriptor.approved.txt`](../../tests/DotnetRaft.Tests/Release/ProtocolDescriptor.approved.txt) |
| SQLite persisted format | [`SqliteSchema.approved.sql`](../../tests/DotnetRaft.Sqlite.Tests/Schema/SqliteSchema.approved.sql) and [reopen/recovery tests](../../tests/DotnetRaft.Sqlite.Tests/SqliteStorageDurabilityTests.cs) |
| Ready, durability, and host recovery behavior | The [integration contract](../integration/README.md) and [host boundary tests](testing.md) |
| Packaged files, dependencies, and symbols | [Release verifier](../../tools/DotnetRaft.ReleaseVerifier/Program.cs) and a consumer restore for each package |

Keep SQLite and its native assets out of the core package. `Grpc.Tools` is
build-only; `Google.Protobuf` is a core runtime dependency. Both packages
include license and attribution files and their own README.

## Rehearse the release locally

Run the [verification suite](testing.md#run-the-suite) first. For a package
to verify against Git, use a clean committed checkout. From its root:

```bash
commit=$(git rev-parse HEAD)
./eng/pack-release.sh "$PWD" "$commit"
./eng/verify-package.sh \
  artifacts/package/DotnetRaft.2.0.0.nupkg \
  artifacts/package/DotnetRaft.2.0.0.snupkg "$PWD" "$commit"
./eng/verify-sqlite-package.sh \
  artifacts/package/DotnetRaft.Sqlite.2.0.0.nupkg \
  artifacts/package/DotnetRaft.Sqlite.2.0.0.snupkg "$PWD" "$commit"
./eng/verify-reproducible-pack.sh "$PWD" "$commit"
```

Consumer checks restore from the candidate package directory in an isolated
cache. The reproducibility check builds the same commit in independent
checkouts and compares package contents. Use the
[local install guide](../integration/install.md) if you just want to try a
package built from an uncommitted checkout. See the
[CI workflow](../../.github/workflows/ci.yml) for the precise commands used
for the current version.
