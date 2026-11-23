# Install from a checkout

Both packages target `net10.0`. Version `2.0.0` is packed and verified by CI,
but is not published to NuGet.org. These commands install a local build; to
just run the project, use the [examples](../../examples/README.md) instead.

## Build the packages

Use the .NET 10 SDK selected by [`global.json`](../../global.json). From the
repository root:

```bash
dotnet restore DotnetRaft.sln
dotnet pack src/DotnetRaft/DotnetRaft.csproj -c Release --no-restore
dotnet pack src/DotnetRaft.Sqlite/DotnetRaft.Sqlite.csproj -c Release --no-restore
```

The packages are written to `artifacts/package`.

## Add them to a consumer

In a .NET 10 consumer project, use its existing NuGet configuration, or create
one with `dotnet new nugetconfig` if it has none. The SDK template creates
`nuget.config` including NuGet.org. Keep an external source for dependencies,
use the actual configuration filename, and replace the path below with the
absolute path to the checkout:

```bash
dotnet nuget add source /absolute/path/to/dotnet-raft/artifacts/package \
  --name dotnet-raft-local --configfile nuget.config
dotnet add package DotnetRaft --version 2.0.0
```

For durable SQLite Raft storage, also install:

```bash
dotnet add package DotnetRaft.Sqlite --version 2.0.0
```

NuGet may reuse `2.0.0` after a rebuild. When testing fresh package contents,
set `NUGET_PACKAGES` to a new directory before restoring the consumer.

Continue with [choosing a node API and storage protocol](README.md). For SQLite
files, see the [package guide](../../src/DotnetRaft.Sqlite/README.md).
