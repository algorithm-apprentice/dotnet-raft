# dotnet-raft

`dotnet-raft` is an educational, behavior-oriented C# implementation of the
deterministic Raft consensus state machine from
[`etcd-io/raft`](https://github.com/etcd-io/raft), pinned to commit
`1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`.

It implements consensus only. Network transport, durable storage engines,
application state, snapshots, compaction policy, timers, and retries remain
host responsibilities.

## Install

Version `1.0.0` is packaged and verified by CI but is not currently published
to NuGet.org. From a repository checkout, build the local packages:

```bash
dotnet restore DotnetRaft.sln
dotnet pack src/DotnetRaft/DotnetRaft.csproj -c Release
dotnet pack src/DotnetRaft.Sqlite/DotnetRaft.Sqlite.csproj -c Release
```

In a .NET 10 consumer project, create `nuget.config` if the project does not
already have a NuGet configuration. The .NET SDK template creates this
lowercase filename and includes NuGet.org for dependencies:

```bash
dotnet new nugetconfig
```

From the consumer project directory, add the local package directory and
install. Replace the placeholder path with the absolute path to your
checkout. If using an existing configuration, pass its actual filename
and keep a source for external dependencies:

```bash
dotnet nuget add source /absolute/path/to/dotnet-raft/artifacts/package \
  --name dotnet-raft-local --configfile nuget.config
dotnet add package DotnetRaft --version 1.0.0
```

The package targets `net10.0`.

For durable SQLite consensus storage:

```bash
dotnet add package DotnetRaft.Sqlite --version 1.0.0
```

To verify a freshly rebuilt package with the same version, restore in a
consumer with an isolated NuGet package cache; NuGet may otherwise reuse
the previously installed `1.0.0`.

`DotnetRaft.Sqlite` is optional and keeps the core consensus package free of
native database dependencies.

## Choose an integration API

- `RawNode` is synchronous and thread-unsafe. The host owns one serialized
  event loop.
- `RaftNode` is a concurrent asynchronous facade. One background owner loop
  serializes commands around `RawNode` and exposes cancellable `ValueTask`
  operations. The host still runs the Ready consumer.

Either facade can use synchronous Ready/Advance or opt into asynchronous
storage writes. Follow the [host integration contract](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/public-api.md)
for the required ordering.

## Examples

Start with the [replicated counter](https://github.com/algorithm-apprentice/dotnet-raft/tree/main/examples/DotnetRaft.Counter):
three in-memory nodes, one command, and two selectable storage protocols.
Then try the [durable gRPC KV cluster](https://github.com/algorithm-apprentice/dotnet-raft/tree/main/examples/DotnetRaft.KvCluster)
to see a service with real networking and restart from SQLite. The
[example index](https://github.com/algorithm-apprentice/dotnet-raft/tree/main/examples)
compares the APIs and storage modes side by side.

## Documentation

- [Offline Raft learning guide and implementation deep dives](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/raft-learning-guide.md)
- [Public API and host responsibilities](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/public-api.md)
- [Durable SQLite storage package](https://github.com/algorithm-apprentice/dotnet-raft/tree/main/src/DotnetRaft.Sqlite)
- [Behavioral parity matrix](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/parity-matrix.md)
- [Reference architecture](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/reference-architecture.md)
- [Design records and completed porting plan](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/design/README.md)
- [Performance methodology and baseline](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/performance.md)
- [Release and compatibility ADR](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/adr/0002-release-and-compatibility-contract.md)
- [Apache-2.0 license](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/LICENSE)
- [Third-party notices](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/THIRD-PARTY-NOTICES.md)

## Project status

- **Completed milestone:** M7 Parity release
- **Completed nodes:** D00-D26
- **Completed:** behavior-preserving post-parity `RaftCore` refactoring
- **Available storage:** durable SQLite Raft log, hard state, snapshots, and
  restart support through `DotnetRaft.Sqlite`
