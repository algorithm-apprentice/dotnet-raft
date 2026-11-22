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

In the consumer project, create `NuGet.Config` if it does not already have one,
add the local package directory alongside NuGet.org, and install:

```bash
dotnet new nugetconfig
dotnet nuget add source /absolute/path/to/dotnet-raft/artifacts/package \
  --name dotnet-raft-local --configfile NuGet.Config
dotnet add package DotnetRaft --version 1.0.0
```

The package targets `net10.0`.

For durable SQLite consensus storage:

```bash
dotnet add package DotnetRaft.Sqlite --version 1.0.0
```

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
for the required ordering. To see the complete synchronous loop running with
three nodes, start the [gRPC KV example](https://github.com/algorithm-apprentice/dotnet-raft/tree/main/examples/DotnetRaft.KvCluster).

## Documentation

- [Offline Raft learning guide and implementation deep dives](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/raft-learning-guide.md)
- [Public API and host responsibilities](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/public-api.md)
- [Runnable gRPC key-value cluster example](https://github.com/algorithm-apprentice/dotnet-raft/tree/main/examples/DotnetRaft.KvCluster)
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
- **Available example:** three-process ASP.NET Core gRPC key-value cluster
- **Available storage:** durable SQLite Raft log, hard state, snapshots, and
  restart support through `DotnetRaft.Sqlite`

The example remains intentionally educational, but now persists both Raft and
application state with SQLite, supports crash/restart recovery, request
deduplication, snapshots, compaction, transport, and read-index barriers.
