# dotnet-raft

`dotnet-raft` is a deterministic Raft consensus library for building replicated
state machines in .NET. It elects a leader, agrees on an ordered log, and
delivers committed commands and read barriers. Your service owns networking,
timers, storage publication, and the application state those commands change.

## Start here

From a checkout with the .NET 10 SDK, run three in-memory voters in one process:

```bash
dotnet run --project examples/DotnetRaft.Counter
```

One proposal increments all three replicas. The program prints when the
proposal is submitted and when each replica has actually applied it. Try the
[same counter with separate append and apply queues](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/examples/DotnetRaft.Counter/README.md#asynchronous-storage-writes)
to see commitment move ahead of application, or
[trace one write](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/examples/DotnetRaft.Counter/README.md#trace-one-write)
to see which acknowledgement advances the commit index.

## Documentation

| I want to… | Start here |
|---|---|
| Run a cluster and observe replication or failover | [Runnable examples](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/examples/README.md) |
| Learn why Raft works | [Learning path](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/learning/README.md) |
| Embed a node in a service | [Host integration](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/integration/README.md) |
| Understand the code or make a change | [Implementation map](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/internals/README.md) and [maintainer guide](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/maintaining/README.md) |

## Install

The packages target `net10.0`. Version `3.0.0` is built and verified by CI but
is not published on NuGet.org. Follow the
[local package installation guide](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/integration/install.md)
to use it in another project. The consensus package has no protobuf dependency;
add [the protobuf adapter](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/src/DotnetRaft.Protobuf/README.md)
for the existing Raft wire format. Add the optional
[SQLite package](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/src/DotnetRaft.Sqlite/README.md)
for durable Raft storage; your application state is still yours to persist.

## Scope

The library is not a database or a transport. The
[durable KV example](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/examples/DotnetRaft.KvCluster/README.md)
shows how to compose it with gRPC and two SQLite databases on a local machine.
It is a fixed-membership teaching service, not a deployment template.

[Documentation map](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/README.md) ·
[Apache-2.0 license](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/LICENSE) ·
[Third-party notices](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/THIRD-PARTY-NOTICES.md)
