# dotnet-raft gRPC KV cluster

This is an educational three-process host for `DotnetRaft`. It demonstrates:

- `RaftNode` logical ticks and Ready processing;
- plaintext HTTP/2 gRPC transport;
- configuration-entry application;
- a replicated in-memory key-value state machine;
- proposal completion after local application; and
- linearizable reads through read index.

It is **not** a production database. Storage and application state are lost on
restart. There is no TLS, authentication, dynamic membership, WAL, compaction,
or snapshot generation.

## Run three nodes

Open three terminals from the repository root.

Node 1:

```bash
dotnet run --project examples/DotnetRaft.KvCluster -- \
  --Raft:NodeId=1 --Raft:HttpPort=7101 --Raft:GrpcPort=7201
```

Node 2:

```bash
dotnet run --project examples/DotnetRaft.KvCluster -- \
  --Raft:NodeId=2 --Raft:HttpPort=7102 --Raft:GrpcPort=7202
```

Node 3:

```bash
dotnet run --project examples/DotnetRaft.KvCluster -- \
  --Raft:NodeId=3 --Raft:HttpPort=7103 --Raft:GrpcPort=7203
```

All three commands use the peer map in `appsettings.json`. Wait a few seconds
for election, or trigger one manually:

```bash
curl -X POST http://127.0.0.1:7101/campaign
```

## Write and read

Propose through any node:

```bash
curl -X PUT http://127.0.0.1:7102/kv/color \
  -H 'content-type: application/json' \
  -d '{"value":"blue"}'
```

Linearizable read through any node:

```bash
curl http://127.0.0.1:7103/kv/color
```

Potentially stale local read:

```bash
curl http://127.0.0.1:7103/local/color
```

Status:

```bash
curl http://127.0.0.1:7101/status
```

The status response reports Raft's logical applied index separately from the
state machine's physical applied index.
