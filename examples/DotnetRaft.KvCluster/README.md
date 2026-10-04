# dotnet-raft gRPC KV cluster

This is an educational three-process host for `DotnetRaft`. It demonstrates:

- `RaftNode` logical ticks and Ready processing;
- plaintext HTTP/2 gRPC transport;
- configuration-entry application;
- durable SQLite Raft and application state;
- process restart, replay, snapshots, and compaction;
- durable request deduplication;
- proposal completion after local application; and
- linearizable reads through read index.

It is still an educational fixed-membership database. There is no TLS,
authentication, dynamic membership, MVCC, transaction API, watch, lease,
sharding, or deployment automation.

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
  -d '{"value":"blue","requestId":"11111111-2222-3333-4444-555555555555"}'
```

Safely retryable mutations must supply and reuse the same request ID. Omitting
it asks the server to generate an ID, but an unknown timeout outcome is then
not safely retryable.

Delete:

```bash
curl -X DELETE \
  'http://127.0.0.1:7102/kv/color?requestId=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
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

## Durable files and restart

When `Raft:DataDirectory` is omitted, each process uses:

```text
./data/node-<NodeId>/
  raft.db
  application.db
```

The three example commands can be stopped and run again with the same
arguments. Data, request results, Raft term/vote/log, application cursor, and
configuration are recovered.

Use explicit directories when running multiple copies from different working
directories:

```bash
--Raft:DataDirectory=/absolute/path/node-1
```

Do not delete or copy one database independently of the other. Live SQLite
databases must be backed up through SQLite-aware tooling.

## Snapshot policy

`Raft:SnapshotThresholdEntries` defaults to `1000`. Bootstrap configuration is
reconciled immediately, and later application snapshots are created after the
configured number of applied entries. Raft log compaction occurs only after
the matching durable application snapshot exists.

`Raft:MaxTransportMessageBytes` defaults to 64 MiB and configures Kestrel HTTP
request bodies, gRPC, and Raft entry batching. Commands larger than the
supported envelope are rejected with HTTP 413. If an application snapshot
exceeds the limit, the host retains the prior snapshot and log suffix instead
of compacting into an undeliverable recovery point, and waits another snapshot
threshold interval before retrying serialization.

## Automated real-process smoke

The smoke requires `curl`, `jq`, and `python3`. Ports `17101`-`17103` and
`17201`-`17203` must be unused. From the repository root:

```bash
dotnet build examples/DotnetRaft.KvCluster -c Release
./eng/smoke-durable-kv-cluster.sh "$PWD"
```

The smoke starts three Kestrel processes, writes through a follower, takes a
follower offline while replicating and snapshotting a 31 MiB value, verifies
HTTP ingestion beyond Kestrel's former default and large-snapshot catch-up,
kills the leader with SIGKILL, continues with the remaining quorum, restarts
the old leader from the same directory, and finally restarts the complete
cluster. It waits for each active node to observe the elected leader and
retries only transient mutation failures with the same durable request ID.
Any failure prints all node logs before cleanup.
