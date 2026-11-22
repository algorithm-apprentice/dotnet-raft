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

This host uses `RaftNode` for concurrent calls and the default Ready/Advance
storage protocol. Start with the [replicated counter](../DotnetRaft.Counter/README.md)
if you want to see the host loop without gRPC and SQLite. For this example,
start reading at `ProcessReadyAsync` in [`RaftClusterHost`](RaftClusterHost.cs):
Raft provides ordered committed entries and read barriers, while this host
owns transport, the application database, and the client's success response.
The [fresh-write walkthrough](../../docs/internals/kv-host.md#follow-a-fresh-kv-write)
connects the HTTP response to the application transaction. The
[host internals](../../docs/internals/kv-host.md) explain how
application effects, request results, and recovery fit together.

It is still an educational fixed-membership database. There is no TLS,
authentication, dynamic membership, MVCC, transaction API, watch, lease,
sharding, or deployment automation.

Both listeners bind to loopback only. Raft message validation rejects malformed
protocol input but does not authenticate `Message.From` or authorize clients.
Do not expose this host outside a trusted local teaching environment.

## Run three nodes

Use the .NET 10 SDK and open three terminals from the repository root. The
HTTP ports `7101`–`7103` and gRPC ports `7201`–`7203` must be free. To use
fresh storage for a lab run, add a different `--Raft:DataDirectory=...` to
each command and keep those arguments for subsequent restarts.

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

All three commands use the peer map in `appsettings.json`. Wait for an
election, then check that the nodes report the same nonzero `leaderId`:

```bash
for port in 7101 7102 7103; do
  curl -fsS "http://127.0.0.1:$port/status"
  printf '\n'
done
```

If there is no leader, you can request a campaign. HTTP 202 means the host
processed the request, not that node 1 won the election:

```bash
curl -X POST http://127.0.0.1:7101/campaign
```

## Write and read

Send the write to any ready node. To observe forwarding, choose a port
whose `/status` has `nodeId` different from the agreed `leaderId`. In the
commands below, replace `7102` with that follower's port:

```bash
write_port=7102
curl -fsS -X PUT "http://127.0.0.1:$write_port/kv/color" \
  -H 'content-type: application/json' \
  -d '{"value":"blue","requestId":"11111111-2222-3333-4444-555555555555"}'
```

Safely retryable mutations must supply and reuse the same request ID. Omitting
it asks the server to generate an ID, but an unknown timeout outcome is then
not safely retryable. Reuse the ID **with the same command** to get the original
result; reuse with different content returns HTTP 409. On a repeat lab run
with the same data, the example IDs may already exist; choose fresh IDs to
observe new writes.

The example retains every unique request ID permanently, including in application
snapshots. There is no retention window or pruning protocol, so storage grows
with unique mutation history.

Linearizable read through any node:

```bash
curl http://127.0.0.1:7103/kv/color
```

Potentially stale local read:

```bash
curl http://127.0.0.1:7103/local/color
```

Optional cleanup: delete the key with a distinct request ID. Skip this step
if you want to keep the value for the quorum-loss experiment:

```bash
curl -fsS -X DELETE \
  'http://127.0.0.1:7102/kv/color?requestId=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
```

The status response reports Raft's logical applied index separately from the
state machine's physical applied index.

## Durable files and restart

When `Raft:DataDirectory` is omitted, the host resolves this path relative to
its process working directory, which need not be the shell's launch directory:

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

The HTTP root endpoint (`/`) reports the resolved `dataDirectory`. Check it
before stopping or restarting a node if you are unsure which files it uses.

Treat both databases as one recovery set. Pending-snapshot recovery handles
process crashes, but startup cannot prove that every arbitrary independently
captured pair belongs together. The simple supported backup procedure stops
the node and copies its complete data directory as one unit. Do not delete,
replace, copy, or restore either database independently; this example does not
implement coordinated online backup.

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

## Observe quorum loss

Use the running demo only. Before stopping any nodes, get a successful PUT
on the node you intend to leave running. Save its port, key, body, and
`appliedIndex`. The simplest setup is to leave the original write target
running; it has already applied the request locally.

1. Stop the other two demo processes with `Ctrl-C`. Do not change any of
   the data directories or peer maps.
2. Repeat the exact successful PUT on the survivor. It can return HTTP 200
   with `duplicate: true` and the original `appliedIndex` from its durable
   request table. This is not a new commit.
3. Send a PUT with a **new** request ID and issue a new `GET /kv/color`.
   With no majority, neither can complete as a new quorum operation; a
   timeout commonly produces HTTP 504. `GET /local/color` may still return
   previously applied data and is explicitly not linearizable.
4. Restart one stopped process with the same arguments. Wait until the
   two running nodes agree on a leader, then try a fresh write and read.

If you retry a write whose outcome was unknown, keep its original ID and
command. Do not infer from a timeout that the write never committed.

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
