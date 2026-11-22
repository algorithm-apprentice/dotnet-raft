# The host integration guide has moved

Start at [Host integration](integration/README.md) to choose the node API and
storage mode. The contract now follows the order in which a service configures,
runs, and recovers a node.

<!-- Keep old heading links usable without maintaining another copy of the contract. -->
| Topic | Current guide |
|---|---|
| <a name="architecture-boundary"></a>Architecture boundary | [Where the library stops](integration/README.md#where-the-library-stops) |
| <a name="durable-sqlite-storage"></a>Durable SQLite storage | [SQLite package guide](../src/DotnetRaft.Sqlite/README.md) |
| <a name="publishing-a-storage-batch"></a>Publishing a storage batch | [Atomic publication](integration/storage.md#publishing-a-storage-batch) |
| <a name="storage-reads"></a>Storage reads | [Storage reads](integration/storage.md#storage-reads) |
| <a name="configuration"></a>Configuration | [Configuration](integration/node.md#configuration) |
| <a name="rawnode"></a>RawNode | [RawNode](integration/node.md#rawnode) |
| <a name="raftnode"></a>RaftNode | [RaftNode](integration/node.md#raftnode) |
| <a name="synchronous-ready-loop"></a>Synchronous Ready loop | [Ready / Advance](integration/ready.md#synchronous-ready-loop) |
| <a name="asynchronous-storage-writes"></a>Asynchronous storage writes | [Asynchronous storage](integration/ready.md#asynchronous-storage-writes) |
| <a name="append-worker"></a>Append worker | [Append worker](integration/ready.md#append-worker) |
| <a name="apply-worker"></a>Apply worker | [Apply worker](integration/ready.md#apply-worker) |
| <a name="call-completion-and-client-results"></a>Call completion and client results | [Client results](integration/application.md#call-completion-and-client-results) |
| <a name="configuration-entries"></a>Configuration entries | [Configuration entries](integration/application.md#configuration-entries) |
| <a name="application-progress"></a>Application progress | [Application progress](integration/application.md#application-progress) |
| <a name="read-index"></a>Read index | [Read barriers](integration/application.md#read-index) |
| <a name="network-transport"></a>Network transport | [Peer messages](integration/node.md#network-transport) |
| <a name="leadership-transfer"></a>Leadership transfer | [Leadership transfer](integration/node.md#leadership-transfer) |
| <a name="snapshots-and-compaction"></a>Snapshots and compaction | [Snapshots and compaction](integration/recovery.md#snapshots-and-compaction) |
| <a name="restart"></a>Restart | [Restart](integration/recovery.md#restart) |
| <a name="fault-handling"></a>Fault handling | [Fault handling](integration/recovery.md#fault-handling) |
| <a name="status-descriptions-and-tracing"></a>Status, descriptions, and tracing | [Status and callbacks](integration/node.md#status-descriptions-and-tracing) |
