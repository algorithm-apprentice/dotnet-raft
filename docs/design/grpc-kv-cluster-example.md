# gRPC Key-Value Cluster Example Design

Historical design, partially superseded by [ADR 0007](../adr/0007-durable-fixed-membership-kv-host.md).
The [runbook](../../examples/DotnetRaft.KvCluster/README.md) and
[durable host design](durable-grpc-kv-host.md) describe the current example.
The [design archive](README.md#completed-reference-port) has the original.

[ADR 0004](../adr/0004-educational-grpc-kv-cluster.md) first used a volatile
three-process host to teach consensus with real transport. Its lasting
choice is that the gRPC envelope carries the generated Raft message; it
does not define a second protocol schema. The example still owns timers,
transport, and application effects outside the consensus library. In-memory
state and lack of restart support were constraints of the original example,
not of the current host.
