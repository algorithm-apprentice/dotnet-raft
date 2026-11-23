# DotnetRaft.Protobuf

The protobuf adapter for [DotnetRaft](https://github.com/algorithm-apprentice/dotnet-raft).
Use `ProtobufRaftCodec.Instance` to encode peer messages and durable values, or
to supply the log format to `MemoryStorage`. The adapter preserves the existing
`raftpb` wire schema; the consensus package does not depend on it.
