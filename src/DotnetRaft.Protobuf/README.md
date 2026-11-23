# DotnetRaft.Protobuf

The protobuf adapter for [DotnetRaft](https://github.com/algorithm-apprentice/dotnet-raft).
Use `ProtobufRaftCodec.EncodeMessage` and `DecodeMessage` for peer traffic,
and `ProtobufRaftCodec.Instance` as the log format for `MemoryStorage`.
Generated wire types are internal to the adapter; the consensus package has
no protobuf dependency.

## Binary compatibility

The adapter now generates its wire types from `proto3` but keeps the same
`raftpb` binary encoding. The pre-migration and migrated .NET builds can
read each other's peer messages and persisted Raft values without format
negotiation or a data rewrite. Raw descriptors did change; this is not a
compatibility promise for generated source or ProtoJSON in other toolchains.
See the [migration decision and verification](https://github.com/algorithm-apprentice/dotnet-raft/blob/main/docs/adr/0012-use-proto3-for-raftpb.md).
