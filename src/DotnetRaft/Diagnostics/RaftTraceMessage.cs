using DotnetRaft.Protocol;

namespace DotnetRaft.Diagnostics;

public readonly record struct RaftTraceMessage(
    MessageType Type,
    ulong From,
    ulong To,
    ulong Term,
    ulong LogTerm,
    ulong Index,
    ulong Commit,
    ulong Vote,
    bool Reject,
    ulong RejectHint,
    int EntryCount,
    ulong SnapshotIndex);
