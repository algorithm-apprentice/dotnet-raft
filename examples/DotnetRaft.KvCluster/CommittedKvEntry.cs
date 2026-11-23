namespace DotnetRaft.Examples.KvCluster;

// A null mutation advances the physical cursor without changing any KV row.
internal readonly record struct CommittedKvEntry(
    ulong Index,
    (KvCommand Command, KvCommandFingerprint Fingerprint)? Mutation);
