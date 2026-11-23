namespace DotnetRaft.Examples.KvCluster;

public sealed record KeyValueReadResult(
    bool Found,
    string? Value,
    ulong PhysicalApplied);

public enum KvCommandType
{
    Set = 1,
    Delete = 2,
}

public sealed record KvCommand(
    Guid RequestId,
    KvCommandType Type,
    string Key,
    string? Value);

public sealed record KvApplyResult(
    Guid RequestId,
    KvCommandFingerprint Fingerprint,
    ulong ResultIndex,
    ulong PhysicalApplied,
    bool Duplicate,
    bool Conflict)
{
    // Fingerprint always identifies the durable winner. A conflicting
    // attempt also implies that winner had already been recorded.
    internal KvApplyResult ForRequest(KvCommandFingerprint expectedFingerprint)
    {
        bool matches = Fingerprint.Equals(expectedFingerprint);
        return this with
        {
            Duplicate = matches && (Duplicate || Conflict),
            Conflict = !matches,
        };
    }
}

public sealed record KvRequestResolution(
    ulong ResultIndex,
    bool Conflict);
