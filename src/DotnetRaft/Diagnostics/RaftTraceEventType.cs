namespace DotnetRaft.Diagnostics;

public enum RaftTraceEventType
{
    Initialized,
    BecameFollower,
    BecamePreCandidate,
    BecameCandidate,
    BecameLeader,
    CommitAdvanced,
    EntriesAppended,
    ConfigurationProposed,
    ConfigurationApplied,
    ReadyAccepted,
    MessageSent,
    MessageReceived,
}
