using DotnetRaft.Protocol;
using DotnetRaft.Storage.Sqlite;

using Google.Protobuf;

namespace DotnetRaft.Examples.KvCluster;

public sealed record DurableRecoveryState(
    bool StartNew,
    ulong Applied);

public static class DurableHostRecovery
{
    public static SqliteKeyValueStateMachine
        OpenApplication(
        SqliteStorage raftStorage,
        string databasePath,
        ulong nodeId)
    {
        ArgumentNullException.ThrowIfNull(
            raftStorage);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            databasePath);
        bool raftFresh = IsRaftFresh(raftStorage);
        bool applicationExists =
            File.Exists(
                Path.GetFullPath(databasePath));
        if (!applicationExists
            && !raftFresh)
        {
            throw new InvalidDataException(
                "Application database is missing beside nonempty Raft storage.");
        }

        return new SqliteKeyValueStateMachine(
            databasePath,
            nodeId,
            createIfMissing: raftFresh);
    }

    public static bool IsRaftFresh(
        SqliteStorage raftStorage)
    {
        ArgumentNullException.ThrowIfNull(
            raftStorage);
        Snapshot retained =
            raftStorage.GetSnapshot();
        return retained.Metadata.Index == 0
            && raftStorage.GetLastIndex() == 0
            && raftStorage.GetHardState() is null;
    }

    public static DurableRecoveryState Reconcile(
        SqliteStorage raftStorage,
        SqliteKeyValueStateMachine application,
        FixedMembershipConfiguration membership,
        int maximumSnapshotBytes =
            int.MaxValue)
    {
        ArgumentNullException.ThrowIfNull(
            raftStorage);
        ArgumentNullException.ThrowIfNull(
            application);
        ArgumentNullException.ThrowIfNull(
            membership);

        Snapshot retained =
            raftStorage.GetSnapshot();
        HardState? hardState =
            raftStorage.GetHardState();
        bool raftFresh =
            IsRaftFresh(raftStorage);
        ValidateDatabasePair(application, raftFresh);
        if (RestorePendingSnapshot(raftStorage, application))
        {
            retained = raftStorage.GetSnapshot();
            hardState = raftStorage.GetHardState();
        }

        RestoreRetainedSnapshot(application, retained);
        ulong applied = application.PhysicalApplied;
        ValidateReplayStart(raftStorage, retained, raftFresh, applied);
        membership.ValidateRecoveredConfiguration(applied, application.ConfState);
        ulong lastIndex = raftStorage.GetLastIndex();
        ValidateApplicationBound(applied, lastIndex);
        if (!raftFresh)
        {
            hardState = RepairCommitFromApplication(raftStorage, applied, lastIndex);
        }

        retained = raftStorage.GetSnapshot();
        if (applied > retained.Metadata.Index)
        {
            retained = SnapshotRecoveredApplication(raftStorage, application, retained, hardState, applied, maximumSnapshotBytes);
        }

        ValidateRecoveryPoint(raftStorage, application, retained, applied);
        return new DurableRecoveryState(StartNew: raftFresh, Applied: applied);
    }

    private static void ValidateDatabasePair(SqliteKeyValueStateMachine application, bool raftFresh)
    {
        if (application.WasCreated
            && !raftFresh)
        {
            throw new InvalidDataException(
                "Application database was recreated beside nonempty Raft storage.");
        }

        if (raftFresh
            && application.PhysicalApplied != 0)
        {
            throw new InvalidDataException(
                "Application state is nonempty while Raft storage is empty.");
        }
    }

    private static bool RestorePendingSnapshot(SqliteStorage raftStorage, SqliteKeyValueStateMachine application)
    {
        Snapshot? pending =
            raftStorage
                .GetPendingApplicationSnapshot();
        if (pending is null)
        {
            return false;
        }

        if (application.WasCreated)
        {
            throw new InvalidDataException("Pending Raft snapshot cannot restore a recreated application database.");
        }

        CompletePendingSnapshot(raftStorage, application, pending, recovering: true);
        return true;
    }

    private static void RestoreRetainedSnapshot(SqliteKeyValueStateMachine application, Snapshot retained)
    {
        if (application.PhysicalApplied
            < retained.Metadata.Index)
        {
            if (application.WasCreated)
            {
                throw new InvalidDataException(
                    "Retained Raft snapshot cannot restore a recreated application database.");
            }

            application.Restore(
                retained.Metadata.Index,
                retained.Data,
                retained.Metadata.ConfState);
        }

    }

    private static void ValidateReplayStart(SqliteStorage raftStorage, Snapshot retained, bool raftFresh, ulong applied)
    {
        if (!raftFresh
            && applied == 0
            && retained.Metadata.Index == 0
            && raftStorage.GetFirstIndex() != 1)
        {
            throw new InvalidDataException(
                "Application replay from zero is impossible because the Raft log prefix was compacted without a restorable snapshot.");
        }

    }

    private static void ValidateApplicationBound(ulong applied, ulong lastIndex)
    {
        if (applied > lastIndex)
        {
            throw new InvalidDataException(
                $"Application index {applied} exceeds Raft last index {lastIndex}.");
        }

    }

    private static HardState RepairCommitFromApplication(SqliteStorage raftStorage, ulong applied, ulong lastIndex)
    {
        HardState hardState = raftStorage.GetHardState()
            ?? throw new InvalidDataException("Nonempty Raft storage has no HardState.");
        if (hardState.Commit < applied)
        {
            ulong compacted = raftStorage.GetFirstIndex() - 1;
            if (applied < compacted || applied > lastIndex)
            {
                throw new InvalidDataException($"Raft storage does not cover application index {applied}.");
            }

            hardState.Commit = applied;
            raftStorage.SetHardState(hardState);
        }

        return hardState;
    }

    private static Snapshot SnapshotRecoveredApplication(
        SqliteStorage raftStorage,
        SqliteKeyValueStateMachine application,
        Snapshot retained,
        HardState? hardState,
        ulong applied,
        int maximumSnapshotBytes)
    {
        if (hardState is null || hardState.Commit < applied)
        {
            throw new InvalidDataException($"HardState does not cover application index {applied}.");
        }

        ByteString snapshotData = application.CreateSnapshotData();
        if (snapshotData.Length <= maximumSnapshotBytes)
        {
            PublishSnapshotAndCompact(raftStorage, applied, application.ConfState, snapshotData);
            retained = raftStorage.GetSnapshot();
        }

        return retained;
    }

    internal static bool PublishSnapshotAndCompact(
        SqliteStorage raftStorage, ulong applied, ConfState configuration, ByteString snapshotData)
    {
        // Shared by live operation and recovery. The snapshot must be durable
        // first; a crash between these writes leaves a snapshot plus extra log,
        // never a deleted prefix without its recovery point.
        raftStorage.CreateSnapshot(applied, configuration, snapshotData);
        ulong compacted = raftStorage.GetFirstIndex() - 1;
        if (compacted >= applied)
        {
            return false;
        }

        raftStorage.Compact(applied);
        return true;
    }

    private static void ValidateRecoveryPoint(
        SqliteStorage raftStorage, SqliteKeyValueStateMachine application, Snapshot retained, ulong applied)
    {
        ulong retainedPrefix =
            raftStorage.GetFirstIndex() - 1;
        if (retained.Metadata.Index > applied
            || retainedPrefix
                > retained.Metadata.Index
            || !retained.Metadata.ConfState.Equals(
                application.ConfState))
        {
            throw new InvalidDataException(
                "Raft snapshot does not match recovered application state.");
        }

    }

    internal static void CompletePendingSnapshot(
        SqliteStorage raftStorage,
        SqliteKeyValueStateMachine application,
        Snapshot snapshot,
        bool recovering)
    {
        SnapshotMetadata metadata = snapshot.Metadata;
        application.Restore(
            metadata.Index,
            snapshot.Data,
            metadata.ConfState);
        HardState hardState =
            raftStorage.GetHardState()
            ?? throw new InvalidDataException(
                recovering
                    ? "Pending Raft snapshot has no durable HardState."
                    : "Persisted snapshot has no HardState.");
        // PersistReady has already validated the live batch. Recovery must
        // verify the retained terms before using the physical snapshot to
        // repair commit; snapshot metadata cannot supply term or vote.
        if (recovering)
        {
            ValidateSnapshotTermCoverage(raftStorage, metadata, hardState);
        }

        RepairSnapshotCommit(raftStorage, metadata, hardState);
        raftStorage.AcknowledgeApplicationSnapshot(metadata.Index);
    }

    private static void ValidateSnapshotTermCoverage(SqliteStorage raftStorage, SnapshotMetadata metadata, HardState hardState)
    {
        ulong lastTerm = raftStorage.GetTerm(raftStorage.GetLastIndex());
        ulong requiredTerm = Math.Max(metadata.Term, lastTerm);
        if (hardState.Term < requiredTerm)
        {
            throw new InvalidDataException($"HardState term {hardState.Term} is below retained term {requiredTerm}.");
        }
    }

    private static void RepairSnapshotCommit(SqliteStorage raftStorage, SnapshotMetadata metadata, HardState hardState)
    {
        if (hardState.Commit < metadata.Index)
        {
            hardState.Commit = metadata.Index;
            raftStorage.SetHardState(hardState);
        }

    }
}
