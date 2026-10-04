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

        Snapshot? pending =
            raftStorage
                .GetPendingApplicationSnapshot();
        if (pending is not null)
        {
            if (application.WasCreated)
            {
                throw new InvalidDataException(
                    "Pending Raft snapshot cannot restore a recreated application database.");
            }

            RestoreAndAcknowledge(
                raftStorage,
                application,
                pending);
            retained = raftStorage.GetSnapshot();
            hardState = raftStorage.GetHardState();
        }

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

        ulong applied =
            application.PhysicalApplied;
        if (!raftFresh
            && applied == 0
            && retained.Metadata.Index == 0
            && raftStorage.GetFirstIndex() != 1)
        {
            throw new InvalidDataException(
                "Application replay from zero is impossible because the Raft log prefix was compacted without a restorable snapshot.");
        }

        membership.ValidateRecoveredConfiguration(
            applied,
            application.ConfState);
        ulong lastIndex =
            raftStorage.GetLastIndex();
        if (applied > lastIndex)
        {
            throw new InvalidDataException(
                $"Application index {applied} exceeds Raft last index {lastIndex}.");
        }

        if (!raftFresh)
        {
            hardState =
                raftStorage.GetHardState()
                ?? throw new InvalidDataException(
                    "Nonempty Raft storage has no HardState.");
            if (hardState.Commit < applied)
            {
                ulong compacted =
                    raftStorage.GetFirstIndex() - 1;
                if (applied < compacted
                    || applied > lastIndex)
                {
                    throw new InvalidDataException(
                        $"Raft storage does not cover application index {applied}.");
                }

                hardState.Commit = applied;
                raftStorage.SetHardState(
                    hardState);
            }
        }

        retained = raftStorage.GetSnapshot();
        if (applied > retained.Metadata.Index)
        {
            if (hardState is null
                || hardState.Commit < applied)
            {
                throw new InvalidDataException(
                    $"HardState does not cover application index {applied}.");
            }

            ByteString snapshotData =
                application.CreateSnapshotData();
            if (snapshotData.Length
                <= maximumSnapshotBytes)
            {
                raftStorage.CreateSnapshot(
                    applied,
                    application.ConfState,
                    snapshotData);
                ulong compacted =
                    raftStorage.GetFirstIndex() - 1;
                if (compacted < applied)
                {
                    raftStorage.Compact(applied);
                }

                retained = raftStorage.GetSnapshot();
            }
        }

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

        return new DurableRecoveryState(
            StartNew: raftFresh,
            Applied: applied);
    }

    private static void RestoreAndAcknowledge(
        SqliteStorage raftStorage,
        SqliteKeyValueStateMachine application,
        Snapshot snapshot)
    {
        application.Restore(
            snapshot.Metadata.Index,
            snapshot.Data,
            snapshot.Metadata.ConfState);
        HardState hardState =
            raftStorage.GetHardState()
            ?? throw new InvalidDataException(
                "Pending Raft snapshot has no durable HardState.");
        ulong lastTerm =
            raftStorage.GetTerm(
                raftStorage.GetLastIndex());
        ulong requiredTerm = Math.Max(
            snapshot.Metadata.Term,
            lastTerm);
        if (hardState.Term < requiredTerm)
        {
            throw new InvalidDataException(
                $"HardState term {hardState.Term} is below retained term {requiredTerm}.");
        }

        if (hardState.Commit
            < snapshot.Metadata.Index)
        {
            hardState.Commit =
                snapshot.Metadata.Index;
            raftStorage.SetHardState(
                hardState);
        }

        raftStorage.AcknowledgeApplicationSnapshot(
            snapshot.Metadata.Index);
    }
}
