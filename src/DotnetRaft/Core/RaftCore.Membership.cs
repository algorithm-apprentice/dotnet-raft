using DotnetRaft.ConfChange;
using DotnetRaft.Protocol;
using DotnetRaft.Tracker;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Core;

internal sealed partial class RaftCore
{
    internal ConfState ApplyConfigurationChange(
        ProtocolConfChange change)
    {
        ArgumentNullException.ThrowIfNull(change);
        return ApplyConfigurationChange(change.AsV2());
    }

    internal ConfState ApplyConfigurationChange(
        ConfChangeV2 change)
    {
        ArgumentNullException.ThrowIfNull(change);
        ConfChangeV2 owned = change.Clone();
        ProgressTracker? earlier = configurationForOutstandingApplication;
        var changer = new ConfigurationChanger(
            earlier ?? Tracker,
            earlier is null ? Log.LastIndex : Log.Applying);
        ConfigurationChangeResult result;
        if (owned.IsLeaveJoint())
        {
            result = changer.LeaveJoint();
        }
        else if (owned.TryGetJointTransition(
                     out bool autoLeave))
        {
            result = changer.EnterJoint(
                autoLeave,
                owned.Changes);
        }
        else
        {
            result = changer.Simple(owned.Changes);
        }

        if (earlier is not null)
        {
            // This entry was handed to the host before the newer snapshot.
            // Return the configuration at its application position without
            // changing the live consensus tracker installed by the snapshot.
            earlier.Install(result.Config, result.Progress);
            ConfState applied = earlier.ToConfState();
            TraceConfigurationApplied(applied);
            return applied;
        }

        return SwitchToConfiguration(
            result.Config,
            result.Progress);
    }

    private ConfState SwitchToConfiguration(
        TrackerConfig config,
        ProgressMap progress)
    {
        Tracker.Install(config, progress);

        ConfState state = Tracker.ToConfState();
        bool hasLocalProgress =
            Tracker.Contains(Id);
        TraceConfigurationApplied(state);

        if ((!hasLocalProgress
                || Tracker.IsLearner(Id))
            && Role == RaftRole.Leader)
        {
            if (StepDownOnRemoval)
            {
                BecomeFollower(
                    Term,
                    RaftMessageTargets.None);
            }
            else
            {
                ReevaluateReadOnlyAfterConfigurationChange();
            }

            return state;
        }

        if (Role != RaftRole.Leader
            || state.Voters.Count == 0)
        {
            return state;
        }

        ReevaluateReadOnlyAfterConfigurationChange();
        if (MaybeCommit())
        {
            BroadcastAppend();
        }
        else
        {
            Tracker.Visit((id, peerProgress) =>
            {
                if (id != Id)
                {
                    MaybeSendAppend(
                        id,
                        peerProgress,
                        sendIfEmpty: false);
                }
            });
        }

        if (LeaderTransferee !=
                RaftMessageTargets.None
            && !Tracker.Config.Voters.Ids()
                .Contains(LeaderTransferee))
        {
            roleState.AbortTransfer();
        }

        return state;
    }

    private void MaybeAutoLeave()
    {
        if (!Tracker.Config.AutoLeave
            || !proposals.IsPendingApplied(
                Log.Applied)
            || Role != RaftRole.Leader)
        {
            return;
        }

        var proposal = new Message
        {
            From = Id,
            To = Id,
            Type = MessageType.MsgProp,
        };
        proposal.Entries.Add(new Entry
        {
            Type = EntryType.EntryConfChangeV2,
            Data = new ConfChangeV2()
                .ToByteString(),
        });
        try
        {
            Step(proposal);
        }
        catch (ProposalDroppedException exception)
        {
            LogDebug(
                $"Automatic joint exit remains pending: {exception.Message}");
        }
    }

    private void ValidateNetworkSnapshot(
        Snapshot? snapshot)
    {
        Snapshot normalized =
            ProtocolDefaults.EnsureSnapshot(
                snapshot?.Clone());
        ulong snapshotIndex =
            normalized.Metadata.Index;
        ConfState state =
            normalized.Metadata.ConfState;
        if (snapshotIndex <= Log.Committed
            || !ContainsLocalNode(state))
        {
            return;
        }

        var snapshotEntry = new EntryId(
            Term: normalized.Metadata.Term,
            Index: snapshotIndex);
        if (Log.MatchTerm(snapshotEntry))
        {
            return;
        }

        if (snapshotIndex == ulong.MaxValue)
        {
            throw new ArgumentException(
                $"Snapshot index {ulong.MaxValue} has no representable successor.",
                nameof(snapshot));
        }

        try
        {
            _ = BuildSnapshotConfiguration(normalized.Metadata);
        }
        catch (ConfigurationChangeException exception)
        {
            throw new ArgumentException(
                $"Snapshot configuration is invalid: {exception.Message}",
                nameof(snapshot),
                exception);
        }
    }

    private void HandleSnapshot(Message message)
    {
        Snapshot snapshot = ProtocolDefaults.EnsureSnapshot(
            message.Snapshot?.Clone());
        bool restored = RestoreSnapshot(snapshot);
        Send(new Message
        {
            To = message.From,
            Type = MessageType.MsgAppResp,
            Index = restored
                ? Log.LastIndex
                : Log.Committed,
        });
    }

    private bool RestoreSnapshot(Snapshot snapshot)
    {
        ulong snapshotIndex = snapshot.Metadata.Index;
        if (snapshotIndex <= Log.Committed)
        {
            return false;
        }

        if (Role != RaftRole.Follower)
        {
            if (Term == ulong.MaxValue)
            {
                throw new RaftInvariantException(
                    "Raft term overflow while rejecting a snapshot outside follower state.");
            }

            BecomeFollower(
                Term + 1,
                RaftMessageTargets.None);
            return false;
        }

        ConfState state = snapshot.Metadata.ConfState;
        if (!ContainsLocalNode(state))
        {
            return false;
        }

        var snapshotEntry = new EntryId(
            Term: snapshot.Metadata.Term,
            Index: snapshotIndex);
        if (Log.MatchTerm(snapshotEntry))
        {
            ulong previousCommit = Log.Committed;
            Log.CommitTo(snapshotIndex);
            TraceCommitAdvanced(previousCommit);
            return false;
        }

        ConfigurationChangeResult restored =
            BuildSnapshotConfiguration(snapshot.Metadata);

        ulong restoredFromCommit = Log.Committed;
        Log.RestoreUncommitted(snapshot);
        if (Log.Applying > Log.Applied
            && configurationForOutstandingApplication is null)
        {
            // No new committed entries are emitted while the snapshot is
            // unstable. Retain the old tracker state only to apply the
            // already delivered prefix, including joint transitions.
            var earlier = new ProgressTracker(
                Tracker.MaxInflightMessages,
                Tracker.MaxInflightBytes);
            earlier.Install(Tracker.Config, Tracker.Progress);
            configurationForOutstandingApplication = earlier;
        }

        InstallSnapshotConfiguration(restored);
        Log.CommitTo(snapshotIndex);
        TraceCommitAdvanced(restoredFromCommit);
        return true;
    }

    private bool ContainsLocalNode(ConfState state)
    {
        return state.Voters.Contains(Id)
            || state.Learners.Contains(Id)
            || state.VotersOutgoing.Contains(Id);
    }

    private ConfigurationChangeResult BuildSnapshotConfiguration(
        SnapshotMetadata metadata)
    {
        var scratch = new ProgressTracker(
            Tracker.MaxInflightMessages,
            Tracker.MaxInflightBytes);
        return ConfigurationRestore.Restore(
            new ConfigurationChanger(scratch, metadata.Index),
            metadata.ConfState);
    }

    private void InstallSnapshotConfiguration(ConfigurationChangeResult restored)
    {
        Tracker.Install(
            restored.Config,
            restored.Progress);
        Tracker.ResetVotes();
        TraceConfigurationApplied(
            Tracker.ToConfState());
    }

    internal void AcknowledgeSnapshot(ulong index)
    {
        if (Log.HasUnstableSnapshotAt(index))
        {
            // Only the matching owned snapshot can restore membership at
            // completion. Do not rely on the detached Ready or reply.
            SnapshotMetadata metadata = Log.GetSnapshot().Metadata;
            if (!metadata.ConfState.IsEquivalentTo(Tracker.ToConfState()))
            {
                InstallSnapshotConfiguration(
                    BuildSnapshotConfiguration(metadata));
            }
        }

        Log.AcknowledgeSnapshot(index);
        AfterApplicationAdvanced();
    }

    private void AfterApplicationAdvanced()
    {
        if (Log.Applying == Log.Applied)
        {
            configurationForOutstandingApplication = null;
        }

        MaybeAutoLeave();
    }
}
