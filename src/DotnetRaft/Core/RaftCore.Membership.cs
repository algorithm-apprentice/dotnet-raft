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

        try
        {
            ProposeConfiguration(new ConfChangeV2());
        }
        catch (ProposalDroppedException exception)
        {
            LogDebug(
                $"Automatic joint exit remains pending: {exception.Message}");
        }
    }

    internal void ProposeConfiguration(ProtocolConfChange owned) =>
        ProposeConfiguration(new Entry { Type = EntryType.EntryConfChange, Data = owned.ToByteString() }, owned.AsV2());

    internal void ProposeConfiguration(ConfChangeV2 owned) =>
        ProposeConfiguration(new Entry { Type = EntryType.EntryConfChangeV2, Data = owned.ToByteString() }, owned);

    private void ProposeConfiguration(Entry entry, ConfChangeV2 change)
    {
        var proposal = new Message { From = Id, To = Id, Type = MessageType.MsgProp };
        proposal.Entries.Add(entry);
        Dispatch(proposal, new PreparedInput(Proposal: ParsedProposal.FromLocalConfiguration(entry, change)));
    }

    private IncomingSnapshot PrepareSnapshot(Snapshot? snapshot, bool fromPeer)
    {
        if (snapshot?.Metadata is not { } metadata
            || metadata.Index <= Log.Committed
            || metadata.ConfState is not { } state
            || !ContainsLocalNode(state))
        {
            return IncomingSnapshot.Ignored;
        }

        ulong index = metadata.Index;
        if (Log.MatchTerm(new EntryId(metadata.Term, index)))
        {
            return IncomingSnapshot.Matching(index);
        }

        if (index == ulong.MaxValue)
        {
            string message = $"Snapshot index {index} has no representable successor.";
            throw fromPeer
                ? new ArgumentException(message, nameof(snapshot))
                : new RaftInvariantException(message);
        }

        try
        {
            return IncomingSnapshot.Install(new SnapshotInstallation(snapshot, BuildSnapshotConfiguration(metadata)));
        }
        catch (ConfigurationChangeException exception) when (fromPeer)
        {
            throw new ArgumentException(
                $"Snapshot configuration is invalid: {exception.Message}",
                nameof(snapshot),
                exception);
        }
    }

    private void HandleSnapshot(Message message, IncomingSnapshot? prepared)
    {
        IncomingSnapshot snapshot = prepared ?? PrepareSnapshot(message.Snapshot, fromPeer: false);
        ulong before = Log.Committed;
        if (snapshot.Installation is { } installation)
        {
            InstallIncomingSnapshot(installation.Snapshot, installation.Configuration);
        }
        else if (snapshot.CommitThrough is ulong commitThrough)
        {
            Log.CommitTo(commitThrough);
        }

        TraceCommitAdvanced(before);
        Send(new Message
        {
            To = message.From,
            Type = MessageType.MsgAppResp,
            Index = Log.Committed,
        });
    }

    private void InstallIncomingSnapshot(Snapshot snapshot, ConfigurationChangeResult restored)
    {
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
        Log.CommitTo(snapshot.Metadata.Index);
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

    private sealed record SnapshotInstallation(Snapshot Snapshot, ConfigurationChangeResult Configuration);

    private sealed class IncomingSnapshot
    {
        internal static IncomingSnapshot Ignored { get; } = new(null, null);

        private IncomingSnapshot(ulong? commitThrough, SnapshotInstallation? installation)
        {
            CommitThrough = commitThrough;
            Installation = installation;
        }

        // Neither value is set for an ignored snapshot.
        internal ulong? CommitThrough { get; }
        internal SnapshotInstallation? Installation { get; }

        internal static IncomingSnapshot Matching(ulong index) => new(index, null);
        internal static IncomingSnapshot Install(SnapshotInstallation installation) => new(null, installation);
    }
}
