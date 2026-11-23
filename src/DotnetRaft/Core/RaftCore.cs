using System.Security.Cryptography;

using DotnetRaft.ConfChange;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Read;
using DotnetRaft.Storage;
using DotnetRaft.Tracker;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Core;

internal sealed partial class RaftCore
{
    private readonly RaftClock clock;
    private readonly RaftOutput output = new();
    private readonly ProposalAdmission proposals;
    private readonly ReadIndexCoordinator reads;
    private readonly RaftRoleState roleState = new();
    private readonly IRaftTraceSink? traceSink;
    private readonly bool suppressTransitionTrace = true;
    private ProgressTracker? configurationForOutstandingApplication;

    internal RaftCore(RaftConfig config)
        : this(
            config,
            maximum => RandomNumberGenerator.GetInt32(maximum))
    {
    }

    internal RaftCore(
        RaftConfig config,
        Func<int, int> randomOffset)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(randomOffset);

        ValidatedRaftConfig validated = config.ValidateAndNormalize();
        clock = new RaftClock(
            validated.ElectionTick,
            validated.HeartbeatTick,
            randomOffset);

        Id = validated.Id;
        MaxMessageSize = validated.MaxSizePerMessage;
        proposals = new ProposalAdmission(validated.Proposals);
        Election = validated.Election;
        StepDownOnRemoval = validated.StepDownOnRemoval;
        Logger = validated.Logger;
        traceSink = validated.TraceSink;

        Log = new RaftLog(
            validated.Storage,
            Logger,
            validated.MaxCommittedSizePerReady);

        Storage.StorageState? storageState =
            validated.Storage.GetInitialState();
        if (storageState is null)
        {
            throw new RaftInvariantException(
                "Storage returned a null initial state.");
        }

        if (storageState.ConfState is null)
        {
            throw new RaftInvariantException(
                "Storage returned a null configuration state.");
        }

        ConfState persistedConfState =
            storageState.ConfState.Clone();

        Tracker = new ProgressTracker(
            validated.MaxInflightMessages,
            validated.MaxInflightBytes);
        reads = new ReadIndexCoordinator(
            validated.ReadOnlyOption);

        ConfigurationChangeResult restored =
            ConfigurationRestore.Restore(
                new ConfigurationChanger(
                    Tracker,
                    Log.LastIndex),
                persistedConfState);
        Tracker.Install(
            restored.Config,
            restored.Progress);

        if (storageState.HardState is { } persisted && persisted != default)
        {
            LoadHardState(persisted);
        }

        if (validated.Applied > 0)
        {
            Log.AppliedTo(validated.Applied, 0);
        }

        BecomeFollower(Term, RaftMessageTargets.None);
        suppressTransitionTrace = false;
        LogInformation(
            $"New Raft {Id:x} [term: {Term}, commit: {Log.Committed}, " +
            $"applied: {Log.Applied}, last index: {Log.LastIndex}].");
        Trace(RaftTraceEventType.Initialized);
    }

    internal ulong Id { get; }

    internal ulong Term =>
        roleState.Term;

    internal ulong Vote =>
        roleState.Vote;

    internal ulong LeaderId =>
        roleState.LeaderId;

    internal RaftRole Role =>
        roleState.Role;

    internal RaftLog Log { get; }

    internal ProgressTracker Tracker { get; }

    internal IRaftLogger Logger { get; }

    internal ulong MaxMessageSize { get; }

    internal ElectionPolicy Election { get; }

    internal ProposalPolicy ProposalPolicy => proposals.Policy;

    internal bool StepDownOnRemoval { get; }

    internal ulong LeaderTransferee =>
        roleState.LeaderTransferee;

    internal ulong PendingConfigurationIndex =>
        proposals.PendingConfigurationIndex;

    internal ulong UncommittedSize =>
        proposals.UncommittedSize;

    internal ReadOnlyOption ReadOnlyOption =>
        reads.Option;

    internal bool Promotable =>
        Tracker.Contains(Id)
        && !Tracker.IsLearner(Id)
        && !Log.HasUnstableSnapshot;

    internal SoftState SoftState =>
        roleState.GetSoftState();

    internal RaftHardState HardState =>
        roleState.GetHardState(Log.Committed);

    internal bool HasMessages =>
        output.HasMessages;

    internal bool HasMessagesAfterAppend =>
        output.HasMessagesAfterAppend;

    internal bool HasReadStates =>
        reads.HasReadStates;

    internal int GatedReadCountForTesting =>
        reads.GatedCount;

    internal void Step(Message message)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (message.Term == 0
            && MessageClassifier.IsVoteMessage(message.Type))
        {
            throw new RaftInvariantException(
                $"{message.Type} must carry a nonzero term.");
        }

        Dispatch(message, new PreparedInput(ReadAcknowledgement: PrepareReadAcknowledgement(message, fromPeer: false)));
    }

    // Kept private so a caller cannot pair a message with unrelated parsed data.
    private void Dispatch(Message message, in PreparedInput input)
    {
        TraceMessage(
            RaftTraceEventType.MessageReceived,
            message);

        if (message.Term > Term)
        {
            if (ShouldIgnoreElectionRequestWithinLease(
                    message))
            {
                return;
            }

            if (!IsHigherTermPreVoteException(message))
            {
                ulong leaderId = IsLeaderMessage(message.Type)
                    ? message.From
                    : RaftMessageTargets.None;
                BecomeFollower(message.Term, leaderId);
            }
        }
        else if (message.Term != 0 && message.Term < Term)
        {
            HandleLowerTermMessage(message);
            return;
        }

        switch (message.Type)
        {
            case MessageType.MsgVote:
            case MessageType.MsgPreVote:
                HandleVoteRequest(message);
                return;
            case MessageType.MsgStorageAppendResp:
                HandleStorageAppendResponse(
                    message,
                    acknowledgeEntries: true);
                return;
            case MessageType.MsgStorageApplyResp:
                HandleStorageApplyResponse(message);
                return;
            default:
                HandleRoleMessage(message, in input);
                return;
        }
    }

    internal void StepPeerMessage(Message message, Action accept)
    {
        bool handlesLeaderMessages = HandlesLeaderMessage(message);
        NetworkMessageValidation.ValidatePeerFields(
            message,
            Log.LastIndex,
            handlesLeaderMessages,
            HandlesLeaderResponse(message));
        PreparedInput input = message.Type switch
        {
            MessageType.MsgApp => new(Append: LogSlice.ParseOwnedPeerAppend(message, Log.Format)),
            MessageType.MsgProp => new(Proposal: ParsedProposal.Parse(message.Entries, fromPeer: true, Log.Format)),
            MessageType.MsgSnap when handlesLeaderMessages => new(Snapshot: PrepareSnapshot(message.Snapshot, fromPeer: true)),
            MessageType.MsgHeartbeatResp => new(ReadAcknowledgement: PrepareReadAcknowledgement(message, fromPeer: true)),
            _ => default,
        };

        // Activate the facade only after peer input is known to be usable.
        accept();
        Dispatch(message, in input);
    }

    private bool HandlesLeaderResponse(Message message) =>
        Role == RaftRole.Leader && (message.Term == 0 || message.Term == Term);

    private ReadAcknowledgement PrepareReadAcknowledgement(Message message, bool fromPeer)
    {
        return message.Type == MessageType.MsgHeartbeatResp
            && !message.Context.IsEmpty
            && HandlesLeaderResponse(message)
            && Tracker.Contains(message.From)
                ? reads.ParseAcknowledgement(message.Context, fromPeer).GetValueOrDefault()
                : default;
    }

    private bool HandlesLeaderMessage(
        Message message)
    {
        if (message.Term != 0
            && message.Term < Term)
        {
            return false;
        }

        return Role != RaftRole.Leader
            || message.Term > Term;
    }

    internal void Send(Message message)
    {
        Message outbound = output.Enqueue(
            message,
            Id,
            Term);
        TraceMessage(
            RaftTraceEventType.MessageSent,
            outbound);
    }

    internal Message[] TakeMessages()
    {
        return output.TakeMessages();
    }

    internal Message[] PeekMessages()
    {
        return output.PeekMessages();
    }

    internal Message[] TakeMessagesAfterAppend()
    {
        return output.TakeMessagesAfterAppend();
    }

    internal Message[] PeekMessagesAfterAppend()
    {
        return output.PeekMessagesAfterAppend();
    }

    internal void ClearReadyOutputs()
    {
        output.Clear();
        reads.ClearReadStates();
    }

    internal void Bootstrap(
        IReadOnlyList<Peer> peers)
    {
        ArgumentNullException.ThrowIfNull(peers);
        BecomeFollower(1, RaftMessageTargets.None);

        var entries = new Entry[peers.Count];
        for (var offset = 0; offset < peers.Count; offset++)
        {
            Peer peer = peers[offset];
            var change = new ProtocolConfChange
            {
                Type =
                    ConfChangeType.ConfChangeAddNode,
                NodeId = peer.Id,
            };
            if (!peer.Context.IsEmpty)
            {
                change.Context = peer.Context;
            }

            entries[offset] = new Entry
            {
                Type = EntryType.EntryConfChange,
                Term = 1,
                Index = (ulong)offset + 1,
                Data = Log.Format.EncodeConfigurationChange(change),
            };
        }

        Log.Append(entries);
        TraceEntriesAppended(entries);
        ulong previousCommit = Log.Committed;
        Log.CommitTo((ulong)entries.Length);
        TraceCommitAdvanced(previousCommit);

        foreach (Peer peer in peers)
        {
            ApplyConfigurationChange(
                new ProtocolConfChange
                {
                    Type =
                        ConfChangeType.ConfChangeAddNode,
                    NodeId = peer.Id,
                });
        }
    }

    internal void SetRandomizedElectionTimeoutForTesting(
        int timeout)
    {
        clock.SetRandomizedElectionTimeoutForTesting(
            timeout);
    }

    internal void SetClockElapsedForTesting(
        int electionElapsed,
        int heartbeatElapsed = 0)
    {
        clock.SetElapsedForTesting(
            electionElapsed,
            heartbeatElapsed);
    }

    internal (
        int ElectionTick,
        int HeartbeatTick,
        int ElectionElapsed,
        int HeartbeatElapsed,
        int RandomizedElectionTimeout)
        GetClockStateForTesting()
    {
        return (
            clock.ElectionTick,
            clock.HeartbeatTick,
            clock.ElectionElapsed,
            clock.HeartbeatElapsed,
            clock.RandomizedElectionTimeout);
    }

    internal int GetReadOnlyPendingCountForTesting()
    {
        return reads.PendingCount;
    }

    internal void SetLeaderTransfereeForTesting(
        ulong transferee)
    {
        roleState.StartTransfer(transferee);
    }

    internal void SetPendingConfigurationIndexForTesting(
        ulong index)
    {
        proposals.SetPendingConfigurationIndex(
            index);
    }

    internal void SetUncommittedSizeForTesting(
        ulong size)
    {
        proposals.SetUncommittedSizeForTesting(
            size);
    }

    internal void AddReadOnlyRequestForTesting(
        ulong commitIndex,
        Message request)
    {
        reads.AddRequestForTesting(
            commitIndex,
            request);
    }

    internal void ReduceUncommittedSize(ulong payloadSize)
    {
        proposals.Release(payloadSize);
    }

    internal void AppliedTo(
        ulong index,
        ulong encodedSize)
    {
        ulong applied = Math.Max(
            index,
            Log.Applied);
        Log.AppliedTo(applied, encodedSize);
        AfterApplicationAdvanced();
    }

    private static bool IsLeaderMessage(MessageType type)
    {
        return type is
            MessageType.MsgApp or
            MessageType.MsgHeartbeat or
            MessageType.MsgSnap;
    }

    private static bool IsHigherTermPreVoteException(
        Message message)
    {
        return message.Type == MessageType.MsgPreVote
            || (message.Type == MessageType.MsgPreVoteResp
                && !message.Reject);
    }

    private void HandleRoleMessage(Message message, in PreparedInput input)
    {
        // Step may have changed the role while processing the message term.
        switch (Role)
        {
            case RaftRole.Follower:
                StepFollower(message, in input);
                return;
            case RaftRole.PreCandidate:
                StepCampaigning(message, in input, preCandidate: true);
                return;
            case RaftRole.Candidate:
                StepCampaigning(message, in input, preCandidate: false);
                return;
            case RaftRole.Leader:
                StepLeader(message, in input);
                return;
            default:
                throw new RaftInvariantException(
                    $"Unknown Raft role {Role}.");
        }
    }

    private void StepFollower(Message message, in PreparedInput input)
    {
        switch (message.Type)
        {
            case MessageType.MsgHup:
                HandleHup();
                return;
            case MessageType.MsgProp:
                HandleFollowerProposal(message, input.Proposal);
                return;
            case MessageType.MsgForgetLeader:
                HandleFollowerForgetLeader();
                return;
            case MessageType.MsgTransferLeader:
                HandleFollowerTransferLeader(message);
                return;
            case MessageType.MsgTimeoutNow:
                HandleHup(CampaignType.Transfer);
                return;
            case MessageType.MsgReadIndex:
                HandleFollowerReadIndex(message);
                return;
            case MessageType.MsgReadIndexResp:
                HandleReadIndexResponse(message);
                return;
            case MessageType.MsgApp:
            case MessageType.MsgHeartbeat:
            case MessageType.MsgSnap:
                clock.ResetElectionElapsed();
                roleState.SetLeader(message.From);
                HandleAcceptedLeaderMessage(message, in input);
                return;
        }
    }

    private void StepCampaigning(Message message, in PreparedInput input, bool preCandidate)
    {
        switch (message.Type)
        {
            case MessageType.MsgHup:
                HandleHup();
                return;
            case MessageType.MsgProp:
                if (input.Proposal is null)
                {
                    ParsedProposal.RequireEntriesForSyntheticProposal(message);
                }

                throw new ProposalDroppedException(
                    $"A {Role} cannot process proposals.");
            case MessageType.MsgApp:
            case MessageType.MsgHeartbeat:
            case MessageType.MsgSnap:
                BecomeFollower(Term, message.From);
                HandleAcceptedLeaderMessage(message, in input);
                return;
            case MessageType.MsgPreVoteResp when preCandidate:
            case MessageType.MsgVoteResp when !preCandidate:
                HandleVoteResponse(message, preCandidate);
                return;
        }
    }

    private void StepLeader(Message message, in PreparedInput input)
    {
        switch (message.Type)
        {
            case MessageType.MsgProp:
                HandleLeaderProposal(message, input.Proposal);
                return;
            case MessageType.MsgBeat:
                BroadcastHeartbeat();
                return;
            case MessageType.MsgCheckQuorum:
                HandleCheckQuorum();
                return;
            case MessageType.MsgTransferLeader:
                HandleLeaderTransfer(message.From);
                return;
            case MessageType.MsgAppResp:
                HandleAppendResponse(message);
                return;
            case MessageType.MsgHeartbeatResp:
                HandleHeartbeatResponse(message.From, input.ReadAcknowledgement);
                return;
            case MessageType.MsgReadIndex:
                HandleLeaderReadIndex(message);
                return;
            case MessageType.MsgUnreachable:
                HandleUnreachable(message);
                return;
            case MessageType.MsgSnapStatus:
                HandleSnapshotStatus(message);
                return;
        }
    }

    private void HandleAcceptedLeaderMessage(
        Message message,
        in PreparedInput input)
    {
        switch (message.Type)
        {
            case MessageType.MsgApp:
                HandleAppendEntries(message, input.Append);
                return;
            case MessageType.MsgHeartbeat:
                HandleHeartbeat(message);
                return;
            case MessageType.MsgSnap:
                HandleSnapshot(message, input.Snapshot);
                return;
            default:
                throw new RaftInvariantException(
                    $"Unexpected leader message {message.Type}.");
        }
    }

    private void HandleLowerTermMessage(Message message)
    {
        if (message.Type
            == MessageType.MsgStorageAppendResp)
        {
            HandleStorageAppendResponse(
                message,
                acknowledgeEntries: false);
            return;
        }

        if ((Election.CheckQuorum || Election.PreVote)
            && message.Type is
                MessageType.MsgApp or
                MessageType.MsgHeartbeat)
        {
            Send(new Message
            {
                To = message.From,
                Type = MessageType.MsgAppResp,
            });
            return;
        }

        if (message.Type == MessageType.MsgPreVote)
        {
            Send(new Message
            {
                To = message.From,
                Term = Term,
                Type = MessageType.MsgPreVoteResp,
                Reject = true,
            });
        }
    }

    private void HandleStorageAppendResponse(
        Message message,
        bool acknowledgeEntries)
        => AcknowledgeStableWork(
            acknowledgeEntries && message.Index != 0 ? new EntryId(message.LogTerm, message.Index) : null,
            message.Snapshot?.Metadata?.Index);

    internal void CompleteStorageAppend(ulong issuedTerm, EntryId? stableEntry, ulong? snapshotIndex)
    {
        // An older term's write cannot stabilize a new log suffix. Its
        // installed snapshot can still be acknowledged at its own index.
        AcknowledgeStableWork(issuedTerm == 0 || issuedTerm == Term ? stableEntry : null, snapshotIndex);
    }

    private void AcknowledgeStableWork(EntryId? stableEntry, ulong? snapshotIndex)
    {
        if (stableEntry is { } entry)
        {
            Log.StableTo(entry);
        }

        if (snapshotIndex is { } index)
        {
            AcknowledgeSnapshot(index);
        }
    }

    private void HandleStorageApplyResponse(
        Message message)
    {
        if (message.Entries.Count == 0)
        {
            return;
        }

        Entry[] entries =
        [
            .. message.Entries.Select(
                entry => entry.Clone()),
        ];
        CompleteStorageApply(entries);
    }

    internal void CompleteStorageApply(Entry[] entries)
    {
        AppliedTo(
            entries[^1].Index,
            EntrySizing.EncodedSize(Log.Format, entries));
        ReduceUncommittedSize(
            EntrySizing.PayloadSize(entries));
    }

    private void Reset(ulong term)
    {
        if (term < Term)
        {
            throw new RaftInvariantException(
                $"Raft term cannot decrease from {Term} to {term}.");
        }

        clock.Reset();

        roleState.Reset(term);
        Tracker.ResetVotes();

        ulong lastIndex = Log.LastIndex;
        ulong next = lastIndex + 1;
        Tracker.Visit((id, progress) =>
        {
            progress.Reset(
                id == Id ? lastIndex : 0,
                next);
        });

        proposals.Reset();
        reads.Reset();
    }

    private void LoadHardState(RaftHardState state)
    {
        ulong commit = state.Commit;
        if (commit < Log.Committed || commit > Log.LastIndex)
        {
            throw new RaftInvariantException(
                $"Persisted commit {commit} is outside " +
                $"[{Log.Committed}, {Log.LastIndex}].");
        }

        Log.CommitTo(commit);
        roleState.Load(
            state.Term,
            state.Vote);
    }

    private readonly record struct PreparedInput(
        ReadAcknowledgement ReadAcknowledgement = default,
        LogSlice? Append = null,
        ParsedProposal? Proposal = null,
        IncomingSnapshot? Snapshot = null);
}
