using System.Security.Cryptography;

using DotnetRaft.ConfChange;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Quorum;
using DotnetRaft.Read;
using DotnetRaft.Storage;
using DotnetRaft.Tracker;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Core;

internal sealed class RaftCore
{
    private static readonly ByteString CampaignTransferContext =
        ByteString.CopyFromUtf8("CampaignTransfer");
    private readonly RaftClock clock;
    private readonly List<Message> messages = [];
    private readonly List<Message> messagesAfterAppend = [];
    private readonly Queue<Message> pendingReadIndexMessages = [];
    private readonly List<ReadState> readStates = [];
    private readonly IRaftTraceSink? traceSink;
    private readonly bool suppressTransitionTrace = true;

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
        AsyncStorageWrites = validated.AsyncStorageWrites;
        MaxMessageSize = validated.MaxSizePerMessage;
        MaxCommittedSizePerReady =
            validated.MaxCommittedSizePerReady;
        MaxUncommittedEntriesSize =
            validated.MaxUncommittedEntriesSize;
        CheckQuorum = validated.CheckQuorum;
        PreVote = validated.PreVote;
        DisableProposalForwarding =
            validated.DisableProposalForwarding;
        DisableConfChangeValidation =
            validated.DisableConfChangeValidation;
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

        HardState? persistedHardState =
            storageState.HardState?.Clone();
        ConfState persistedConfState =
            storageState.ConfState.Clone();

        Tracker = new ProgressTracker(
            validated.MaxInflightMessages,
            validated.MaxInflightBytes);
        ReadOnly = new ReadOnlyTracker(validated.ReadOnlyOption);

        ConfigurationChangeResult restored =
            ConfigurationRestore.Restore(
                new ConfigurationChanger(
                    Tracker,
                    Log.LastIndex),
                persistedConfState);
        Tracker.Install(
            restored.Config,
            restored.Progress);

        if (!IsEmpty(persistedHardState))
        {
            LoadHardState(persistedHardState!);
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

    internal ulong Term { get; private set; }

    internal ulong Vote { get; private set; }

    internal ulong LeaderId { get; private set; }

    internal RaftRole Role { get; private set; }

    internal bool IsLearner =>
        Tracker.IsLearner(Id);

    internal RaftLog Log { get; }

    internal ProgressTracker Tracker { get; }

    internal ReadOnlyTracker ReadOnly { get; private set; }

    internal IRaftLogger Logger { get; }

    internal int ElectionTick =>
        clock.ElectionTick;

    internal int HeartbeatTick =>
        clock.HeartbeatTick;

    internal int ElectionElapsed
    {
        get => clock.ElectionElapsed;
        set => clock.ElectionElapsed = value;
    }

    internal int HeartbeatElapsed
    {
        get => clock.HeartbeatElapsed;
        set => clock.HeartbeatElapsed = value;
    }

    internal int RandomizedElectionTimeout =>
        clock.RandomizedElectionTimeout;

    internal bool AsyncStorageWrites { get; }

    internal ulong MaxMessageSize { get; }

    internal ulong MaxCommittedSizePerReady { get; }

    internal ulong MaxUncommittedEntriesSize { get; }

    internal bool CheckQuorum { get; }

    internal bool PreVote { get; }

    internal bool DisableProposalForwarding { get; }

    internal bool DisableConfChangeValidation { get; }

    internal bool StepDownOnRemoval { get; }

    internal ulong LeaderTransferee { get; set; }

    internal ulong PendingConfigurationIndex { get; set; }

    internal ulong UncommittedSize { get; set; }

    internal bool Promotable =>
        Tracker.Contains(Id)
        && !Tracker.IsLearner(Id)
        && !Log.HasUnstableSnapshot;

    internal bool PastElectionTimeout =>
        clock.PastElectionTimeout;

    internal SoftState SoftState => new(LeaderId, Role);

    internal HardState HardState => new()
    {
        Term = Term,
        Vote = Vote,
        Commit = Log.Committed,
    };

    internal bool HasMessages => messages.Count > 0;

    internal bool HasMessagesAfterAppend =>
        messagesAfterAppend.Count > 0;

    internal bool HasReadStates => readStates.Count > 0;

    internal int PendingReadIndexMessageCount =>
        pendingReadIndexMessages.Count;

    internal void BecomeFollower(ulong term, ulong leaderId)
    {
        Reset(term);
        LeaderId = leaderId;
        Role = RaftRole.Follower;
        if (!suppressTransitionTrace)
        {
            Trace(RaftTraceEventType.BecameFollower);
        }

        LogInformation(
            $"{Id:x} became follower at term {Term}.");
    }

    internal void BecomeCandidate()
    {
        if (Role == RaftRole.Leader)
        {
            throw new RaftInvariantException(
                "Invalid transition from leader to candidate.");
        }

        if (Term == ulong.MaxValue)
        {
            throw new RaftInvariantException(
                "Raft term overflow while becoming candidate.");
        }

        Reset(Term + 1);
        Vote = Id;
        Role = RaftRole.Candidate;
        Trace(RaftTraceEventType.BecameCandidate);
        LogInformation(
            $"{Id:x} became candidate at term {Term}.");
    }

    internal void BecomePreCandidate()
    {
        if (Role == RaftRole.Leader)
        {
            throw new RaftInvariantException(
                "Invalid transition from leader to pre-candidate.");
        }

        Tracker.ResetVotes();
        LeaderId = RaftMessageTargets.None;
        Role = RaftRole.PreCandidate;
        Trace(RaftTraceEventType.BecamePreCandidate);
        LogInformation(
            $"{Id:x} became pre-candidate at term {Term}.");
    }

    internal void BecomeLeader()
    {
        if (Role == RaftRole.Follower)
        {
            throw new RaftInvariantException(
                "Invalid transition from follower to leader.");
        }

        if (!Tracker.Progress.TryGetValue(
                Id,
                out Progress? localProgress))
        {
            throw new RaftInvariantException(
                $"Cannot become leader without local progress for {Id:x}.");
        }

        ulong lastIndex = Log.LastIndex;
        if (lastIndex >= ulong.MaxValue - 1)
        {
            throw new RaftInvariantException(
                $"Cannot append a leader no-op after index {lastIndex} with a representable successor.");
        }

        Reset(Term);
        LeaderId = Id;
        Role = RaftRole.Leader;

        localProgress.BecomeReplicate();
        localProgress.RecentActive = true;
        PendingConfigurationIndex = lastIndex;
        Trace(RaftTraceEventType.BecameLeader);
        if (!AppendLeaderEntries([new Entry()]))
        {
            throw new RaftInvariantException(
                "The leader no-op cannot be rejected by the uncommitted-size quota.");
        }

        LogInformation(
            $"{Id:x} became leader at term {Term}.");
    }

    internal bool TickElectionClock()
    {
        if (Role == RaftRole.Leader)
        {
            throw new RaftInvariantException(
                "Leader state must use leader clocks.");
        }

        return clock.TickElection(Promotable);
    }

    internal LeaderClockTick TickLeaderClocks()
    {
        if (Role != RaftRole.Leader)
        {
            throw new RaftInvariantException(
                "Only a leader can tick leader clocks.");
        }

        return clock.TickLeader();
    }

    internal void TickElection()
    {
        if (!TickElectionClock())
        {
            return;
        }

        Step(new Message
        {
            From = Id,
            To = Id,
            Type = MessageType.MsgHup,
        });
    }

    internal void TickLeader()
    {
        LeaderClockTick tick = TickLeaderClocks();
        if (tick.ElectionDue && CheckQuorum)
        {
            Step(new Message
            {
                From = Id,
                To = Id,
                Type = MessageType.MsgCheckQuorum,
            });
        }

        if (tick.ElectionDue
            && Role == RaftRole.Leader
            && LeaderTransferee !=
                RaftMessageTargets.None)
        {
            AbortLeaderTransfer();
        }

        if (Role != RaftRole.Leader
            || !tick.HeartbeatDue)
        {
            return;
        }

        Step(new Message
        {
            From = Id,
            To = Id,
            Type = MessageType.MsgBeat,
        });
    }

    internal void Step(Message message)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (message.Term == 0
            && IsVoteMessage(message.Type))
        {
            throw new RaftInvariantException(
                $"{message.Type} must carry a nonzero term.");
        }

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
            case MessageType.MsgHup:
                HandleHup();
                return;
            case MessageType.MsgVote:
            case MessageType.MsgPreVote:
                HandleVoteRequest(message);
                return;
            default:
                HandleRoleMessage(message);
                return;
        }
    }

    internal void Campaign()
    {
        Campaign(CampaignType.Election);
    }

    internal void Campaign(CampaignType campaignType)
    {
        MessageType voteType;
        ulong campaignTerm;
        ByteString context;
        switch (campaignType)
        {
            case CampaignType.PreElection:
                if (Term == ulong.MaxValue)
                {
                    throw new RaftInvariantException(
                        "Raft term overflow while starting pre-election.");
                }

                BecomePreCandidate();
                voteType = MessageType.MsgPreVote;
                campaignTerm = Term + 1;
                context = ByteString.Empty;
                break;
            case CampaignType.Election:
                BecomeCandidate();
                voteType = MessageType.MsgVote;
                campaignTerm = Term;
                context = ByteString.Empty;
                break;
            case CampaignType.Transfer:
                BecomeCandidate();
                voteType = MessageType.MsgVote;
                campaignTerm = Term;
                context = CampaignTransferContext;
                break;
            default:
                throw new RaftInvariantException(
                    $"Unknown campaign type {campaignType}.");
        }

        EntryId lastEntry = default;
        bool hasLastEntry = false;
        foreach (ulong voterId in Tracker.VoterNodes())
        {
            if (voterId == Id)
            {
                Send(new Message
                {
                    To = voterId,
                    Term = campaignTerm,
                    Type =
                        MessageClassifier.VoteResponseType(
                            voteType),
                });
                continue;
            }

            if (!hasLastEntry)
            {
                lastEntry = Log.LastEntryId;
                hasLastEntry = true;
            }

            Send(new Message
            {
                To = voterId,
                Term = campaignTerm,
                Type = voteType,
                Index = lastEntry.Index,
                LogTerm = lastEntry.Term,
                Context = context,
            });
        }
    }

    internal void Send(Message message)
    {
        ArgumentNullException.ThrowIfNull(message);
        Message outbound = message.Clone();

        if (outbound.From == RaftMessageTargets.None)
        {
            outbound.From = Id;
        }

        if (IsVoteMessage(outbound.Type))
        {
            if (outbound.Term == 0)
            {
                throw new RaftInvariantException(
                    $"Term must be set when sending {outbound.Type}.");
            }
        }
        else
        {
            if (outbound.Term != 0)
            {
                throw new RaftInvariantException(
                    $"Term must not be set when sending {outbound.Type}.");
            }

            if (outbound.Type is not MessageType.MsgProp
                and not MessageType.MsgReadIndex)
            {
                outbound.Term = Term;
            }
        }

        if (RequiresDurableState(outbound.Type))
        {
            messagesAfterAppend.Add(outbound);
            TraceMessage(
                RaftTraceEventType.MessageSent,
                outbound);
            return;
        }

        if (outbound.To == Id)
        {
            throw new RaftInvariantException(
                $"Immediate outbound {outbound.Type} cannot target the local node.");
        }

        messages.Add(outbound);
        TraceMessage(
            RaftTraceEventType.MessageSent,
            outbound);
    }

    internal Message[] TakeMessages()
    {
        return Take(messages);
    }

    internal Message[] PeekMessages()
    {
        return CloneMessages(messages);
    }

    internal Message[] TakeMessagesAfterAppend()
    {
        return Take(messagesAfterAppend);
    }

    internal Message[] PeekMessagesAfterAppend()
    {
        return CloneMessages(messagesAfterAppend);
    }

    internal ReadState[] TakeReadStates()
    {
        return Take(readStates);
    }

    internal ReadState[] PeekReadStates()
    {
        return
        [
            .. readStates.Select(
                state => new ReadState(
                    state.Index,
                    state.RequestContext)),
        ];
    }

    internal BasicStatus GetBasicStatus()
    {
        return new BasicStatus(
            Id,
            Term,
            Vote,
            Log.Committed,
            LeaderId,
            Role,
            Log.Applied,
            LeaderTransferee);
    }

    internal ConfigurationStatus GetConfigurationStatus()
    {
        ConfState state = Tracker.ToConfState();
        return new ConfigurationStatus(
            state.Voters,
            state.VotersOutgoing,
            state.Learners,
            state.LearnersNext,
            state.AutoLeave);
    }

    internal KeyValuePair<ulong, ProgressStatus>[]
        GetProgressStatuses()
    {
        return
        [
            .. Tracker.Progress
                .OrderBy(pair => pair.Key)
                .Select(pair =>
                    new KeyValuePair<ulong, ProgressStatus>(
                        pair.Key,
                        ToStatus(pair.Value))),
        ];
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
                Data = change.ToByteString(),
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

    internal void TraceReadyAccepted()
    {
        Trace(RaftTraceEventType.ReadyAccepted);
    }

    internal void SetRandomizedElectionTimeoutForTesting(
        int timeout)
    {
        clock.SetRandomizedElectionTimeoutForTesting(
            timeout);
    }

    internal void ReduceUncommittedSize(ulong payloadSize)
    {
        UncommittedSize = payloadSize >= UncommittedSize
            ? 0
            : UncommittedSize - payloadSize;
    }

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
        var changer = new ConfigurationChanger(
            Tracker,
            Log.LastIndex);
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

        return SwitchToConfiguration(
            result.Config,
            result.Progress);
    }

    private static ProgressStatus ToStatus(
        Progress progress)
    {
        return new ProgressStatus(
            progress.Match,
            progress.Next,
            progress.LastSentCommit,
            progress.State switch
            {
                ProgressState.Probe =>
                    ReplicationState.Probe,
                ProgressState.Replicate =>
                    ReplicationState.Replicate,
                ProgressState.Snapshot =>
                    ReplicationState.Snapshot,
                _ => throw new RaftInvariantException(
                    $"Unknown progress state {progress.State}."),
            },
            progress.PendingSnapshot,
            progress.RecentActive,
            progress.AppendFlowPaused,
            progress.IsPaused,
            progress.IsLearner,
            progress.Inflights.Count,
            progress.Inflights.Bytes,
            progress.Inflights.Capacity,
            progress.Inflights.MaxBytes);
    }

    internal void AppliedTo(
        ulong index,
        ulong encodedSize)
    {
        ulong applied = Math.Max(
            index,
            Log.Applied);
        Log.AppliedTo(applied, encodedSize);
        MaybeAutoLeave();
    }

    internal void TraceSyntheticMessageSent(
        Message message)
    {
        ArgumentNullException.ThrowIfNull(message);
        TraceMessage(
            RaftTraceEventType.MessageSent,
            message);
    }

    private static bool IsEmpty(HardState? state)
    {
        return state is null
            || (state.Term == 0
                && state.Vote == 0
                && state.Commit == 0);
    }

    private static bool IsVoteMessage(MessageType type)
    {
        return type is
            MessageType.MsgVote or
            MessageType.MsgVoteResp or
            MessageType.MsgPreVote or
            MessageType.MsgPreVoteResp;
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

    private static bool RequiresDurableState(MessageType type)
    {
        return type is
            MessageType.MsgAppResp or
            MessageType.MsgVoteResp or
            MessageType.MsgPreVoteResp;
    }

    private static T[] Take<T>(List<T> queue)
    {
        T[] taken = [.. queue];
        queue.Clear();
        return taken;
    }

    private static Message[] CloneMessages(
        IEnumerable<Message> source)
    {
        return
        [
            .. source.Select(
                message => message.Clone()),
        ];
    }

    private void HandleHup()
    {
        HandleHup(
            PreVote
                ? CampaignType.PreElection
                : CampaignType.Election);
    }

    private void HandleHup(
        CampaignType campaignType)
    {
        if (Role == RaftRole.Leader
            || !Promotable
            || HasUnappliedConfigurationChanges())
        {
            return;
        }

        Campaign(campaignType);
    }

    private bool HasUnappliedConfigurationChanges()
    {
        if (Log.Applied >= Log.Committed)
        {
            return false;
        }

        ulong next = Log.Applied + 1;
        ulong high = Log.Committed + 1;
        while (next < high)
        {
            IReadOnlyList<Entry> entries = Log.Slice(
                next,
                high,
                MaxCommittedSizePerReady);
            if (entries.Count == 0)
            {
                throw new RaftInvariantException(
                    $"Configuration scan returned no entries for [{next}, {high}).");
            }

            foreach (Entry entry in entries)
            {
                if (entry.Type is
                    EntryType.EntryConfChange or
                    EntryType.EntryConfChangeV2)
                {
                    return true;
                }
            }

            next += (ulong)entries.Count;
        }

        return false;
    }

    private void HandleVoteRequest(Message message)
    {
        bool canVote =
            Vote == message.From
            || (Vote == RaftMessageTargets.None
                && LeaderId == RaftMessageTargets.None)
            || (message.Type == MessageType.MsgPreVote
                && message.Term > Term);
        bool upToDate = Log.IsUpToDate(
            new EntryId(message.LogTerm, message.Index));
        MessageType responseType =
            MessageClassifier.VoteResponseType(
                message.Type);

        if (canVote && upToDate)
        {
            Send(new Message
            {
                To = message.From,
                Term = message.Term,
                Type = responseType,
            });
            if (message.Type == MessageType.MsgVote)
            {
                ElectionElapsed = 0;
                Vote = message.From;
            }

            return;
        }

        Send(new Message
        {
            To = message.From,
            Term = Term,
            Type = responseType,
            Reject = true,
        });
    }

    private void HandleRoleMessage(Message message)
    {
        switch (message.Type)
        {
            case MessageType.MsgProp:
                HandleProposal(message);
                return;
            case MessageType.MsgStorageAppendResp:
                HandleStorageAppendResponse(
                    message,
                    acknowledgeEntries: true);
                return;
            case MessageType.MsgStorageApplyResp:
                HandleStorageApplyResponse(message);
                return;
            case MessageType.MsgBeat:
                if (Role == RaftRole.Leader)
                {
                    BroadcastHeartbeat();
                }

                return;
            case MessageType.MsgCheckQuorum:
                if (Role == RaftRole.Leader)
                {
                    HandleCheckQuorum();
                }

                return;
            case MessageType.MsgForgetLeader:
                HandleForgetLeader();
                return;
            case MessageType.MsgTransferLeader:
                HandleTransferLeader(message);
                return;
            case MessageType.MsgTimeoutNow:
                if (Role == RaftRole.Follower)
                {
                    HandleHup(CampaignType.Transfer);
                }

                return;
            case MessageType.MsgAppResp:
                if (Role == RaftRole.Leader)
                {
                    HandleAppendResponse(message);
                }

                return;
            case MessageType.MsgHeartbeatResp:
                if (Role == RaftRole.Leader)
                {
                    HandleHeartbeatResponse(message);
                }

                return;
            case MessageType.MsgReadIndex:
                HandleReadIndexMessage(message);
                return;
            case MessageType.MsgReadIndexResp:
                if (Role == RaftRole.Follower)
                {
                    HandleReadIndexResponse(message);
                }

                return;
            case MessageType.MsgUnreachable:
                if (Role == RaftRole.Leader)
                {
                    HandleUnreachable(message);
                }

                return;
            case MessageType.MsgSnapStatus:
                if (Role == RaftRole.Leader)
                {
                    HandleSnapshotStatus(message);
                }

                return;
            case MessageType.MsgApp:
            case MessageType.MsgHeartbeat:
            case MessageType.MsgSnap:
                if (!HandleLeaderMessage(message))
                {
                    return;
                }

                break;
        }

        switch (message.Type)
        {
            case MessageType.MsgApp:
                HandleAppendEntries(message);
                return;
            case MessageType.MsgHeartbeat:
                HandleHeartbeat(message);
                return;
            case MessageType.MsgSnap:
                HandleSnapshot(message);
                return;
            case MessageType.MsgVoteResp
                when Role == RaftRole.Candidate:
                HandleVoteResponse(message);
                return;
            case MessageType.MsgPreVoteResp
                when Role == RaftRole.PreCandidate:
                HandleVoteResponse(message);
                return;
        }
    }

    private bool HandleLeaderMessage(Message message)
    {
        switch (Role)
        {
            case RaftRole.Follower:
                ElectionElapsed = 0;
                LeaderId = message.From;
                return true;
            case RaftRole.PreCandidate:
            case RaftRole.Candidate:
                BecomeFollower(Term, message.From);
                return true;
            case RaftRole.Leader:
                return false;
            default:
                throw new RaftInvariantException(
                    $"Unknown Raft role {Role}.");
        }
    }

    private void HandleVoteResponse(Message message)
    {
        Tracker.RecordVote(message.From, !message.Reject);
        (_, _, VoteResult result) = Tracker.TallyVotes();
        switch (result)
        {
            case VoteResult.Won:
                if (Role == RaftRole.PreCandidate)
                {
                    Campaign(CampaignType.Election);
                }
                else
                {
                    BecomeLeader();
                    BroadcastAppend();
                }

                return;
            case VoteResult.Lost:
                BecomeFollower(Term, RaftMessageTargets.None);
                return;
            case VoteResult.Pending:
                return;
            default:
                throw new RaftInvariantException(
                    $"Unknown vote result {result}.");
        }
    }

    private void HandleProposal(Message message)
    {
        if (message.Entries.Count == 0)
        {
            throw new RaftInvariantException(
                "A proposal must contain at least one entry.");
        }

        switch (Role)
        {
            case RaftRole.Leader:
                if (!Tracker.Progress.ContainsKey(Id))
                {
                    throw new ProposalDroppedException(
                        "The leader has no local replication progress.");
                }

                if (LeaderTransferee !=
                    RaftMessageTargets.None)
                {
                    throw new ProposalDroppedException(
                        "The leader is transferring leadership.");
                }

                (
                    Entry[] entries,
                    ulong pendingConfigurationIndex) =
                    PrepareProposalEntries(
                        message.Entries);
                if (!AppendLeaderEntries(
                        entries,
                        traceConfigurationProposals: true))
                {
                    throw new ProposalDroppedException(
                        "The proposal exceeds the uncommitted entry size limit.");
                }

                PendingConfigurationIndex =
                    pendingConfigurationIndex;
                BroadcastAppend();
                return;
            case RaftRole.Follower:
                if (LeaderId == RaftMessageTargets.None)
                {
                    throw new ProposalDroppedException(
                        "The follower has no known leader.");
                }

                if (DisableProposalForwarding)
                {
                    throw new ProposalDroppedException(
                        "Proposal forwarding is disabled.");
                }

                Message forwarded = message.Clone();
                forwarded.To = LeaderId;
                Send(forwarded);
                return;
            case RaftRole.PreCandidate:
            case RaftRole.Candidate:
                throw new ProposalDroppedException(
                    $"A {Role} cannot process proposals.");
            default:
                throw new RaftInvariantException(
                    $"Unknown Raft role {Role}.");
        }
    }

    private bool AppendLeaderEntries(
        IEnumerable<Entry> entries,
        bool traceConfigurationProposals = false)
    {
        Entry[] appended = ProtocolCloning.CloneEntries(entries);
        if (appended.Length == 0)
        {
            throw new RaftInvariantException(
                "At least one leader entry is required.");
        }

        ulong lastIndex = Log.LastIndex;
        if (lastIndex == ulong.MaxValue)
        {
            throw new RaftInvariantException(
                "The log has no representable successor index.");
        }

        ulong availableIndexes =
            ulong.MaxValue - lastIndex - 1;
        if ((ulong)appended.Length > availableIndexes)
        {
            throw new RaftInvariantException(
                $"Appending {appended.Length} entries after index {lastIndex} would leave no representable successor.");
        }

        ulong payloadSize = 0;
        try
        {
            payloadSize = EntrySizing.PayloadSize(appended);
        }
        catch (OverflowException exception)
        {
            throw new RaftInvariantException(
                $"Proposal payload size overflowed: {exception.Message}");
        }

        ulong nextUncommittedSize = UncommittedSize;
        if (UncommittedSize > 0 && payloadSize > 0)
        {
            if (payloadSize > ulong.MaxValue - UncommittedSize)
            {
                return false;
            }

            nextUncommittedSize =
                UncommittedSize + payloadSize;
            if (nextUncommittedSize >
                MaxUncommittedEntriesSize)
            {
                return false;
            }
        }
        else
        {
            nextUncommittedSize = payloadSize == 0
                ? UncommittedSize
                : payloadSize;
        }

        for (var offset = 0; offset < appended.Length; offset++)
        {
            Entry entry = appended[offset];
            entry.Term = Term;
            entry.Index =
                lastIndex + (ulong)offset + 1;
        }

        if (traceConfigurationProposals)
        {
            TraceConfigurationProposals(appended);
        }

        ulong newLastIndex = Log.Append(appended);
        UncommittedSize = nextUncommittedSize;
        TraceEntriesAppended(appended);
        Send(new Message
        {
            To = Id,
            Type = MessageType.MsgAppResp,
            Index = newLastIndex,
        });
        return true;
    }

    private (
        Entry[] Entries,
        ulong PendingConfigurationIndex)
        PrepareProposalEntries(
            IEnumerable<Entry> entries)
    {
        Entry[] owned = ProtocolCloning.CloneEntries(entries);
        ulong candidatePending =
            PendingConfigurationIndex;
        ulong lastIndex = Log.LastIndex;

        for (var offset = 0; offset < owned.Length; offset++)
        {
            Entry entry = owned[offset];
            ConfChangeV2? change = entry.Type switch
            {
                EntryType.EntryConfChange =>
                    ProtocolConfChange.Parser
                        .ParseFrom(entry.Data)
                        .AsV2(),
                EntryType.EntryConfChangeV2 =>
                    ConfChangeV2.Parser.ParseFrom(
                        entry.Data),
                _ => null,
            };
            if (change is null)
            {
                continue;
            }

            bool alreadyPending =
                candidatePending > Log.Applied;
            bool alreadyJoint =
                Tracker.Config.Voters.Outgoing.Count > 0;
            bool wantsLeaveJoint =
                change.Changes.Count == 0;
            bool incompatible =
                alreadyPending
                || (alreadyJoint && !wantsLeaveJoint)
                || (!alreadyJoint && wantsLeaveJoint);
            if (incompatible
                && !DisableConfChangeValidation)
            {
                owned[offset] = new Entry();
                continue;
            }

            try
            {
                candidatePending = checked(
                    lastIndex + (ulong)offset + 1);
            }
            catch (OverflowException exception)
            {
                throw new RaftInvariantException(
                    $"Configuration entry at offset {offset} has no representable log index: {exception.Message}");
            }
        }

        return (owned, candidatePending);
    }

    private void BroadcastAppend()
    {
        Tracker.Visit((id, progress) =>
        {
            if (id != Id)
            {
                MaybeSendAppend(
                    id,
                    progress,
                    sendIfEmpty: true);
            }
        });
    }

    private bool MaybeSendAppend(
        ulong to,
        Progress progress,
        bool sendIfEmpty)
    {
        if (progress.IsPaused)
        {
            return false;
        }

        ulong previousIndex = progress.Next - 1;
        ulong previousTerm = 0;
        IReadOnlyList<Entry> entries = [];
        try
        {
            previousTerm = Log.GetTerm(previousIndex);
            entries =
                progress.State == ProgressState.Replicate
                && progress.Inflights.IsFull
                    ? []
                    : Log.GetEntries(
                        progress.Next,
                        MaxMessageSize);
        }
        catch (StorageException exception)
            when (exception.Error == StorageError.Compacted)
        {
            return MaybeSendSnapshot(to, progress);
        }

        if (entries.Count == 0 && !sendIfEmpty)
        {
            return false;
        }

        ulong payloadSize = 0;
        try
        {
            payloadSize = EntrySizing.PayloadSize(entries);
        }
        catch (OverflowException exception)
        {
            throw new RaftInvariantException(
                $"Append payload size overflowed: {exception.Message}");
        }

        var message = new Message
        {
            To = to,
            Type = MessageType.MsgApp,
            Index = previousIndex,
            LogTerm = previousTerm,
            Commit = Log.Committed,
        };
        message.Entries.Add(entries);
        Send(message);
        progress.SentEntries(entries.Count, payloadSize);
        progress.RecordSentCommit(Log.Committed);
        return true;
    }

    private void HandleAppendEntries(Message message)
    {
        if (message.Index < Log.Committed)
        {
            Send(new Message
            {
                To = message.From,
                Type = MessageType.MsgAppResp,
                Index = Log.Committed,
            });
            return;
        }

        var slice = new LogSlice(
            message.Term,
            new EntryId(
                Term: message.LogTerm,
                Index: message.Index),
            message.Entries);
        ulong previousCommit = Log.Committed;
        if (Log.MaybeAppendEntries(
                slice,
                out ulong lastNewIndex,
                out ulong firstAppendedIndex,
                out int appendedCount))
        {
            if (appendedCount > 0)
            {
                TraceEntriesAppended(
                    appendedCount,
                    firstAppendedIndex,
                    lastNewIndex);
            }

            Log.CommitTo(
                Math.Min(
                    message.Commit,
                    lastNewIndex));
            TraceCommitAdvanced(previousCommit);
            Send(new Message
            {
                To = message.From,
                Type = MessageType.MsgAppResp,
                Index = lastNewIndex,
            });
            return;
        }

        ulong hintIndex = Math.Min(
            message.Index,
            Log.LastIndex);
        EntryId hint = Log.FindConflictByTerm(
            hintIndex,
            message.LogTerm);
        Send(new Message
        {
            To = message.From,
            Type = MessageType.MsgAppResp,
            Index = message.Index,
            Reject = true,
            RejectHint = hint.Index,
            LogTerm = hint.Term,
        });
    }

    private void HandleAppendResponse(Message message)
    {
        if (!Tracker.Progress.TryGetValue(
                message.From,
                out Progress? progress))
        {
            return;
        }

        if (message.Reject)
        {
            progress.RecentActive = true;
            if (message.Index == 0)
            {
                return;
            }

            ulong nextProbeIndex = message.RejectHint;
            if (message.LogTerm > 0)
            {
                nextProbeIndex = Log.FindConflictByTerm(
                    message.RejectHint,
                    message.LogTerm).Index;
            }

            if (progress.MaybeDecrementTo(
                    message.Index,
                    nextProbeIndex))
            {
                if (progress.State ==
                    ProgressState.Replicate)
                {
                    progress.BecomeProbe();
                }

                MaybeSendAppend(
                    message.From,
                    progress,
                    sendIfEmpty: true);
            }

            return;
        }

        if (message.Index > Log.LastIndex)
        {
            throw new RaftInvariantException(
                $"Append acknowledgement {message.Index} exceeds last index {Log.LastIndex}.");
        }

        progress.RecentActive = true;
        bool updated = progress.MaybeUpdate(
            message.Index);
        if (!updated
            && !(progress.State == ProgressState.Probe
                 && progress.Match == message.Index))
        {
            return;
        }

        switch (progress.State)
        {
            case ProgressState.Probe:
                progress.BecomeReplicate();
                break;
            case ProgressState.Replicate:
                progress.Inflights.FreeThrough(
                    message.Index);
                break;
            case ProgressState.Snapshot:
                if (progress.Match >= Log.FirstIndex - 1)
                {
                    progress.BecomeProbe();
                    progress.BecomeReplicate();
                }

                break;
            default:
                throw new RaftInvariantException(
                    $"Unknown progress state {progress.State}.");
        }

        if (MaybeCommit())
        {
            BroadcastAppend();
        }
        else if (message.From != Id
                 && progress.CanBumpCommit(
                     Log.Committed))
        {
            MaybeSendAppend(
                message.From,
                progress,
                sendIfEmpty: true);
        }

        if (message.From != Id)
        {
            while (MaybeSendAppend(
                       message.From,
                       progress,
                       sendIfEmpty: false))
            {
            }
        }

        if (message.From == LeaderTransferee
            && progress.Match == Log.LastIndex)
        {
            SendTimeoutNow(message.From);
        }
    }

    private bool MaybeCommit()
    {
        ulong previousCommit = Log.Committed;
        bool committed = Log.MaybeCommit(new EntryId(
            Term: Term,
            Index: Tracker.CommittedIndex));
        if (committed)
        {
            TraceCommitAdvanced(previousCommit);
            ReleasePendingReadIndexMessages();
        }

        return committed;
    }

    private void BroadcastHeartbeat()
    {
        ByteString context =
            ReadOnly.GetHeartbeatContext();
        Tracker.Visit((id, progress) =>
        {
            if (id == Id)
            {
                return;
            }

            Send(new Message
            {
                To = id,
                Type = MessageType.MsgHeartbeat,
                Commit = Math.Min(progress.Match, Log.Committed),
                Context = context,
            });
            progress.RecordSentCommit(
                Math.Min(progress.Match, Log.Committed));
        });
    }

    private void HandleHeartbeat(Message message)
    {
        ulong previousCommit = Log.Committed;
        Log.CommitTo(message.Commit);
        TraceCommitAdvanced(previousCommit);
        Send(new Message
        {
            To = message.From,
            Type = MessageType.MsgHeartbeatResp,
            Context = message.Context,
        });
    }

    private void HandleHeartbeatResponse(Message message)
    {
        if (!Tracker.Progress.TryGetValue(
                message.From,
                out Progress? progress))
        {
            return;
        }

        progress.RecentActive = true;
        progress.AppendFlowPaused = false;
        if (progress.Match < Log.LastIndex
            || progress.State == ProgressState.Probe)
        {
            MaybeSendAppend(
                message.From,
                progress,
                sendIfEmpty: true);
        }

        if (!message.Context.IsEmpty)
        {
            ReadOnly.ReceiveAcknowledgement(
                message.From,
                message.Context);
            AdvanceReadOnly();
        }
    }

    private void HandleTransferLeader(
        Message message)
    {
        switch (Role)
        {
            case RaftRole.Follower:
                if (LeaderId == RaftMessageTargets.None)
                {
                    return;
                }

                Message forwarded = message.Clone();
                forwarded.To = LeaderId;
                Send(forwarded);
                return;
            case RaftRole.PreCandidate:
            case RaftRole.Candidate:
                return;
            case RaftRole.Leader:
                HandleLeaderTransfer(message.From);
                return;
            default:
                throw new RaftInvariantException(
                    $"Unknown Raft role {Role}.");
        }
    }

    private void HandleLeaderTransfer(
        ulong transferee)
    {
        if (!Tracker.Progress.TryGetValue(
                transferee,
                out Progress? progress)
            || progress.IsLearner)
        {
            return;
        }

        if (LeaderTransferee !=
            RaftMessageTargets.None)
        {
            if (LeaderTransferee == transferee)
            {
                return;
            }

            AbortLeaderTransfer();
        }

        if (transferee == Id)
        {
            return;
        }

        ElectionElapsed = 0;
        LeaderTransferee = transferee;
        if (progress.Match == Log.LastIndex)
        {
            SendTimeoutNow(transferee);
            return;
        }

        MaybeSendAppend(
            transferee,
            progress,
            sendIfEmpty: true);
    }

    private void SendTimeoutNow(ulong to)
    {
        Send(new Message
        {
            To = to,
            Type = MessageType.MsgTimeoutNow,
        });
    }

    private void AbortLeaderTransfer()
    {
        LeaderTransferee =
            RaftMessageTargets.None;
    }

    private void HandleReadIndexMessage(Message message)
    {
        switch (Role)
        {
            case RaftRole.Leader:
                HandleLeaderReadIndex(message);
                return;
            case RaftRole.Follower:
                if (LeaderId == RaftMessageTargets.None)
                {
                    return;
                }

                Message forwarded = message.Clone();
                forwarded.To = LeaderId;
                Send(forwarded);
                return;
            case RaftRole.PreCandidate:
            case RaftRole.Candidate:
                return;
            default:
                throw new RaftInvariantException(
                    $"Unknown Raft role {Role}.");
        }
    }

    private void HandleLeaderReadIndex(Message message)
    {
        ValidateReadIndexRequest(message);
        if (IsLocalSingleton())
        {
            RespondToReadIndex(
                message,
                Log.Committed);
            return;
        }

        if (!HasCommittedEntryInCurrentTerm())
        {
            pendingReadIndexMessages.Enqueue(
                message.Clone());
            return;
        }

        if (ReadOnly.Option ==
                ReadOnlyOption.LeaseBased
            && IsLocalVoter())
        {
            RespondToReadIndex(
                message,
                Log.Committed);
        }
        else
        {
            TrackReadIndex(message);
        }
    }

    private void HandleReadIndexResponse(Message message)
    {
        if (message.Entries.Count != 1)
        {
            LogError(
                $"{Id:x} ignored {MessageType.MsgReadIndexResp} " +
                $"with {message.Entries.Count} entries; expected exactly one entry.");
            return;
        }

        readStates.Add(new ReadState(
            message.Index,
            message.Entries[0].Data));
    }

    private void TrackReadIndex(Message message)
    {
        ReadOnly.AddRequest(
            Log.Committed,
            message);
        ByteString context =
            ReadOnly.GetHeartbeatContext();
        ReadOnly.ReceiveAcknowledgement(
            Id,
            context);
        AdvanceReadOnly();
        if (ReadOnly.PendingCount > 0)
        {
            BroadcastHeartbeat();
        }
    }

    private void AdvanceReadOnly()
    {
        foreach (ReadIndexRequest request in
                 ReadOnly.Advance(
                     Tracker.Config.Voters))
        {
            RespondToReadIndex(
                request.Request,
                request.Index);
        }
    }

    private void RespondToReadIndex(
        Message request,
        ulong index)
    {
        if (request.From == RaftMessageTargets.None
            || request.From == Id)
        {
            readStates.Add(new ReadState(
                index,
                request.Entries[0].Data));
            return;
        }

        var response = new Message
        {
            To = request.From,
            Type = MessageType.MsgReadIndexResp,
            Index = index,
        };
        response.Entries.Add(
            request.Entries.Select(
                entry => entry.Clone()));
        Send(response);
    }

    private void ReleasePendingReadIndexMessages()
    {
        if (Role != RaftRole.Leader
            || !HasCommittedEntryInCurrentTerm())
        {
            return;
        }

        while (pendingReadIndexMessages.TryDequeue(
                   out Message? request))
        {
            HandleLeaderReadIndex(request);
        }
    }

    private bool HasCommittedEntryInCurrentTerm()
    {
        try
        {
            return Log.GetTerm(Log.Committed) == Term;
        }
        catch (StorageException exception)
            when (exception.Error is
                  StorageError.Compacted or
                  StorageError.Unavailable)
        {
            return false;
        }
    }

    private bool IsLocalSingleton()
    {
        return Tracker.IsSingleton
            && Tracker.IsVoter(Id);
    }

    private bool IsLocalVoter()
    {
        return Tracker.IsVoter(Id);
    }

    private static void ValidateReadIndexRequest(
        Message message)
    {
        if (message.Entries.Count != 1)
        {
            throw new RaftInvariantException(
                $"{MessageType.MsgReadIndex} must contain exactly one entry.");
        }
    }

    private void HandleUnreachable(Message message)
    {
        if (!Tracker.Progress.TryGetValue(
                message.From,
                out Progress? progress))
        {
            return;
        }

        if (progress.State == ProgressState.Replicate)
        {
            progress.BecomeProbe();
        }
    }

    private bool MaybeSendSnapshot(
        ulong to,
        Progress progress)
    {
        if (!progress.RecentActive)
        {
            return false;
        }

        Snapshot snapshot;
        try
        {
            snapshot = Log.GetSnapshot();
        }
        catch (StorageException exception)
            when (exception.Error ==
                  StorageError.SnapshotTemporarilyUnavailable)
        {
            return false;
        }

        ulong snapshotIndex = snapshot.Metadata.Index;
        if (snapshotIndex == 0)
        {
            throw new RaftInvariantException(
                "Cannot send an empty snapshot.");
        }

        progress.BecomeSnapshot(snapshotIndex);
        Send(new Message
        {
            To = to,
            Type = MessageType.MsgSnap,
            Snapshot = snapshot,
        });
        return true;
    }

    private void HandleSnapshotStatus(Message message)
    {
        if (!Tracker.Progress.TryGetValue(
                message.From,
                out Progress? progress)
            || progress.State != ProgressState.Snapshot)
        {
            return;
        }

        progress.ReportSnapshot(!message.Reject);
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

        var scratch = new ProgressTracker(
            Tracker.MaxInflightMessages,
            Tracker.MaxInflightBytes);
        ConfigurationChangeResult restored =
            ConfigurationRestore.Restore(
                new ConfigurationChanger(
                    scratch,
                    snapshotIndex),
                state);

        ulong restoredFromCommit = Log.Committed;
        Log.RestoreUncommitted(snapshot);
        Tracker.Install(
            restored.Config,
            restored.Progress);
        Tracker.ResetVotes();
        TraceConfigurationApplied(
            Tracker.ToConfState());
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

    private ConfState SwitchToConfiguration(
        TrackerConfig config,
        ProgressMap progress)
    {
        Tracker.Install(config, progress);

        ConfState state = Tracker.ToConfState();
        bool hasLocalProgress =
            Tracker.Contains(Id);
        TraceConfigurationApplied(state);

        if ((!hasLocalProgress || IsLearner)
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
            LeaderTransferee =
                RaftMessageTargets.None;
        }

        return state;
    }

    private void ReevaluateReadOnlyAfterConfigurationChange()
    {
        AdvanceReadOnly();
        if (ReadOnly.PendingCount > 0)
        {
            BroadcastHeartbeat();
        }
    }

    private void HandleCheckQuorum()
    {
        if (!Tracker.QuorumActive())
        {
            LogWarning(
                $"{Id:x} stepped down because quorum is not active.");
            BecomeFollower(
                Term,
                RaftMessageTargets.None);
        }

        Tracker.Visit((id, progress) =>
        {
            if (id != Id)
            {
                progress.RecentActive = false;
            }
        });
    }

    private void HandleForgetLeader()
    {
        switch (Role)
        {
            case RaftRole.Follower:
                if (ReadOnly.Option ==
                    ReadOnlyOption.LeaseBased)
                {
                    LogError(
                        "Ignoring MsgForgetLeader in lease-based read mode.");
                    return;
                }

                LeaderId = RaftMessageTargets.None;
                return;
            case RaftRole.PreCandidate:
            case RaftRole.Candidate:
            case RaftRole.Leader:
                return;
            default:
                throw new RaftInvariantException(
                    $"Unknown Raft role {Role}.");
        }
    }

    private bool ShouldIgnoreElectionRequestWithinLease(
        Message message)
    {
        return message.Type is
                MessageType.MsgVote or
                MessageType.MsgPreVote
            && CheckQuorum
            && LeaderId != RaftMessageTargets.None
            && ElectionElapsed < ElectionTick
            && message.Context != CampaignTransferContext;
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

        if ((CheckQuorum || PreVote)
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
    {
        if (acknowledgeEntries
            && message.Index != 0)
        {
            Log.StableTo(
                new EntryId(
                    message.LogTerm,
                    message.Index));
        }

        if (message.Snapshot is null)
        {
            return;
        }

        Snapshot snapshot =
            ProtocolDefaults.EnsureSnapshot(
                message.Snapshot.Clone());
        ulong index = snapshot.Metadata.Index;
        if (Log.HasUnstableSnapshotAt(index))
        {
            ReassertSnapshotConfiguration(snapshot);
        }

        Log.StableSnapshotTo(index);
        AppliedTo(index, 0);
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
        AppliedTo(
            entries[^1].Index,
            EntrySizing.EncodedSize(entries));
        ReduceUncommittedSize(
            EntrySizing.PayloadSize(entries));
    }

    private void ReassertSnapshotConfiguration(
        Snapshot snapshot)
    {
        ulong index = snapshot.Metadata.Index;
        var scratch = new ProgressTracker(
            Tracker.MaxInflightMessages,
            Tracker.MaxInflightBytes);
        ConfigurationChangeResult restored =
            ConfigurationRestore.Restore(
                new ConfigurationChanger(
                    scratch,
                    index),
                snapshot.Metadata.ConfState);
        Tracker.Install(
            restored.Config,
            restored.Progress);
        Tracker.ResetVotes();
        TraceConfigurationApplied(
            Tracker.ToConfState());
    }

    private void MaybeAutoLeave()
    {
        if (!Tracker.Config.AutoLeave
            || Log.Applied <
                PendingConfigurationIndex
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

    private void Reset(ulong term)
    {
        if (term < Term)
        {
            throw new RaftInvariantException(
                $"Raft term cannot decrease from {Term} to {term}.");
        }

        clock.Reset();

        if (Term != term)
        {
            Term = term;
            Vote = RaftMessageTargets.None;
        }

        LeaderId = RaftMessageTargets.None;
        LeaderTransferee = RaftMessageTargets.None;
        Tracker.ResetVotes();

        ulong lastIndex = Log.LastIndex;
        ulong next = lastIndex + 1;
        Tracker.Visit((id, progress) =>
        {
            progress.Reset(
                id == Id ? lastIndex : 0,
                next);
        });

        PendingConfigurationIndex = 0;
        UncommittedSize = 0;
        ReadOnly = new ReadOnlyTracker(ReadOnly.Option);
    }

    private void LoadHardState(HardState state)
    {
        ulong commit = state.Commit;
        if (commit < Log.Committed || commit > Log.LastIndex)
        {
            throw new RaftInvariantException(
                $"Persisted commit {commit} is outside " +
                $"[{Log.Committed}, {Log.LastIndex}].");
        }

        Log.CommitTo(commit);
        Term = state.Term;
        Vote = state.Vote;
    }

    private void TraceConfigurationProposals(
        IReadOnlyList<Entry> entries)
    {
        if (traceSink is null)
        {
            return;
        }

        foreach (Entry entry in entries)
        {
            string? detail = entry.Type switch
            {
                EntryType.EntryConfChange =>
                    RaftDescriptions.DescribeConfChange(
                        ProtocolConfChange.Parser.ParseFrom(
                            entry.Data)),
                EntryType.EntryConfChangeV2 =>
                    RaftDescriptions.DescribeConfChange(
                        ConfChangeV2.Parser.ParseFrom(
                            entry.Data)),
                _ => null,
            };
            if (detail is not null)
            {
                Trace(
                    RaftTraceEventType.ConfigurationProposed,
                    detail: detail);
            }
        }
    }

    private void TraceEntriesAppended(
        Entry[] entries)
    {
        if (traceSink is null || entries.Length == 0)
        {
            return;
        }

        TraceEntriesAppended(
            entries.Length,
            entries[0].Index,
            entries[^1].Index);
    }

    private void TraceEntriesAppended(
        int count,
        ulong firstIndex,
        ulong lastIndex)
    {
        if (traceSink is null || count == 0)
        {
            return;
        }

        string detail = string.Create(
            System.Globalization.CultureInfo.InvariantCulture,
            $"count:{count} first:{firstIndex} last:{lastIndex}");
        Trace(
            RaftTraceEventType.EntriesAppended,
            detail: detail);
    }

    private void TraceCommitAdvanced(
        ulong previousCommit)
    {
        if (traceSink is null
            || Log.Committed <= previousCommit)
        {
            return;
        }

        Trace(RaftTraceEventType.CommitAdvanced);
    }

    private void TraceConfigurationApplied(
        ConfState state)
    {
        if (traceSink is null)
        {
            return;
        }

        Trace(
            RaftTraceEventType.ConfigurationApplied,
            detail:
                RaftDescriptions.DescribeConfState(state));
    }

    private void TraceMessage(
        RaftTraceEventType type,
        Message message)
    {
        if (traceSink is null)
        {
            return;
        }

        ulong snapshotIndex =
            message.Snapshot?.Metadata?.Index ?? 0;
        Trace(
            type,
            new RaftTraceMessage(
                message.Type,
                message.From,
                message.To,
                message.Term,
                message.LogTerm,
                message.Index,
                message.Commit,
                message.Vote,
                message.Reject,
                message.RejectHint,
                message.Entries.Count,
                snapshotIndex));
    }

    private void Trace(
        RaftTraceEventType type,
        RaftTraceMessage? message = null,
        string? detail = null)
    {
        if (traceSink is null)
        {
            return;
        }

        var traceEvent = new RaftTraceEvent(
            type,
            GetBasicStatus(),
            Log.LastIndex,
            GetConfigurationStatus(),
            message,
            detail);
        try
        {
            traceSink.Trace(traceEvent);
        }
        catch (Exception exception)
        {
            throw new RaftTracingException(
                $"Raft trace sink failed while emitting {type}.",
                exception);
        }
    }

    private void LogInformation(string message)
    {
        if (Logger.IsEnabled(RaftLogLevel.Information))
        {
            Logger.Log(RaftLogLevel.Information, message);
        }
    }

    private void LogDebug(string message)
    {
        if (Logger.IsEnabled(RaftLogLevel.Debug))
        {
            Logger.Log(RaftLogLevel.Debug, message);
        }
    }

    private void LogError(string message)
    {
        if (Logger.IsEnabled(RaftLogLevel.Error))
        {
            Logger.Log(RaftLogLevel.Error, message);
        }
    }

    private void LogWarning(string message)
    {
        if (Logger.IsEnabled(RaftLogLevel.Warning))
        {
            Logger.Log(RaftLogLevel.Warning, message);
        }
    }
}
