using System.Security.Cryptography;

using DotnetRaft.ConfChange;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Quorum;
using DotnetRaft.Read;
using DotnetRaft.Storage;
using DotnetRaft.Tracker;

namespace DotnetRaft.Core;

internal sealed class RaftCore
{
    private readonly Func<int, int> randomOffset;
    private readonly List<Message> messages = [];
    private readonly List<Message> messagesAfterAppend = [];
    private int electionElapsed;
    private int heartbeatElapsed;

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
        this.randomOffset = randomOffset;

        Id = validated.Id;
        ElectionTick = validated.ElectionTick;
        HeartbeatTick = validated.HeartbeatTick;
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
        Tracker.Config = restored.Config;
        Tracker.Progress = restored.Progress;
        UpdateLocalLearnerState();

        if (!IsEmpty(persistedHardState))
        {
            LoadHardState(persistedHardState!);
        }

        if (validated.Applied > 0)
        {
            Log.AppliedTo(validated.Applied, 0);
        }

        BecomeFollower(Term, RaftMessageTargets.None);
        LogInformation(
            $"New Raft {Id:x} [term: {Term}, commit: {Log.Committed}, " +
            $"applied: {Log.Applied}, last index: {Log.LastIndex}].");
    }

    internal ulong Id { get; }

    internal ulong Term { get; private set; }

    internal ulong Vote { get; private set; }

    internal ulong LeaderId { get; private set; }

    internal RaftRole Role { get; private set; }

    internal bool IsLearner { get; private set; }

    internal RaftLog Log { get; }

    internal ProgressTracker Tracker { get; }

    internal ReadOnlyTracker ReadOnly { get; private set; }

    internal IRaftLogger Logger { get; }

    internal int ElectionTick { get; }

    internal int HeartbeatTick { get; }

    internal int ElectionElapsed
    {
        get => electionElapsed;
        set => electionElapsed = value;
    }

    internal int HeartbeatElapsed
    {
        get => heartbeatElapsed;
        set => heartbeatElapsed = value;
    }

    internal int RandomizedElectionTimeout { get; private set; }

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
        Tracker.Progress.TryGetValue(Id, out Progress? progress)
        && !progress.IsLearner
        && !Log.HasUnstableSnapshot;

    internal bool PastElectionTimeout =>
        ElectionElapsed >= RandomizedElectionTimeout;

    internal SoftState SoftState => new(LeaderId, Role);

    internal HardState HardState => new()
    {
        Term = Term,
        Vote = Vote,
        Commit = Log.Committed,
    };

    internal void BecomeFollower(ulong term, ulong leaderId)
    {
        Reset(term);
        LeaderId = leaderId;
        Role = RaftRole.Follower;
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
        AppendLeaderEntries([new Entry()]);
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

        electionElapsed = IncrementElapsed(
            electionElapsed,
            "election");
        if (!Promotable || !PastElectionTimeout)
        {
            return false;
        }

        electionElapsed = 0;
        return true;
    }

    internal LeaderClockTick TickLeaderClocks()
    {
        if (Role != RaftRole.Leader)
        {
            throw new RaftInvariantException(
                "Only a leader can tick leader clocks.");
        }

        int nextElectionElapsed = IncrementElapsed(
            electionElapsed,
            "election");
        int nextHeartbeatElapsed = IncrementElapsed(
            heartbeatElapsed,
            "heartbeat");

        bool electionDue = nextElectionElapsed >= ElectionTick;
        bool heartbeatDue = nextHeartbeatElapsed >= HeartbeatTick;
        electionElapsed = electionDue ? 0 : nextElectionElapsed;
        heartbeatElapsed = heartbeatDue ? 0 : nextHeartbeatElapsed;
        return new LeaderClockTick(electionDue, heartbeatDue);
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
        if (!tick.HeartbeatDue || Role != RaftRole.Leader)
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
            && IsRealVoteMessage(message.Type))
        {
            throw new RaftInvariantException(
                $"{message.Type} must carry a nonzero term.");
        }

        if (message.Term > Term)
        {
            if (IsHigherTermPreVoteException(message))
            {
                return;
            }

            ulong leaderId = IsLeaderMessage(message.Type)
                ? message.From
                : RaftMessageTargets.None;
            BecomeFollower(message.Term, leaderId);
        }
        else if (message.Term != 0 && message.Term < Term)
        {
            return;
        }

        switch (message.Type)
        {
            case MessageType.MsgHup:
                HandleHup();
                return;
            case MessageType.MsgVote:
                HandleVoteRequest(message);
                return;
            default:
                HandleRoleMessage(message);
                return;
        }
    }

    internal void Campaign()
    {
        BecomeCandidate();

        EntryId lastEntry = default;
        bool hasLastEntry = false;
        foreach (ulong voterId in Tracker.VoterNodes())
        {
            if (voterId == Id)
            {
                Send(new Message
                {
                    To = voterId,
                    Term = Term,
                    Type = MessageType.MsgVoteResp,
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
                Term = Term,
                Type = MessageType.MsgVote,
                Index = lastEntry.Index,
                LogTerm = lastEntry.Term,
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
            return;
        }

        if (outbound.To == Id)
        {
            throw new RaftInvariantException(
                $"Immediate outbound {outbound.Type} cannot target the local node.");
        }

        messages.Add(outbound);
    }

    internal Message[] TakeMessages()
    {
        return Take(messages);
    }

    internal Message[] TakeMessagesAfterAppend()
    {
        return Take(messagesAfterAppend);
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

    private static bool IsRealVoteMessage(MessageType type)
    {
        return type is
            MessageType.MsgVote or
            MessageType.MsgVoteResp;
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

    private static Message[] Take(List<Message> queue)
    {
        Message[] taken = [.. queue];
        queue.Clear();
        return taken;
    }

    private static int IncrementElapsed(
        int elapsed,
        string clockName)
    {
        if (elapsed == int.MaxValue)
        {
            throw new RaftInvariantException(
                $"{clockName} elapsed counter overflowed.");
        }

        return elapsed + 1;
    }

    private void HandleHup()
    {
        if (Role == RaftRole.Leader
            || !Promotable
            || HasUnappliedConfigurationChanges())
        {
            return;
        }

        Campaign();
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
                && LeaderId == RaftMessageTargets.None);
        bool upToDate = Log.IsUpToDate(
            new EntryId(message.LogTerm, message.Index));

        if (canVote && upToDate)
        {
            Send(new Message
            {
                To = message.From,
                Term = message.Term,
                Type = MessageType.MsgVoteResp,
            });
            electionElapsed = 0;
            Vote = message.From;
            return;
        }

        Send(new Message
        {
            To = message.From,
            Term = Term,
            Type = MessageType.MsgVoteResp,
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
            case MessageType.MsgBeat:
                if (Role == RaftRole.Leader)
                {
                    BroadcastHeartbeat();
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
            case MessageType.MsgVoteResp
                when Role == RaftRole.Candidate:
                HandleVoteResponse(message);
                return;
        }
    }

    private bool HandleLeaderMessage(Message message)
    {
        switch (Role)
        {
            case RaftRole.Follower:
                electionElapsed = 0;
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
                BecomeLeader();
                BroadcastAppend();
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

                AppendLeaderEntries(message.Entries);
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

    private void AppendLeaderEntries(
        IEnumerable<Entry> entries)
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

        for (var offset = 0; offset < appended.Length; offset++)
        {
            Entry entry = appended[offset];
            entry.Term = Term;
            entry.Index =
                lastIndex + (ulong)offset + 1;
        }

        ulong newLastIndex = Log.Append(appended);
        Send(new Message
        {
            To = Id,
            Type = MessageType.MsgAppResp,
            Index = newLastIndex,
        });
    }

    private void BroadcastAppend()
    {
        Tracker.Visit((id, progress) =>
        {
            if (id != Id)
            {
                SendAppend(id, progress);
            }
        });
    }

    private bool SendAppend(ulong to, Progress progress)
    {
        ulong previousIndex = progress.Next - 1;
        ulong previousTerm;
        IReadOnlyList<Entry> entries;
        try
        {
            previousTerm = Log.GetTerm(previousIndex);
            entries = Log.GetEntries(progress.Next);
        }
        catch (StorageException exception)
            when (exception.Error == StorageError.Compacted)
        {
            return false;
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
        if (Log.MaybeAppend(
                slice,
                message.Commit,
                out ulong lastNewIndex))
        {
            Send(new Message
            {
                To = message.From,
                Type = MessageType.MsgAppResp,
                Index = lastNewIndex,
            });
            return;
        }

        Send(new Message
        {
            To = message.From,
            Type = MessageType.MsgAppResp,
            Index = message.Index,
            Reject = true,
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

            if (progress.MaybeDecrementTo(
                    message.Index,
                    message.Index - 1))
            {
                SendAppend(message.From, progress);
            }

            return;
        }

        if (message.Index > Log.LastIndex)
        {
            throw new RaftInvariantException(
                $"Append acknowledgement {message.Index} exceeds last index {Log.LastIndex}.");
        }

        progress.RecentActive = true;
        if (!progress.MaybeUpdate(message.Index))
        {
            return;
        }

        if (MaybeCommit())
        {
            BroadcastAppend();
        }
        else if (message.From != Id
                 && progress.Match < Log.LastIndex)
        {
            SendAppend(message.From, progress);
        }
    }

    private bool MaybeCommit()
    {
        return Log.MaybeCommit(new EntryId(
            Term: Term,
            Index: Tracker.CommittedIndex));
    }

    private void BroadcastHeartbeat()
    {
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
                Commit = Math.Min(
                    progress.Match,
                    Log.Committed),
            });
        });
    }

    private void HandleHeartbeat(Message message)
    {
        Log.CommitTo(message.Commit);
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
        if (progress.Match < Log.LastIndex)
        {
            SendAppend(message.From, progress);
        }
    }

    private void Reset(ulong term)
    {
        if (term < Term)
        {
            throw new RaftInvariantException(
                $"Raft term cannot decrease from {Term} to {term}.");
        }

        int randomizedTimeout = NextRandomizedElectionTimeout();

        if (Term != term)
        {
            Term = term;
            Vote = RaftMessageTargets.None;
        }

        LeaderId = RaftMessageTargets.None;
        electionElapsed = 0;
        heartbeatElapsed = 0;
        RandomizedElectionTimeout = randomizedTimeout;
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

    private int NextRandomizedElectionTimeout()
    {
        int offset = randomOffset(ElectionTick);
        if (offset < 0 || offset >= ElectionTick)
        {
            throw new RaftInvariantException(
                $"Random election offset {offset} is outside [0, {ElectionTick}).");
        }

        try
        {
            return checked(ElectionTick + offset);
        }
        catch (OverflowException exception)
        {
            throw new RaftInvariantException(
                $"Randomized election timeout overflowed: {exception.Message}");
        }
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

    private void UpdateLocalLearnerState()
    {
        IsLearner =
            Tracker.Progress.TryGetValue(Id, out Progress? progress)
            && progress.IsLearner;
    }

    private void LogInformation(string message)
    {
        if (Logger.IsEnabled(RaftLogLevel.Information))
        {
            Logger.Log(RaftLogLevel.Information, message);
        }
    }
}
