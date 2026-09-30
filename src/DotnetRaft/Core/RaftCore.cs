using System.Security.Cryptography;

using DotnetRaft.ConfChange;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Read;
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

        Reset(Term);
        LeaderId = Id;
        Role = RaftRole.Leader;

        localProgress.BecomeReplicate();
        localProgress.RecentActive = true;
        PendingConfigurationIndex = Log.LastIndex;
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
