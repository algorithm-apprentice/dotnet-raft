using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Quorum;
using DotnetRaft.Read;
using DotnetRaft.Tracker;

namespace DotnetRaft.Core;

internal sealed partial class RaftCore
{
    private static readonly RaftBytes CampaignTransferContext =
        RaftBytes.CopyFromUtf8("CampaignTransfer");

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
            MaybeAutoLeave();
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

    internal void Campaign()
    {
        Campaign(CampaignType.Election);
    }

    internal void Campaign(CampaignType campaignType)
    {
        MessageType voteType;
        ulong campaignTerm;
        RaftBytes context;
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
                context = RaftBytes.Empty;
                break;
            case CampaignType.Election:
                BecomeCandidate();
                voteType = MessageType.MsgVote;
                campaignTerm = Term;
                context = RaftBytes.Empty;
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
                clock.ResetElectionElapsed();
                roleState.GrantVote(message.From);
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

    private void HandleVoteResponse(
        Message message,
        bool preCandidate)
    {
        Tracker.RecordVote(message.From, !message.Reject);
        (_, _, VoteResult result) = Tracker.TallyVotes();
        switch (result)
        {
            case VoteResult.Won:
                if (preCandidate)
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

    internal void BecomeFollower(ulong term, ulong leaderId)
    {
        Reset(term);
        roleState.BecomeFollower(leaderId);
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
        roleState.BecomeCandidate(Id);
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
        roleState.BecomePreCandidate();
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
        roleState.BecomeLeader(Id);

        localProgress.BecomeReplicate();
        localProgress.RecentActive = true;
        proposals.SetPendingConfigurationIndex(
            lastIndex);
        Trace(RaftTraceEventType.BecameLeader);
        ProposalAdmissionPlan plan =
            AppendLeaderEntries(
                [new Entry()],
                proposals.PendingConfigurationIndex);
        if (!plan.Accepted)
        {
            throw new RaftInvariantException(
                "The leader no-op cannot be rejected by the uncommitted-size quota.");
        }

        LogInformation(
            $"{Id:x} became leader at term {Term}.");
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

    private bool ShouldIgnoreElectionRequestWithinLease(
        Message message)
    {
        return message.Type is
                MessageType.MsgVote or
                MessageType.MsgPreVote
            && CheckQuorum
            && LeaderId != RaftMessageTargets.None
            && clock.ElectionElapsed <
                clock.ElectionTick
            && message.Context != CampaignTransferContext;
    }

    private void HandleFollowerForgetLeader()
    {
        if (reads.Option ==
            ReadOnlyOption.LeaseBased)
        {
            LogError(
                "Ignoring MsgForgetLeader in lease-based read mode.");
            return;
        }

        roleState.ForgetLeader();
    }

    private void HandleFollowerTransferLeader(
        Message message)
    {
        if (LeaderId == RaftMessageTargets.None)
        {
            return;
        }

        Message forwarded = message.Clone();
        forwarded.To = LeaderId;
        Send(forwarded);
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

        clock.ResetElectionElapsed();
        roleState.StartTransfer(transferee);
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
        roleState.AbortTransfer();
    }
}
