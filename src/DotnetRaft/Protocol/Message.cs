namespace DotnetRaft.Protocol;

/// <summary>Owned input or output for a Raft transition, before encoding.</summary>
public sealed class Message : IEquatable<Message>
{
    private MessageType? type;
    private ulong? to;
    private ulong? from;
    private ulong? term;
    private ulong? logTerm;
    private ulong? index;
    private ulong? commit;
    private ulong? vote;
    private bool? reject;
    private ulong? rejectHint;
    private RaftBytes? context;

    public MessageType Type { get => type.GetValueOrDefault(); set => type = value; }
    public ulong To { get => to.GetValueOrDefault(); set => to = value; }
    public ulong From { get => from.GetValueOrDefault(); set => from = value; }
    public ulong Term { get => term.GetValueOrDefault(); set => term = value; }
    public ulong LogTerm { get => logTerm.GetValueOrDefault(); set => logTerm = value; }
    public ulong Index { get => index.GetValueOrDefault(); set => index = value; }
    public ulong Commit { get => commit.GetValueOrDefault(); set => commit = value; }
    public ulong Vote { get => vote.GetValueOrDefault(); set => vote = value; }
    public bool Reject { get => reject.GetValueOrDefault(); set => reject = value; }
    public ulong RejectHint { get => rejectHint.GetValueOrDefault(); set => rejectHint = value; }
    public RaftBytes Context { get => context.GetValueOrDefault(); set => context = value; }
    public Snapshot? Snapshot { get; set; }
    public List<Entry> Entries { get; } = [];
    public List<Message> Responses { get; } = [];
    public RaftBytes ExtensionData { get; init; }

    public bool HasType => type.HasValue;
    public bool HasTo => to.HasValue;
    public bool HasFrom => from.HasValue;
    public bool HasTerm => term.HasValue;
    public bool HasLogTerm => logTerm.HasValue;
    public bool HasIndex => index.HasValue;
    public bool HasCommit => commit.HasValue;
    public bool HasVote => vote.HasValue;
    public bool HasReject => reject.HasValue;
    public bool HasRejectHint => rejectHint.HasValue;
    public bool HasContext => context.HasValue;

    public Message Clone()
    {
        var copy = new Message
        {
            type = type,
            to = to,
            from = from,
            term = term,
            logTerm = logTerm,
            index = index,
            commit = commit,
            vote = vote,
            reject = reject,
            rejectHint = rejectHint,
            context = context,
            Snapshot = Snapshot?.Clone(),
            ExtensionData = ExtensionData,
        };
        foreach (Entry entry in Entries)
        {
            ArgumentNullException.ThrowIfNull(entry);
            copy.Entries.Add(entry.Clone());
        }

        foreach (Message response in Responses)
        {
            ArgumentNullException.ThrowIfNull(response);
            copy.Responses.Add(response.Clone());
        }

        return copy;
    }

    public bool Equals(Message? other) => other is not null && Type == other.Type && To == other.To
        && From == other.From && Term == other.Term && LogTerm == other.LogTerm && Index == other.Index
        && Commit == other.Commit && Vote == other.Vote && Reject == other.Reject && RejectHint == other.RejectHint
        && Context == other.Context && Equals(Snapshot, other.Snapshot) && Entries.SequenceEqual(other.Entries)
        && Responses.SequenceEqual(other.Responses) && ExtensionData == other.ExtensionData;

    public override bool Equals(object? obj) => obj is Message other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(Type, To, From, Term, Index, Commit, Context, ExtensionData);
}
