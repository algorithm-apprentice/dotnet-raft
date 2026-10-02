using DotnetRaft.Core;
using DotnetRaft.Protocol;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft;

public sealed class RawNode
{
    private readonly RaftCore _core;
    private SoftState _previousSoftState;
    private HardState _previousHardState;
    private Ready? _outstanding;
    private ReadyAcknowledgement? _acknowledgement;
    private Exception? _fault;

    public RawNode(RaftConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        if (config.AsyncStorageWrites)
        {
            throw new NotSupportedException(
                "Asynchronous storage writes are introduced in D25.");
        }

        _core = new RaftCore(config);
        _previousSoftState = _core.SoftState;
        _previousHardState = _core.HardState;
    }

    internal RaftCore Core => _core;

    public void Tick()
    {
        EnsureUsable();
        if (_core.Role == RaftRole.Leader)
        {
            _core.TickLeader();
            return;
        }

        _core.TickElection();
    }

    public void Campaign()
    {
        EnsureUsable();
        _core.Step(LocalMessage(MessageType.MsgHup));
    }

    public void Propose(ReadOnlySpan<byte> data)
    {
        EnsureUsable();
        var proposal = LocalMessage(MessageType.MsgProp);
        proposal.Entries.Add(new Entry
        {
            Data = ByteString.CopyFrom(data),
        });
        _core.Step(proposal);
    }

    public void ProposeConfChange(ProtocolConfChange change)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(change);
        var proposal = LocalMessage(MessageType.MsgProp);
        proposal.Entries.Add(new Entry
        {
            Type = EntryType.EntryConfChange,
            Data = change.ToByteString(),
        });
        _core.Step(proposal);
    }

    public void ProposeConfChange(ConfChangeV2 change)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(change);
        var proposal = LocalMessage(MessageType.MsgProp);
        proposal.Entries.Add(new Entry
        {
            Type = EntryType.EntryConfChangeV2,
            Data = change.ToByteString(),
        });
        _core.Step(proposal);
    }

    public ConfState ApplyConfChange(ProtocolConfChange change)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(change);
        return ApplyConfChangeCore(
            () => _core.ApplyConfigurationChange(change));
    }

    public ConfState ApplyConfChange(ConfChangeV2 change)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(change);
        return ApplyConfChangeCore(
            () => _core.ApplyConfigurationChange(change));
    }

    public void Step(Message message)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(message);

        if (RaftMessageTargets.IsLocal(message.From))
        {
            throw new InvalidOperationException(
                $"Messages from reserved local sender {message.From} cannot be stepped through the network facade.");
        }

        if (MessageClassifier.IsLocal(message.Type))
        {
            throw new InvalidOperationException(
                $"Local message {message.Type} cannot be stepped through the network facade.");
        }

        if (MessageClassifier.IsResponse(message.Type)
            && !_core.Tracker.Progress.ContainsKey(
                message.From))
        {
            throw new InvalidOperationException(
                $"Response sender {message.From} is not a known peer.");
        }

        _core.Step(message);
    }

    public bool HasReady()
    {
        EnsureUsable();
        if (_outstanding is not null)
        {
            return false;
        }

        SoftState softState = _core.SoftState;
        if (softState != _previousSoftState)
        {
            return true;
        }

        HardState hardState = _core.HardState;
        if (!HardStateEquals(
                hardState,
                _previousHardState))
        {
            return true;
        }

        return _core.Log.HasNextUnstableSnapshot
            || _core.HasMessages
            || _core.HasMessagesAfterAppend
            || _core.Log.HasNextUnstableEntries
            || _core.Log.HasNextCommittedEntries(
                allowUnstable: true)
            || _core.HasReadStates;
    }

    public Ready Ready()
    {
        EnsureUsable();
        if (_outstanding is not null)
        {
            throw new InvalidOperationException(
                "The outstanding Ready must be advanced before requesting another batch.");
        }

        SoftState currentSoftState = _core.SoftState;
        HardState currentHardState = _core.HardState;
        SoftState? emittedSoftState =
            currentSoftState == _previousSoftState
                ? null
                : currentSoftState;
        HardState? emittedHardState =
            HardStateEquals(
                currentHardState,
                _previousHardState)
                ? null
                : currentHardState;

        Entry[] unstableEntries =
        [
            .. _core.Log.GetNextUnstableEntries(),
        ];
        Snapshot? snapshot =
            _core.Log.GetNextUnstableSnapshot();
        Entry[] committedEntries =
        [
            .. _core.Log.GetNextCommittedEntries(
                allowUnstable: true),
        ];
        Message[] immediateMessages =
            _core.PeekMessages();
        Message[] afterAppendMessages =
            _core.PeekMessagesAfterAppend();
        ReadState[] readStates =
            _core.PeekReadStates();

        var exposedMessages = new List<Message>(
            immediateMessages.Length
            + afterAppendMessages.Length);
        exposedMessages.AddRange(immediateMessages);
        exposedMessages.AddRange(
            afterAppendMessages.Where(
                message => message.To != _core.Id));
        Message[] selfMessages =
        [
            .. afterAppendMessages
                .Where(message => message.To == _core.Id)
                .Select(message => message.Clone()),
        ];

        bool mustSync = MustSync(
            currentHardState,
            _previousHardState,
            unstableEntries.Length);
        var ready = new Ready(
            emittedSoftState,
            emittedHardState,
            readStates,
            unstableEntries,
            snapshot,
            committedEntries,
            exposedMessages,
            mustSync);
        ReadyAcknowledgement acknowledgement =
            CreateAcknowledgement(
                selfMessages,
                unstableEntries,
                snapshot,
                committedEntries);

        if (emittedSoftState is not null)
        {
            _previousSoftState = currentSoftState;
        }

        if (emittedHardState is not null)
        {
            _previousHardState =
                currentHardState.Clone();
        }

        _core.TakeMessages();
        _core.TakeMessagesAfterAppend();
        _core.TakeReadStates();
        _core.Log.AcceptUnstable();
        if (acknowledgement.AppliedIndex.HasValue)
        {
            _core.Log.AcceptApplying(
                acknowledgement.AppliedIndex.Value,
                acknowledgement.AppliedEncodedSize,
                allowUnstable: true);
        }

        _outstanding = ready;
        _acknowledgement = acknowledgement;
        return ready;
    }

    public void Advance(Ready ready)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(ready);
        if (_outstanding is null
            || _acknowledgement is null)
        {
            throw new InvalidOperationException(
                "No Ready is awaiting advancement.");
        }

        if (!ReferenceEquals(_outstanding, ready))
        {
            throw new InvalidOperationException(
                "Advance must receive the exact outstanding Ready instance.");
        }

        ReadyAcknowledgement acknowledgement =
            _acknowledgement;
        try
        {
            foreach (Message message
                     in acknowledgement.SelfMessages)
            {
                _core.Step(message);
            }

            if (acknowledgement.StableEntry.HasValue)
            {
                _core.Log.StableTo(
                    acknowledgement.StableEntry.Value);
            }

            if (acknowledgement.SnapshotIndex.HasValue)
            {
                _core.Log.AcknowledgeSnapshot(
                    acknowledgement.SnapshotIndex.Value);
            }

            if (acknowledgement.AppliedIndex.HasValue)
            {
                _core.AppliedTo(
                    acknowledgement.AppliedIndex.Value,
                    acknowledgement.AppliedEncodedSize);
                _core.ReduceUncommittedSize(
                    acknowledgement.AppliedPayloadSize);
            }
        }
        catch (Exception exception)
        {
            _fault = exception;
            throw;
        }

        _outstanding = null;
        _acknowledgement = null;
    }

    public void ReportUnreachable(ulong id)
    {
        EnsureUsable();
        _core.Step(new Message
        {
            From = id,
            To = _core.Id,
            Type = MessageType.MsgUnreachable,
        });
    }

    public void ReportSnapshot(
        ulong id,
        SnapshotStatus status)
    {
        EnsureUsable();
        if (status is not (
                SnapshotStatus.Success
                or SnapshotStatus.Failure))
        {
            throw new ArgumentOutOfRangeException(
                nameof(status),
                status,
                "Unknown snapshot status.");
        }

        _core.Step(new Message
        {
            From = id,
            To = _core.Id,
            Type = MessageType.MsgSnapStatus,
            Reject = status == SnapshotStatus.Failure,
        });
    }

    public void TransferLeader(ulong transferee)
    {
        EnsureUsable();
        _core.Step(new Message
        {
            From = transferee,
            To = _core.Id,
            Type = MessageType.MsgTransferLeader,
        });
    }

    public void ForgetLeader()
    {
        EnsureUsable();
        _core.Step(LocalMessage(
            MessageType.MsgForgetLeader));
    }

    public void ReadIndex(ReadOnlySpan<byte> context)
    {
        EnsureUsable();
        var request = LocalMessage(
            MessageType.MsgReadIndex);
        request.Entries.Add(new Entry
        {
            Data = ByteString.CopyFrom(context),
        });
        _core.Step(request);
    }

    public static bool MustSync(
        HardState state,
        HardState previousState,
        int entryCount)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(previousState);
        ArgumentOutOfRangeException.ThrowIfNegative(
            entryCount);

        return entryCount != 0
            || state.Term != previousState.Term
            || state.Vote != previousState.Vote;
    }

    private ConfState ApplyConfChangeCore(
        Func<ConfState> apply)
    {
        try
        {
            return apply().Clone();
        }
        catch (Exception exception)
        {
            _fault = exception;
            throw;
        }
    }

    private Message LocalMessage(MessageType type)
    {
        return new Message
        {
            From = _core.Id,
            To = _core.Id,
            Type = type,
        };
    }

    private void EnsureUsable()
    {
        if (_fault is not null)
        {
            throw new InvalidOperationException(
                "RawNode is faulted and must be discarded.",
                _fault);
        }
    }

    private static bool HardStateEquals(
        HardState left,
        HardState right)
    {
        return left.Term == right.Term
            && left.Vote == right.Vote
            && left.Commit == right.Commit;
    }

    private static ReadyAcknowledgement
        CreateAcknowledgement(
            Message[] selfMessages,
            Entry[] unstableEntries,
            Snapshot? snapshot,
            Entry[] committedEntries)
    {
        EntryId? stableEntry =
            unstableEntries.Length == 0
                ? null
                : EntryId.From(unstableEntries[^1]);
        ulong? snapshotIndex =
            snapshot?.Metadata.Index;
        ulong? appliedIndex =
            committedEntries.Length == 0
                ? null
                : committedEntries[^1].Index;
        ulong encodedSize =
            EntrySizing.EncodedSize(committedEntries);
        ulong payloadSize =
            EntrySizing.PayloadSize(committedEntries);

        return new ReadyAcknowledgement(
            selfMessages,
            stableEntry,
            snapshotIndex,
            appliedIndex,
            encodedSize,
            payloadSize);
    }

    private sealed record ReadyAcknowledgement(
        IReadOnlyList<Message> SelfMessages,
        EntryId? StableEntry,
        ulong? SnapshotIndex,
        ulong? AppliedIndex,
        ulong AppliedEncodedSize,
        ulong AppliedPayloadSize);
}
