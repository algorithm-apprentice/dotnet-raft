using System.Security.Cryptography;

using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;

using Google.Protobuf;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft;

public sealed partial class RawNode
{
    private readonly RaftCore _core;
    private SoftState _previousSoftState;
    private HardState _previousHardState;
    private Ready? _outstanding;
    private ReadyAcknowledgement? _acknowledgement;
    private Exception? _fault;
    private RawNodeLifecycle _lifecycle;
    private bool _operationInProgress;

    public RawNode(RaftConfig config)
        : this(
            config,
            maximum =>
                RandomNumberGenerator.GetInt32(maximum))
    {
    }

    internal RawNode(
        RaftConfig config,
        Func<int, int> randomOffset)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(randomOffset);

        _core = new RaftCore(
            config,
            randomOffset);
        _previousSoftState = _core.SoftState;
        _previousHardState = _core.HardState;
    }

    internal RaftCore Core => _core;

    public bool AsyncStorageWrites =>
        _core.AsyncStorageWrites;

    internal bool IsFaultedForTesting =>
        IsFaulted;

    internal bool IsFaulted =>
        _fault is not null;

    internal void SetRandomizedElectionTimeoutForTesting(
        int timeout)
    {
        Execute(() =>
        {
            Activate();
            _core.SetRandomizedElectionTimeoutForTesting(
                timeout);
        });
    }

    public static RawNode Start(
        RaftConfig config,
        IEnumerable<Peer> peers)
    {
        ArgumentNullException.ThrowIfNull(config);
        Peer[] materialized = MaterializePeers(peers);
        var node = new RawNode(config);
        node.BootstrapMaterialized(materialized);
        return node;
    }

    public static RawNode Restart(RaftConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        return new RawNode(config);
    }

    public void Bootstrap(IEnumerable<Peer> peers)
    {
        Execute(() =>
        {
            Peer[] materialized = MaterializePeers(peers);
            BootstrapValidated(materialized);
        });
    }

    public void Tick()
    {
        Execute(() =>
        {
            Activate();
            if (_core.Role == RaftRole.Leader)
            {
                _core.TickLeader();
                return;
            }

            _core.TickElection();
        });
    }

    public void Campaign()
    {
        Execute(() =>
        {
            Activate();
            _core.Step(LocalMessage(MessageType.MsgHup));
        });
    }

    public void Propose(ReadOnlySpan<byte> data)
    {
        byte[] owned = data.ToArray();
        Execute(() =>
        {
            Activate();
            var proposal = LocalMessage(
                MessageType.MsgProp);
            proposal.Entries.Add(new Entry
            {
                Data = ByteString.CopyFrom(owned),
            });
            _core.Step(proposal);
        });
    }

    public void ProposeConfChange(
        ProtocolConfChange change)
    {
        Execute(() =>
        {
            ArgumentNullException.ThrowIfNull(change);
            Activate();
            var proposal = LocalMessage(
                MessageType.MsgProp);
            proposal.Entries.Add(new Entry
            {
                Type = EntryType.EntryConfChange,
                Data = change.ToByteString(),
            });
            _core.Step(proposal);
        });
    }

    public void ProposeConfChange(
        ConfChangeV2 change)
    {
        Execute(() =>
        {
            ArgumentNullException.ThrowIfNull(change);
            Activate();
            var proposal = LocalMessage(
                MessageType.MsgProp);
            proposal.Entries.Add(new Entry
            {
                Type = EntryType.EntryConfChangeV2,
                Data = change.ToByteString(),
            });
            _core.Step(proposal);
        });
    }

    public ConfState ApplyConfChange(
        ProtocolConfChange change)
    {
        return Execute(() =>
        {
            ArgumentNullException.ThrowIfNull(change);
            Activate();
            return ApplyConfChangeCore(
                () => _core.ApplyConfigurationChange(
                    change));
        });
    }

    public ConfState ApplyConfChange(
        ConfChangeV2 change)
    {
        return Execute(() =>
        {
            ArgumentNullException.ThrowIfNull(change);
            Activate();
            return ApplyConfChangeCore(
                () => _core.ApplyConfigurationChange(
                    change));
        });
    }

    public void Step(Message message)
    {
        Execute(() =>
        {
            ArgumentNullException.ThrowIfNull(message);
            Message owned = message.Clone();

            if (IsStorageResponse(owned.Type)
                || RaftMessageTargets.IsLocal(owned.From))
            {
                ValidateStorageResponse(owned);
                Activate();
                StepStorageResponseCore(owned);
                return;
            }

            if (MessageClassifier.IsLocal(owned.Type))
            {
                throw new InvalidOperationException(
                    $"Local message {owned.Type} cannot be stepped through the network facade.");
            }

            if (MessageClassifier.IsResponse(owned.Type)
                && !_core.Tracker.Progress.ContainsKey(
                    owned.From))
            {
                throw new InvalidOperationException(
                    $"Response sender {owned.From} is not a known peer.");
            }

            Activate();
            _core.Step(owned);
        });
    }

    public bool HasReady()
    {
        return Execute(HasReadyCore);
    }

    public Ready Ready()
    {
        return Execute(CreateReady);
    }

    public void Advance(Ready ready)
    {
        Execute(() => AdvanceCore(ready));
    }

    public void ReportUnreachable(ulong id)
    {
        Execute(() =>
        {
            Activate();
            _core.Step(new Message
            {
                From = id,
                To = _core.Id,
                Type = MessageType.MsgUnreachable,
            });
        });
    }

    public void ReportSnapshot(
        ulong id,
        SnapshotStatus status)
    {
        Execute(() =>
        {
            if (status is not (
                    SnapshotStatus.Success
                    or SnapshotStatus.Failure))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(status),
                    status,
                    "Unknown snapshot status.");
            }

            Activate();
            _core.Step(new Message
            {
                From = id,
                To = _core.Id,
                Type = MessageType.MsgSnapStatus,
                Reject =
                    status == SnapshotStatus.Failure,
            });
        });
    }

    public void TransferLeader(ulong transferee)
    {
        Execute(() =>
        {
            Activate();
            _core.Step(new Message
            {
                From = transferee,
                To = _core.Id,
                Type = MessageType.MsgTransferLeader,
            });
        });
    }

    public void ForgetLeader()
    {
        Execute(() =>
        {
            Activate();
            _core.Step(LocalMessage(
                MessageType.MsgForgetLeader));
        });
    }

    public void ReadIndex(ReadOnlySpan<byte> context)
    {
        byte[] owned = context.ToArray();
        Execute(() =>
        {
            Activate();
            var request = LocalMessage(
                MessageType.MsgReadIndex);
            request.Entries.Add(new Entry
            {
                Data = ByteString.CopyFrom(owned),
            });
            _core.Step(request);
        });
    }

    public BasicStatus GetBasicStatus()
    {
        return ExecuteDiagnostic(
            _core.GetBasicStatus);
    }

    public Status GetStatus()
    {
        return ExecuteDiagnostic(() =>
        {
            BasicStatus basic =
                _core.GetBasicStatus();
            ConfigurationStatus configuration =
                _core.GetConfigurationStatus();
            KeyValuePair<ulong, ProgressStatus>[] progress =
                basic.Role == RaftRole.Leader
                    ? _core.GetProgressStatuses()
                    : [];
            return new Status(
                basic,
                configuration,
                progress);
        });
    }

    public void VisitProgress(
        Action<ulong, ProgressStatus> visitor)
    {
        ExecuteDiagnostic(() =>
        {
            ArgumentNullException.ThrowIfNull(visitor);
            KeyValuePair<ulong, ProgressStatus>[] progress =
                _core.GetProgressStatuses();
            foreach ((ulong id, ProgressStatus status)
                     in progress)
            {
                visitor(id, status);
            }
        });
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

    private void BootstrapMaterialized(
        Peer[] peers)
    {
        Execute(() => BootstrapValidated(peers));
    }

    private void BootstrapValidated(
        Peer[] peers)
    {
        try
        {
            BootstrapValidatedCore(peers);
        }
        catch (Exception exception)
        {
            Fault(exception);
            throw;
        }
    }

    private void BootstrapValidatedCore(
        Peer[] peers)
    {
        if (_lifecycle != RawNodeLifecycle.Fresh)
        {
            ThrowBootstrapPrecondition(
                "Bootstrap requires a never-used RawNode.");
        }

        HardState hardState = _core.HardState;
        ConfigurationStatus configuration =
            _core.GetConfigurationStatus();
        if (_core.Log.LastIndex != 0)
        {
            ThrowBootstrapPrecondition(
                "Bootstrap requires an empty log.");
        }

        if (!IsEmpty(hardState))
        {
            ThrowBootstrapPrecondition(
                "Bootstrap requires empty hard state.");
        }

        if (configuration.Voters.Count != 0
            || configuration.VotersOutgoing.Count != 0
            || configuration.Learners.Count != 0
            || configuration.LearnersNext.Count != 0
            || configuration.AutoLeave
            || _core.Tracker.Progress.Count != 0)
        {
            ThrowBootstrapPrecondition(
                "Bootstrap requires empty membership and progress.");
        }

        if (_outstanding is not null)
        {
            ThrowBootstrapPrecondition(
                "Bootstrap cannot run with an outstanding Ready.");
        }

        _lifecycle = RawNodeLifecycle.Active;
        _core.Bootstrap(peers);
    }

    private bool HasReadyCore()
    {
        if (!_core.AsyncStorageWrites
            && _outstanding is not null)
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
                allowUnstable:
                    !_core.AsyncStorageWrites)
            || _core.HasReadStates;
    }

    private Ready CreateReady()
    {
        bool asyncStorageWrites =
            _core.AsyncStorageWrites;
        if (!asyncStorageWrites
            && _outstanding is not null)
        {
            throw new InvalidOperationException(
                "The outstanding Ready must be advanced before requesting another batch.");
        }

        Activate();
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
                allowUnstable:
                    !asyncStorageWrites),
        ];
        Message[] immediateMessages =
            _core.PeekMessages();
        Message[] afterAppendMessages =
            _core.PeekMessagesAfterAppend();
        ReadState[] readStates =
            _core.PeekReadStates();

        var exposedMessages = new List<Message>(
            immediateMessages.Length
            + afterAppendMessages.Length
            + 2);
        exposedMessages.AddRange(immediateMessages);
        Message[] selfMessages;
        var syntheticMessages = new List<Message>(2);
        if (asyncStorageWrites)
        {
            selfMessages = [];
            if (NeedsStorageAppendMessage(
                    emittedHardState,
                    unstableEntries,
                    snapshot,
                    afterAppendMessages))
            {
                Message append =
                    CreateStorageAppendMessage(
                        emittedHardState,
                        unstableEntries,
                        snapshot,
                        afterAppendMessages);
                exposedMessages.Add(append);
                syntheticMessages.Add(append);
            }

            if (committedEntries.Length > 0)
            {
                Message apply =
                    CreateStorageApplyMessage(
                        committedEntries);
                exposedMessages.Add(apply);
                syntheticMessages.Add(apply);
            }
        }
        else
        {
            exposedMessages.AddRange(
                afterAppendMessages.Where(
                    message => message.To != _core.Id));
            selfMessages =
            [
                .. afterAppendMessages
                    .Where(
                        message =>
                            message.To == _core.Id)
                    .Select(
                        message => message.Clone()),
            ];
        }

        foreach (Message message in syntheticMessages)
        {
            _core.TraceSyntheticMessageSent(message);
        }

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
        ReadyAcknowledgement? acknowledgement =
            asyncStorageWrites
                ? null
                : CreateAcknowledgement(
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
        ulong? appliedIndex =
            committedEntries.Length == 0
                ? null
                : committedEntries[^1].Index;
        if (appliedIndex.HasValue)
        {
            _core.Log.AcceptApplying(
                appliedIndex.Value,
                EntrySizing.EncodedSize(
                    committedEntries),
                allowUnstable:
                    !asyncStorageWrites);
        }

        if (asyncStorageWrites)
        {
            _core.TraceReadyAccepted();
            return ready;
        }

        _outstanding = ready;
        _acknowledgement = acknowledgement
            ?? throw new RaftInvariantException(
                "Synchronous Ready acknowledgement is missing.");
        _core.TraceReadyAccepted();
        return ready;
    }

    private void AdvanceCore(Ready ready)
    {
        if (_core.AsyncStorageWrites)
        {
            throw new NotSupportedException(
                "Advance is replaced by storage response messages when asynchronous storage writes are enabled.");
        }

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

        Activate();
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
            Fault(exception);
            throw;
        }

        _outstanding = null;
        _acknowledgement = null;
    }

    private static bool NeedsStorageAppendMessage(
        HardState? hardState,
        Entry[] entries,
        Snapshot? snapshot,
        Message[] afterAppendMessages)
    {
        return hardState is not null
            || entries.Length > 0
            || snapshot is not null
            || afterAppendMessages.Length > 0;
    }

    private Message CreateStorageAppendMessage(
        HardState? hardState,
        Entry[] entries,
        Snapshot? snapshot,
        Message[] afterAppendMessages)
    {
        var request = new Message
        {
            From = _core.Id,
            To =
                RaftLocalMessageTargets.AppendThread,
            Type = MessageType.MsgStorageAppend,
        };
        request.Entries.Add(
            entries.Select(entry => entry.Clone()));
        if (hardState is not null)
        {
            request.Term = hardState.Term;
            request.Vote = hardState.Vote;
            request.Commit = hardState.Commit;
        }

        if (snapshot is not null)
        {
            request.Snapshot = snapshot.Clone();
        }

        request.Responses.Add(
            afterAppendMessages.Select(
                message => message.Clone()));
        if (_core.Log.HasUnstableEntries
            || snapshot is not null)
        {
            request.Responses.Add(
                CreateStorageAppendResponse(snapshot));
        }

        return request;
    }

    private Message CreateStorageAppendResponse(
        Snapshot? snapshot)
    {
        var response = new Message
        {
            From =
                RaftLocalMessageTargets.AppendThread,
            To = _core.Id,
            Type =
                MessageType.MsgStorageAppendResp,
            Term = _core.Term,
        };
        if (_core.Log.HasUnstableEntries)
        {
            EntryId last = _core.Log.LastEntryId;
            response.Index = last.Index;
            response.LogTerm = last.Term;
        }

        if (snapshot is not null)
        {
            response.Snapshot = snapshot.Clone();
        }

        return response;
    }

    private Message CreateStorageApplyMessage(
        Entry[] entries)
    {
        var request = new Message
        {
            From = _core.Id,
            To = RaftLocalMessageTargets.ApplyThread,
            Type = MessageType.MsgStorageApply,
            Term = 0,
        };
        request.Entries.Add(
            entries.Select(entry => entry.Clone()));

        var response = new Message
        {
            From = RaftLocalMessageTargets.ApplyThread,
            To = _core.Id,
            Type =
                MessageType.MsgStorageApplyResp,
            Term = 0,
        };
        response.Entries.Add(
            entries.Select(entry => entry.Clone()));
        request.Responses.Add(response);
        return request;
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
            Fault(exception);
            throw;
        }
    }

    private void StepStorageResponseCore(
        Message message)
    {
        try
        {
            _core.Step(message);
        }
        catch (Exception exception)
        {
            Fault(exception);
            throw;
        }
    }

    private static bool IsStorageResponse(
        MessageType type)
    {
        return type is
            MessageType.MsgStorageAppendResp
            or MessageType.MsgStorageApplyResp;
    }

    private void ValidateStorageResponse(
        Message message)
    {
        if (!_core.AsyncStorageWrites)
        {
            throw new StorageResponseValidationException(
                "Storage-thread responses require asynchronous storage writes.");
        }

        if (message.To != _core.Id)
        {
            throw new StorageResponseValidationException(
                $"Storage response target {message.To} does not match local node {_core.Id}.");
        }

        switch (message.From)
        {
            case RaftMessageTargets.LocalAppendThread:
                ValidateAppendResponse(message);
                return;
            case RaftMessageTargets.LocalApplyThread:
                ValidateApplyResponse(message);
                return;
            default:
                throw new StorageResponseValidationException(
                    $"Unknown local storage sender {message.From}.");
        }
    }

    private static void ValidateAppendResponse(
        Message message)
    {
        if (message.Type
            != MessageType.MsgStorageAppendResp)
        {
            throw new StorageResponseValidationException(
                $"{RaftLocalMessageTargets.AppendThread} must send MsgStorageAppendResp.");
        }

        if (!message.HasTerm)
        {
            throw new StorageResponseValidationException(
                "MsgStorageAppendResp must carry a present term.");
        }

        if (message.HasIndex != message.HasLogTerm
            || (message.HasIndex && message.Index == 0))
        {
            throw new StorageResponseValidationException(
                "MsgStorageAppendResp must carry a paired nonzero index and log term.");
        }

        if (message.HasVote
            || message.HasCommit
            || message.HasReject
            || message.HasRejectHint
            || message.HasContext
            || message.Entries.Count != 0
            || message.Responses.Count != 0)
        {
            throw new StorageResponseValidationException(
                "MsgStorageAppendResp contains request-only fields.");
        }

        if (message.Snapshot is not null
            && (message.Snapshot.Metadata is null
                || message.Snapshot.Metadata.Index == 0))
        {
            throw new StorageResponseValidationException(
                "MsgStorageAppendResp snapshot must be nonempty.");
        }
    }

    private static void ValidateApplyResponse(
        Message message)
    {
        if (message.Type
            != MessageType.MsgStorageApplyResp)
        {
            throw new StorageResponseValidationException(
                $"{RaftLocalMessageTargets.ApplyThread} must send MsgStorageApplyResp.");
        }

        if (!message.HasTerm || message.Term != 0)
        {
            throw new StorageResponseValidationException(
                "MsgStorageApplyResp must carry a present zero term.");
        }

        if (message.HasIndex
            || message.HasLogTerm
            || message.HasVote
            || message.HasCommit
            || message.HasReject
            || message.HasRejectHint
            || message.HasContext
            || message.Snapshot is not null
            || message.Responses.Count != 0
            || message.Entries.Count == 0)
        {
            throw new StorageResponseValidationException(
                "MsgStorageApplyResp has an invalid field shape.");
        }

        ulong? previous = null;
        foreach (Entry entry in message.Entries)
        {
            if (entry is null
                || entry.Index == 0
                || (previous.HasValue
                    && (previous.Value == ulong.MaxValue
                        || entry.Index
                            != previous.Value + 1)))
            {
                throw new StorageResponseValidationException(
                    "MsgStorageApplyResp entries must be non-null, nonzero, and contiguous.");
            }

            previous = entry.Index;
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

    private void Activate()
    {
        if (_lifecycle == RawNodeLifecycle.Fresh)
        {
            _lifecycle = RawNodeLifecycle.Active;
        }
    }

    private void ThrowBootstrapPrecondition(
        string message)
    {
        var exception =
            new InvalidOperationException(message);
        Fault(exception);
        throw exception;
    }

    private void Fault(Exception exception)
    {
        _fault ??= exception;
        _lifecycle = RawNodeLifecycle.Faulted;
        _outstanding = null;
        _acknowledgement = null;
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

    private void Execute(Action action)
    {
        EnterOperation(allowFaulted: false);
        try
        {
            action();
        }
        catch (RaftTracingException exception)
        {
            Fault(exception);
            throw;
        }
        finally
        {
            _operationInProgress = false;
        }
    }

    private T Execute<T>(Func<T> action)
    {
        EnterOperation(allowFaulted: false);
        try
        {
            return action();
        }
        catch (RaftTracingException exception)
        {
            Fault(exception);
            throw;
        }
        finally
        {
            _operationInProgress = false;
        }
    }

    private void ExecuteDiagnostic(Action action)
    {
        EnterOperation(allowFaulted: true);
        try
        {
            action();
        }
        finally
        {
            _operationInProgress = false;
        }
    }

    private T ExecuteDiagnostic<T>(Func<T> action)
    {
        EnterOperation(allowFaulted: true);
        try
        {
            return action();
        }
        finally
        {
            _operationInProgress = false;
        }
    }

    private void EnterOperation(bool allowFaulted)
    {
        if (_operationInProgress)
        {
            throw new InvalidOperationException(
                "RawNode does not allow same-instance reentry.");
        }

        if (!allowFaulted)
        {
            EnsureUsable();
        }

        _operationInProgress = true;
    }

    private static Peer[] MaterializePeers(
        IEnumerable<Peer> peers)
    {
        ArgumentNullException.ThrowIfNull(peers);
        var materialized = new List<Peer>();
        var ids = new HashSet<ulong>();
        foreach (Peer peer in peers)
        {
            if (peer is null)
            {
                throw new ArgumentException(
                    "Peers cannot contain null values.",
                    nameof(peers));
            }

            if (peer.Id == RaftMessageTargets.None
                || RaftMessageTargets.IsLocal(peer.Id))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(peers),
                    peer.Id,
                    "Peer IDs must be nonzero remote node IDs.");
            }

            if (!ids.Add(peer.Id))
            {
                throw new ArgumentException(
                    $"Peer ID {peer.Id} is duplicated.",
                    nameof(peers));
            }

            materialized.Add(peer);
        }

        if (materialized.Count == 0)
        {
            throw new ArgumentException(
                "Bootstrap requires at least one peer.",
                nameof(peers));
        }

        return [.. materialized];
    }

    private static bool IsEmpty(HardState state)
    {
        return state.Term == 0
            && state.Vote == 0
            && state.Commit == 0;
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

    private enum RawNodeLifecycle
    {
        Fresh,
        Active,
        Faulted,
    }

    private sealed record ReadyAcknowledgement(
        IReadOnlyList<Message> SelfMessages,
        EntryId? StableEntry,
        ulong? SnapshotIndex,
        ulong? AppliedIndex,
        ulong AppliedEncodedSize,
        ulong AppliedPayloadSize);
}
