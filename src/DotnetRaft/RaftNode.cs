using System.Threading.Channels;

using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft;

public sealed class RaftNode : IRaftNode
{
    private const int Running = 0;
    private const int StopRequested = 1;
    private const int Stopped = 2;
    private const int Faulted = 3;
    private const int TickCapacity = 128;

    private static readonly AsyncLocal<RaftNode?>
        ActiveOwner = new();

    private readonly object _claimGate = new();
    private readonly Channel<ICommand> _commands;
    private readonly Channel<bool> _ticks;
    private readonly Channel<bool> _wake;
    private readonly CancellationTokenSource _stopSource =
        new();
    private readonly LinkedList<ProposalRequest>
        _blockedProposals = new();
    private readonly RawNode _rawNode;
    private readonly bool _asyncStorageWrites;
    private readonly IRaftLogger _logger;
    private readonly ulong _id;
    private readonly Task _runner;
    private ReadyRequest? _readyWaiter;
    private Ready? _outstandingReady;
    private Exception? _terminalCause;
    private Status? _terminalStatus;
    private bool _hasObservedLocalProgress;
    private bool _removedFromProgress;
    private int _missedTicks;
    private int _schedulerStart;
    private int _state;

    private RaftNode(
        RawNode rawNode,
        IRaftLogger logger)
    {
        ArgumentNullException.ThrowIfNull(rawNode);
        ArgumentNullException.ThrowIfNull(logger);

        _rawNode = rawNode;
        _asyncStorageWrites =
            rawNode.AsyncStorageWrites;
        _logger = logger;
        _id = ExecuteOwned(
            () => _rawNode.GetBasicStatus().Id);
        ExecuteOwned(RefreshLocalProgress);

        _commands =
            Channel.CreateUnbounded<ICommand>(
                new UnboundedChannelOptions
                {
                    AllowSynchronousContinuations = false,
                    SingleReader = true,
                    SingleWriter = false,
                });
        _ticks =
            Channel.CreateBounded<bool>(
                new BoundedChannelOptions(
                    TickCapacity)
                {
                    AllowSynchronousContinuations = false,
                    FullMode =
                        BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = false,
                });
        _wake =
            Channel.CreateBounded<bool>(
                new BoundedChannelOptions(1)
                {
                    AllowSynchronousContinuations = false,
                    FullMode =
                        BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = false,
                });
        _runner = Task.Run(RunAsync);
    }

    public Task Completion
    {
        get
        {
            EnsureNotReentrant();
            return _runner;
        }
    }

    public Status? TerminalStatus =>
        Volatile.Read(ref _terminalStatus);

    public static RaftNode Start(
        RaftConfig config,
        IEnumerable<Peer> peers)
    {
        ArgumentNullException.ThrowIfNull(config);
        IRaftLogger logger =
            config.Logger ?? NullRaftLogger.Instance;
        return new RaftNode(
            RawNode.Start(config, peers),
            logger);
    }

    public static RaftNode Restart(
        RaftConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        IRaftLogger logger =
            config.Logger ?? NullRaftLogger.Instance;
        return new RaftNode(
            RawNode.Restart(config),
            logger);
    }

    public void Tick()
    {
        EnsureNotReentrant();
        if (Volatile.Read(ref _state) != Running)
        {
            return;
        }

        if (!_ticks.Writer.TryWrite(true))
        {
            Interlocked.Exchange(
                ref _missedTicks,
                1);
        }

        SignalWake();
    }

    public ValueTask CampaignAsync(
        CancellationToken cancellationToken = default)
    {
        return QueueOperation(
            rawNode => rawNode.Campaign(),
            cancellationToken);
    }

    public ValueTask ProposeAsync(
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        byte[] owned = data.ToArray();
        return QueueProposal(
            rawNode => rawNode.Propose(owned),
            cancellationToken);
    }

    public ValueTask ProposeConfChangeAsync(
        ProtocolConfChange change,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(change);
        ProtocolConfChange owned = change.Clone();
        return QueueProposal(
            rawNode =>
                rawNode.ProposeConfChange(owned),
            cancellationToken);
    }

    public ValueTask ProposeConfChangeAsync(
        ConfChangeV2 change,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(change);
        ConfChangeV2 owned = change.Clone();
        return QueueProposal(
            rawNode =>
                rawNode.ProposeConfChange(owned),
            cancellationToken);
    }

    public ValueTask StepAsync(
        Message message,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        Message owned = message.Clone();

        if (owned.Type is
            MessageType.MsgStorageAppendResp
            or MessageType.MsgStorageApplyResp)
        {
            if (!_asyncStorageWrites
                && RaftMessageTargets.IsLocal(
                    owned.From))
            {
                return QueueOperation(
                    _ => throw new NotSupportedException(
                        "Storage-thread responses require asynchronous storage writes."),
                    cancellationToken);
            }

            return QueueOperation(
                rawNode => rawNode.Step(owned),
                cancellationToken);
        }

        if (RaftMessageTargets.IsLocal(owned.From))
        {
            return _asyncStorageWrites
                ? QueueOperation(
                    rawNode => rawNode.Step(owned),
                    cancellationToken)
                : QueueOperation(
                    _ => throw new NotSupportedException(
                        "Storage-thread responses require asynchronous storage writes."),
                    cancellationToken);
        }

        if (MessageClassifier.IsLocal(owned.Type))
        {
            return QueueOperation(
                _ =>
                {
                },
                cancellationToken);
        }

        Action<RawNode> step = rawNode =>
        {
            if (MessageClassifier.IsResponse(owned.Type)
                && !IsKnownPeer(
                    rawNode,
                    owned.From))
            {
                return;
            }

            if (owned.Type == MessageType.MsgProp)
            {
                owned.From = _id;
            }
            rawNode.Step(owned);
        };
        return owned.Type == MessageType.MsgProp
            ? QueueProposal(
                step,
                cancellationToken)
            : QueueOperation(
                step,
                cancellationToken);
    }

    public ValueTask ForgetLeaderAsync(
        CancellationToken cancellationToken = default)
    {
        return QueueOperation(
            rawNode => rawNode.ForgetLeader(),
            cancellationToken);
    }

    public ValueTask ReadIndexAsync(
        ReadOnlyMemory<byte> context,
        CancellationToken cancellationToken = default)
    {
        byte[] owned = context.ToArray();
        return QueueOperation(
            rawNode => rawNode.ReadIndex(owned),
            cancellationToken);
    }

    public ValueTask TransferLeadershipAsync(
        ulong transferee,
        CancellationToken cancellationToken = default)
    {
        return QueueOperation(
            rawNode =>
                rawNode.TransferLeader(transferee),
            cancellationToken);
    }

    public ValueTask ReportUnreachableAsync(
        ulong id,
        CancellationToken cancellationToken = default)
    {
        return QueueOperation(
            rawNode =>
                rawNode.ReportUnreachable(id),
            cancellationToken);
    }

    public ValueTask ReportSnapshotAsync(
        ulong id,
        SnapshotStatus status,
        CancellationToken cancellationToken = default)
    {
        return QueueOperation(
            rawNode =>
                rawNode.ReportSnapshot(id, status),
            cancellationToken);
    }

    public ValueTask<Ready> WaitForReadyAsync(
        CancellationToken cancellationToken = default)
    {
        EnsureNotReentrant();
        Exception? unavailable =
            GetUnavailableException();
        if (unavailable is not null)
        {
            return ValueTask.FromException<Ready>(
                unavailable);
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<Ready>(
                cancellationToken);
        }

        var request = new ReadyRequest(
            CancelRequest,
            cancellationToken);
        Enqueue(request);
        return new ValueTask<Ready>(request.Task);
    }

    public ValueTask AdvanceAsync(
        CancellationToken cancellationToken = default)
    {
        if (_asyncStorageWrites)
        {
            return QueueOperation(
                _ => throw new NotSupportedException(
                    "Advance is replaced by storage response messages when asynchronous storage writes are enabled."),
                cancellationToken);
        }

        EnsureNotReentrant();
        Exception? unavailable =
            GetUnavailableException();
        if (unavailable is not null)
        {
            return ValueTask.FromException(
                unavailable);
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled(
                cancellationToken);
        }

        var request = new AdvanceRequest(
            CancelRequest,
            cancellationToken);
        Enqueue(request);
        return new ValueTask(request.Task);
    }

    public ValueTask<ConfState> ApplyConfChangeAsync(
        ProtocolConfChange change,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(change);
        ProtocolConfChange owned = change.Clone();
        return QueueOperation(
            rawNode =>
                rawNode.ApplyConfChange(owned),
            cancellationToken);
    }

    public ValueTask<ConfState> ApplyConfChangeAsync(
        ConfChangeV2 change,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(change);
        ConfChangeV2 owned = change.Clone();
        return QueueOperation(
            rawNode =>
                rawNode.ApplyConfChange(owned),
            cancellationToken);
    }

    public ValueTask<Status> GetStatusAsync(
        CancellationToken cancellationToken = default)
    {
        return QueueOperation(
            rawNode => rawNode.GetStatus(),
            cancellationToken);
    }

    public ValueTask StopAsync()
    {
        EnsureNotReentrant();
        var requestStop = false;
        lock (_claimGate)
        {
            if (_state == Running)
            {
                _state = StopRequested;
                requestStop = true;
            }
        }

        if (requestStop)
        {
            _commands.Writer.TryComplete();
            _ticks.Writer.TryComplete();
            _wake.Writer.TryComplete();
            _stopSource.Cancel();
        }

        return new ValueTask(_runner);
    }

    public ValueTask DisposeAsync()
    {
        return StopAsync();
    }

    private async Task RunAsync()
    {
        RaftNodeFaultedException? terminalFault = null;
        try
        {
            while (Volatile.Read(ref _state) == Running)
            {
                TryEmitMissedTickWarning();
                TryPublishReady();

                if (Volatile.Read(ref _state)
                    != Running)
                {
                    break;
                }

                var didWork = false;
                int start = _schedulerStart;
                _schedulerStart =
                    (_schedulerStart + 1) % 3;
                for (var offset = 0;
                     offset < 3;
                     offset++)
                {
                    if (Volatile.Read(ref _state)
                        != Running)
                    {
                        break;
                    }

                    int lane = (start + offset) % 3;
                    didWork |= lane switch
                    {
                        0 => TryProcessCommand(),
                        1 => TryProcessProposal(),
                        2 => TryProcessTick(),
                        _ => false,
                    };
                }

                if (didWork)
                {
                    continue;
                }

                await WaitForWorkAsync(
                        _stopSource.Token)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
            when (Volatile.Read(ref _state)
                  == StopRequested)
        {
        }
        catch (ChannelClosedException)
            when (Volatile.Read(ref _state)
                  == StopRequested)
        {
        }
        catch (Exception exception)
        {
            terminalFault =
                exception as RaftNodeFaultedException
                ?? new RaftNodeFaultedException(
                    exception);
            _terminalCause =
                terminalFault.InnerException
                ?? terminalFault;
            lock (_claimGate)
            {
                _state = Faulted;
            }

            throw terminalFault;
        }
        finally
        {
            CaptureTerminalStatus();
            Exception terminal = terminalFault is null
                ? new RaftNodeStoppedException()
                : terminalFault;
            _commands.Writer.TryComplete();
            _ticks.Writer.TryComplete();
            _wake.Writer.TryComplete();
            FailPending(terminal);
            _outstandingReady = null;

            if (terminalFault is null)
            {
                lock (_claimGate)
                {
                    if (_state != Faulted)
                    {
                        _state = Stopped;
                    }
                }
            }
        }
    }

    private bool TryProcessCommand()
    {
        if (!_commands.Reader.TryRead(
                out ICommand? command))
        {
            return false;
        }

        switch (command)
        {
            case ReadyRequest ready:
                ProcessReadyRequest(ready);
                return true;
            case AdvanceRequest advance:
                ProcessAdvanceRequest(advance);
                return true;
            default:
                command.Execute(this);
                return true;
        }
    }

    private bool TryProcessProposal()
    {
        lock (_claimGate)
        {
            if (_blockedProposals.First is null)
            {
                return false;
            }
        }

        if (!CanProcessProposals())
        {
            return false;
        }

        while (true)
        {
            ProposalRequest request;
            lock (_claimGate)
            {
                if (_state != Running
                    || _blockedProposals.First is null)
                {
                    return false;
                }

                request =
                    _blockedProposals.First.Value;
                _blockedProposals.RemoveFirst();
                request.QueueNode = null;
            }

            if (!request.IsWaiting)
            {
                request.ReleaseRegistration();
                request.ReleasePayload();
                continue;
            }

            request.Execute(this);
            return true;
        }
    }

    private bool TryProcessTick()
    {
        if (!_ticks.Reader.TryRead(out _))
        {
            return false;
        }

        if (!TryClaimOwnerWork())
        {
            return true;
        }

        try
        {
            ExecuteOwned(_rawNode.Tick);
        }
        catch (Exception exception)
        {
            throw new RaftNodeFaultedException(
                exception);
        }

        return true;
    }

    private void TryPublishReady()
    {
        ReadyRequest? request = _readyWaiter;
        if (request is null
            || _outstandingReady is not null)
        {
            return;
        }

        var hasReady = false;
        try
        {
            hasReady = ExecuteOwned(
                _rawNode.HasReady);
        }
        catch (Exception exception)
        {
            throw new RaftNodeFaultedException(
                exception);
        }

        if (!hasReady)
        {
            return;
        }

        if (!TryClaimForDispatch(request))
        {
            _readyWaiter = null;
            request.ReleaseRegistration();
            request.Fail(
                GetUnavailableException()
                ?? new RaftNodeStoppedException());
            return;
        }

        _readyWaiter = null;
        try
        {
            Ready ready = ExecuteOwned(
                _rawNode.Ready);
            if (!_asyncStorageWrites)
            {
                _outstandingReady = ready;
            }

            request.Succeed(ready);
        }
        catch (Exception exception)
        {
            request.Fail(exception);
            throw new RaftNodeFaultedException(
                exception);
        }
    }

    private void ProcessReadyRequest(
        ReadyRequest request)
    {
        if (!request.IsWaiting)
        {
            request.ReleaseRegistration();
            return;
        }

        Exception? failure = null;
        var stored = false;
        var canceled = false;
        lock (_claimGate)
        {
            if (_state != Running)
            {
                failure =
                    GetUnavailableExceptionLocked();
            }
            else if (!request.IsWaiting)
            {
                canceled = true;
            }
            else if (_readyWaiter is null
                && (_asyncStorageWrites
                    || _outstandingReady is null))
            {
                _readyWaiter = request;
                stored = true;
            }
        }

        if (failure is not null)
        {
            request.Fail(failure);
            return;
        }

        if (canceled)
        {
            request.ReleaseRegistration();
            return;
        }

        if (stored)
        {
            return;
        }

        if (TryClaimForDispatch(request))
        {
            request.Fail(
                new InvalidOperationException(
                    "Only one Ready wait or outstanding batch is allowed."));
        }
        else
        {
            request.Fail(
                GetUnavailableException()
                ?? new RaftNodeStoppedException());
        }
    }

    private void ProcessAdvanceRequest(
        AdvanceRequest request)
    {
        if (!TryClaimForDispatch(request))
        {
            request.ReleaseRegistration();
            request.Fail(
                GetUnavailableException()
                ?? new RaftNodeStoppedException());
            return;
        }

        if (_outstandingReady is null)
        {
            request.Fail(
                new InvalidOperationException(
                    "No Ready is awaiting advancement."));
            return;
        }

        try
        {
            Ready ready = _outstandingReady;
            ExecuteOwned(() =>
            {
                _rawNode.Advance(ready);
                RefreshLocalProgress();
            });
            _outstandingReady = null;
            request.Succeed();
        }
        catch (Exception exception)
        {
            request.Fail(exception);
            throw new RaftNodeFaultedException(
                exception);
        }
    }

    private bool CanProcessProposals()
    {
        if (_removedFromProgress)
        {
            return false;
        }

        return ExecuteOwned(
            () => _rawNode
                .GetBasicStatus()
                .LeaderId != 0);
    }

    private void TryEmitMissedTickWarning()
    {
        if (Interlocked.Exchange(
                ref _missedTicks,
                0) == 0
            || !TryClaimOwnerWork())
        {
            return;
        }

        try
        {
            ExecuteOwned(() =>
                RaftLogging.Write(
                    _logger,
                    RaftLogLevel.Warning,
                    $"{_id:x} missed one or more ticks because the Node loop was busy."));
        }
        catch (Exception exception)
        {
            throw new RaftNodeFaultedException(
                exception);
        }
    }

    private async Task WaitForWorkAsync(
        CancellationToken cancellationToken)
    {
        await _wake.Reader
            .ReadAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private void FailPending(Exception exception)
    {
        _readyWaiter?.Fail(exception);
        _readyWaiter = null;

        while (_blockedProposals.First is not null)
        {
            ProposalRequest proposal =
                _blockedProposals.First.Value;
            _blockedProposals.RemoveFirst();
            proposal.QueueNode = null;
            proposal.ReleasePayload();
            proposal.Fail(exception);
        }

        while (_commands.Reader.TryRead(
                   out ICommand? command))
        {
            command.Fail(exception);
        }

        while (_ticks.Reader.TryRead(out _))
        {
        }
    }

    private void CaptureTerminalStatus()
    {
        try
        {
            Volatile.Write(
                ref _terminalStatus,
                ExecuteOwned(
                    _rawNode.GetStatus));
        }
        catch
        {
        }
    }

    private void RefreshLocalProgress()
    {
        var present = false;
        _rawNode.VisitProgress(
            (id, _) => present |= id == _id);
        if (present)
        {
            _hasObservedLocalProgress = true;
            _removedFromProgress = false;
        }
        else if (_hasObservedLocalProgress)
        {
            _removedFromProgress = true;
        }
    }

    private static bool IsKnownPeer(
        RawNode rawNode,
        ulong id)
    {
        var known = false;
        rawNode.VisitProgress(
            (peerId, _) => known |= peerId == id);
        return known;
    }

    private static bool IsExpectedRequestException(
        Exception exception)
    {
        return exception is
            ProposalDroppedException
            or ArgumentException
            or StorageResponseValidationException
            or NotSupportedException;
    }

    private bool TryClaimForDispatch(
        NodeRequest request)
    {
        bool claimed;
        lock (_claimGate)
        {
            claimed = _state == Running
                && request.TryClaimState();
        }

        if (claimed)
        {
            request.ReleaseRegistration();
        }

        return claimed;
    }

    private bool TryClaimOwnerWork()
    {
        lock (_claimGate)
        {
            return _state == Running;
        }
    }

    private void Enqueue(ICommand command)
    {
        if (!_commands.Writer.TryWrite(command))
        {
            command.Fail(
                GetUnavailableException()
                ?? new RaftNodeStoppedException());
            return;
        }

        SignalWake();
    }

    private ValueTask QueueOperation(
        Action<RawNode> operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);
        EnsureNotReentrant();
        Exception? unavailable =
            GetUnavailableException();
        if (unavailable is not null)
        {
            return ValueTask.FromException(
                unavailable);
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled(
                cancellationToken);
        }

        var request = new OperationRequest(
            operation,
            CancelRequest,
            cancellationToken);
        Enqueue(request);
        return new ValueTask(request.Task);
    }

    private ValueTask<T> QueueOperation<T>(
        Func<RawNode, T> operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);
        EnsureNotReentrant();
        Exception? unavailable =
            GetUnavailableException();
        if (unavailable is not null)
        {
            return ValueTask.FromException<T>(
                unavailable);
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<T>(
                cancellationToken);
        }

        var request = new OperationRequest<T>(
            operation,
            CancelRequest,
            cancellationToken);
        Enqueue(request);
        return new ValueTask<T>(request.Task);
    }

    private ValueTask QueueProposal(
        Action<RawNode> operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);
        EnsureNotReentrant();
        Exception? unavailable =
            GetUnavailableException();
        if (unavailable is not null)
        {
            return ValueTask.FromException(
                unavailable);
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled(
                cancellationToken);
        }

        var request = new ProposalRequest(
            operation,
            CancelRequest,
            cancellationToken);
        EnqueueProposal(request);
        return new ValueTask(request.Task);
    }

    private void EnqueueProposal(
        ProposalRequest request)
    {
        Exception? failure = null;
        var enqueued = false;
        lock (_claimGate)
        {
            if (_state != Running)
            {
                failure =
                    GetUnavailableExceptionLocked();
            }
            else if (request.IsWaiting)
            {
                request.QueueNode =
                    _blockedProposals.AddLast(
                        request);
                enqueued = true;
            }
        }

        if (failure is not null)
        {
            request.ReleasePayload();
            request.Fail(failure);
            return;
        }

        if (!enqueued)
        {
            request.ReleaseRegistration();
            request.ReleasePayload();
            return;
        }

        SignalWake();
    }

    private void CancelRequest(
        NodeRequest request)
    {
        var canceled = false;
        lock (_claimGate)
        {
            if (_state != Running
                || !request.TryCancelState())
            {
                return;
            }

            canceled = true;
            if (ReferenceEquals(
                    _readyWaiter,
                    request))
            {
                _readyWaiter = null;
            }

            if (request is ProposalRequest proposal
                && proposal.QueueNode is not null)
            {
                _blockedProposals.Remove(
                    proposal.QueueNode);
                proposal.QueueNode = null;
                proposal.ReleasePayload();
            }
        }

        if (!canceled)
        {
            return;
        }

        request.UnregisterCancellation();
        request.CompleteCancellation();
        SignalWake();
    }

    private void SignalWake()
    {
        _wake.Writer.TryWrite(true);
    }

    private Exception? GetUnavailableException()
    {
        int state = Volatile.Read(ref _state);
        return state switch
        {
            Running => null,
            Faulted => new RaftNodeFaultedException(
                _terminalCause
                ?? new InvalidOperationException(
                    "The Raft node faulted.")),
            _ => new RaftNodeStoppedException(),
        };
    }

    private Exception GetUnavailableExceptionLocked()
    {
        return _state == Faulted
            ? new RaftNodeFaultedException(
                _terminalCause
                ?? new InvalidOperationException(
                    "The Raft node faulted."))
            : new RaftNodeStoppedException();
    }

    private void EnsureNotReentrant()
    {
        if (ReferenceEquals(
                ActiveOwner.Value,
                this))
        {
            throw new InvalidOperationException(
                "RaftNode methods cannot re-enter the same node from a trace or logger callback.");
        }
    }

    private T ExecuteOwned<T>(Func<T> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        RaftNode? previous = ActiveOwner.Value;
        ActiveOwner.Value = this;
        try
        {
            return action();
        }
        finally
        {
            ActiveOwner.Value = previous;
        }
    }

    private void ExecuteOwned(Action action)
    {
        ArgumentNullException.ThrowIfNull(action);
        RaftNode? previous = ActiveOwner.Value;
        ActiveOwner.Value = this;
        try
        {
            action();
        }
        finally
        {
            ActiveOwner.Value = previous;
        }
    }

    private interface ICommand
    {
        void Execute(RaftNode owner);

        void Fail(Exception exception);
    }

    private abstract class NodeRequest : ICommand
    {
        private const int Waiting = 0;
        private const int Claimed = 1;
        private const int Completed = 2;

        private readonly object _gate = new();
        private readonly Action<NodeRequest>
            _cancelRequest;
        private readonly CancellationToken _cancellationToken;
        private readonly CancellationTokenRegistration
            _registration;
        private int _requestState;

        protected NodeRequest(
            Action<NodeRequest> cancelRequest,
            CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
            _cancelRequest = cancelRequest;
            if (cancellationToken.CanBeCanceled)
            {
                _registration =
                    cancellationToken.Register(
                        static state =>
                            ((NodeRequest)state!)
                            .RequestCancellation(),
                        this);
            }
        }

        internal bool IsWaiting
        {
            get
            {
                lock (_gate)
                {
                    return _requestState == Waiting;
                }
            }
        }

        internal bool TryClaimState()
        {
            lock (_gate)
            {
                if (_requestState != Waiting)
                {
                    return false;
                }

                _requestState = Claimed;
                return true;
            }
        }

        internal bool TryCancelState()
        {
            lock (_gate)
            {
                if (_requestState != Waiting)
                {
                    return false;
                }

                _requestState = Completed;
                return true;
            }
        }

        internal void ReleaseRegistration()
        {
            _registration.Dispose();
        }

        internal void UnregisterCancellation()
        {
            _registration.Unregister();
        }

        internal void CompleteCancellation()
        {
            CompleteCanceled(_cancellationToken);
        }

        public abstract void Execute(RaftNode owner);

        public void Fail(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);
            var complete = false;
            lock (_gate)
            {
                if (_requestState != Completed)
                {
                    _requestState = Completed;
                    complete = true;
                }
            }

            _registration.Dispose();
            if (complete)
            {
                CompleteException(exception);
            }
        }

        protected bool TryComplete()
        {
            lock (_gate)
            {
                if (_requestState != Claimed)
                {
                    return false;
                }

                _requestState = Completed;
                return true;
            }
        }

        protected abstract void CompleteCanceled(
            CancellationToken cancellationToken);

        protected abstract void CompleteException(
            Exception exception);

        private void RequestCancellation()
        {
            _cancelRequest(this);
        }
    }

    private abstract class NodeRequest<T> : NodeRequest
    {
        private readonly TaskCompletionSource<T> _completion =
            new(
                TaskCreationOptions
                    .RunContinuationsAsynchronously);

        protected NodeRequest(
            Action<NodeRequest> cancelRequest,
            CancellationToken cancellationToken)
            : base(cancelRequest, cancellationToken)
        {
        }

        internal Task<T> Task => _completion.Task;

        internal void Succeed(T result)
        {
            if (TryComplete())
            {
                _completion.TrySetResult(result);
            }
        }

        protected override void CompleteCanceled(
            CancellationToken cancellationToken)
        {
            _completion.TrySetCanceled(
                cancellationToken);
        }

        protected override void CompleteException(
            Exception exception)
        {
            _completion.TrySetException(exception);
        }
    }

    private abstract class VoidNodeRequest : NodeRequest
    {
        private readonly TaskCompletionSource _completion =
            new(
                TaskCreationOptions
                    .RunContinuationsAsynchronously);

        protected VoidNodeRequest(
            Action<NodeRequest> cancelRequest,
            CancellationToken cancellationToken)
            : base(cancelRequest, cancellationToken)
        {
        }

        internal Task Task => _completion.Task;

        internal void Succeed()
        {
            if (TryComplete())
            {
                _completion.TrySetResult();
            }
        }

        protected override void CompleteCanceled(
            CancellationToken cancellationToken)
        {
            _completion.TrySetCanceled(
                cancellationToken);
        }

        protected override void CompleteException(
            Exception exception)
        {
            _completion.TrySetException(exception);
        }
    }

    private class OperationRequest : VoidNodeRequest
    {
        internal OperationRequest(
            Action<RawNode> operation,
            Action<NodeRequest> cancelRequest,
            CancellationToken cancellationToken)
            : base(cancelRequest, cancellationToken)
        {
            Operation = operation;
        }

        internal Action<RawNode>? Operation
        {
            get;
            set;
        }

        public override void Execute(RaftNode owner)
        {
            if (!owner.TryClaimForDispatch(this))
            {
                ReleaseRegistration();
                if (this is ProposalRequest proposal)
                {
                    proposal.ReleasePayload();
                }

                Fail(
                    owner.GetUnavailableException()
                    ?? new RaftNodeStoppedException());
                return;
            }

            try
            {
                Action<RawNode> operation =
                    Operation
                    ?? throw new InvalidOperationException(
                        "The queued operation payload is unavailable.");
                Operation = null;
                owner.ExecuteOwned(() =>
                {
                    operation(owner._rawNode);
                    owner.RefreshLocalProgress();
                });
                Succeed();
            }
            catch (Exception exception)
            {
                Fail(exception);
                if (!IsExpectedRequestException(
                        exception)
                    || owner._rawNode.IsFaulted)
                {
                    throw new RaftNodeFaultedException(
                        exception);
                }
            }
        }
    }

    private class OperationRequest<T> : NodeRequest<T>
    {
        internal OperationRequest(
            Func<RawNode, T> operation,
            Action<NodeRequest> cancelRequest,
            CancellationToken cancellationToken)
            : base(cancelRequest, cancellationToken)
        {
            Operation = operation;
        }

        internal Func<RawNode, T>? Operation
        {
            get;
            set;
        }

        public override void Execute(RaftNode owner)
        {
            if (!owner.TryClaimForDispatch(this))
            {
                ReleaseRegistration();
                Fail(
                    owner.GetUnavailableException()
                    ?? new RaftNodeStoppedException());
                return;
            }

            try
            {
                Func<RawNode, T> operation =
                    Operation
                    ?? throw new InvalidOperationException(
                        "The queued operation payload is unavailable.");
                Operation = null;
                T result = owner.ExecuteOwned(() =>
                {
                    T value = operation(owner._rawNode);
                    owner.RefreshLocalProgress();
                    return value;
                });
                Succeed(result);
            }
            catch (Exception exception)
            {
                Fail(exception);
                if (!IsExpectedRequestException(
                        exception)
                    || owner._rawNode.IsFaulted)
                {
                    throw new RaftNodeFaultedException(
                        exception);
                }
            }
        }

    }

    private sealed class ProposalRequest(
        Action<RawNode> operation,
        Action<NodeRequest> cancelRequest,
        CancellationToken cancellationToken)
        : OperationRequest(
            operation,
            cancelRequest,
            cancellationToken)
    {
        internal LinkedListNode<ProposalRequest>?
            QueueNode
        {
            get;
            set;
        }

        internal void ReleasePayload()
        {
            Operation = null;
        }
    }

    private sealed class ReadyRequest(
        Action<NodeRequest> cancelRequest,
        CancellationToken cancellationToken)
        : NodeRequest<Ready>(
            cancelRequest,
            cancellationToken)
    {
        public override void Execute(RaftNode owner)
        {
            owner.ProcessReadyRequest(this);
        }
    }

    private sealed class AdvanceRequest(
        Action<NodeRequest> cancelRequest,
        CancellationToken cancellationToken)
        : VoidNodeRequest(
            cancelRequest,
            cancellationToken)
    {
        public override void Execute(RaftNode owner)
        {
            owner.ProcessAdvanceRequest(this);
        }
    }

}
