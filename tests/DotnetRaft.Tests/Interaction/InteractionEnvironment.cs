using System.Collections.ObjectModel;
using System.Globalization;
using System.Text;

using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Read;
using DotnetRaft.Storage;

using static DotnetRaft.Tests.AsyncStorage.AsyncStorageTestSupport;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Interaction;

internal sealed class InteractionEnvironment
{
    private const int DefaultStabilizationLimit = 100_000;
    private readonly int _stabilizationLimit;
    private readonly List<InteractionNode> _nodes = [];
    private readonly List<InteractionDelivery> _deliveries = [];
    private readonly HashSet<ulong> _crashed = [];
    private readonly InteractionOutput _output = new();

    internal InteractionEnvironment(
        int stabilizationLimit =
            DefaultStabilizationLimit)
    {
        if (stabilizationLimit <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(stabilizationLimit),
                stabilizationLimit,
                "Stabilization limit must be positive.");
        }

        _stabilizationLimit = stabilizationLimit;
    }

    internal IReadOnlyList<InteractionNode> Nodes =>
        new ReadOnlyCollection<InteractionNode>(
            _nodes);

    internal IReadOnlyList<Message> QueuedMessages =>
        Array.AsReadOnly(
            _deliveries
                .Select(delivery => delivery.Frame.Clone())
                .ToArray());

    internal string CurrentOutput =>
        _output.ToString();

    internal Predicate<Message>? NetworkLinkIsOpen { get; set; }

    internal Action<ulong, Entry>? EntryApplied { get; init; }
    internal Action<ulong, Entry>? EntryDelivered { get; init; }
    internal Action<ulong, ReadState>? ReadStateDelivered { get; init; }
    internal IRaftTraceSink? TraceSink { get; init; }

    internal string Handle(
        string directive,
        string input,
        string file = "<command>",
        int lineNumber = 1)
    {
        _output.Reset();
        try
        {
            InteractionDirective parsed =
                InteractionDirective.Parse(
                    directive,
                    file,
                    lineNumber);
            try
            {
                Execute(parsed, input);
            }
            catch (FormatException exception)
            {
                throw new FormatException(
                    $"{file}:{lineNumber}: {exception.Message}",
                    exception);
            }
        }
        catch (Exception exception)
        {
            if (_output.IsQuiet)
            {
                return exception.Message;
            }

            _output.Write(exception.Message);
        }

        return _output.Length == 0
            ? "ok"
            : _output.ToString();
    }

    internal void AddNodes(
        int count,
        InteractionNodeOptions options)
    {
        if (count <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(count),
                count,
                "Node count must be positive.");
        }

        ArgumentNullException.ThrowIfNull(options);
        if (options.SnapshotIndex == 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.SnapshotIndex,
                "Snapshot index must be greater than one when specified.");
        }

        ulong[] voters = [.. options.Voters];
        ulong[] learners = [.. options.Learners];
        for (var offset = 0; offset < count; offset++)
        {
            ulong id = checked(
                (ulong)_nodes.Count + 1);
            var storage = new MemoryStorage(ProtobufRaftCodec.Instance);
            Snapshot applicationSnapshot =
                ProtocolDefaults.EnsureSnapshot(
                    new Snapshot());
            if (options.SnapshotIndex > 0)
            {
                var state = new ConfState();
                state.Voters.AddRange(voters);
                state.Learners.AddRange(learners);
                applicationSnapshot =
                    ProtocolDefaults.EnsureSnapshot(
                        new Snapshot
                        {
                            Data =
                                options.SnapshotData,
                            Metadata =
                                new SnapshotMetadata
                                {
                                    Index =
                                        options.SnapshotIndex,
                                    Term = 1,
                                    ConfState = state,
                                },
                        });
                storage.ApplySnapshot(
                    applicationSnapshot);
            }

            _nodes.Add(CreateNode(id, storage, options, applicationSnapshot));
        }
    }

    internal void CrashNode(int nodeIndex)
    {
        InteractionNode node = GetNode(nodeIndex);
        ulong id = node.Config.Id;
        if (!_crashed.Add(id))
        {
            throw new InvalidOperationException($"Node {id} is already crashed.");
        }

        node.AppendWork.Clear();
        node.ApplyWork.Clear();
        _deliveries.RemoveAll(delivery => delivery.From == id || delivery.To == id);
    }

    internal void RestartNode(int nodeIndex)
    {
        InteractionNode node = GetNode(nodeIndex);
        ulong id = node.Config.Id;
        if (!_crashed.Contains(id))
        {
            throw new InvalidOperationException($"Node {id} has not crashed.");
        }

        // Keep only what the mock storage and application published. A new
        // instance has no in-flight storage work, responses, or unstable log.
        Snapshot application = node.GetApplicationSnapshot();
        RaftHardState? persisted = node.Storage.GetInitialState().HardState;
        ulong commit = persisted?.Commit ?? node.Storage.GetFirstIndex() - 1;
        if (commit < application.Metadata.Index)
        {
            // Independent apply can publish a stable, committed entry
            // before a commit-only append request runs. Repair only commit,
            // and only from the retained physical application boundary.
            if (persisted is not { } state
                || state.Term < application.Metadata.Term
                || application.Metadata.Index > node.Storage.GetLastIndex()
                || node.Storage.GetTerm(application.Metadata.Index) != application.Metadata.Term)
            {
                throw new InvalidOperationException("The recovered application is not covered by durable Raft state.");
            }

            node.Storage.SetHardState(state with { Commit = application.Metadata.Index });
        }

        _nodes[nodeIndex] = CreateNode(id, node.Storage, node.Options, application);
        _deliveries.RemoveAll(delivery => delivery.From == id || delivery.To == id);
        _crashed.Remove(id);
    }

    private InteractionNode CreateNode(
        ulong id,
        MemoryStorage storage,
        InteractionNodeOptions options,
        Snapshot applicationSnapshot)
    {
        var config = new RaftConfig
        {
            Id = id,
            ElectionTick = options.ElectionTick,
            HeartbeatTick = options.HeartbeatTick,
            Storage = storage,
            Applied = applicationSnapshot.Metadata.Index,
            AsyncStorageWrites = options.AsyncStorageWrites,
            MaxSizePerMessage = ulong.MaxValue,
            MaxCommittedSizePerReady = options.MaxCommittedSizePerReady,
            MaxUncommittedEntriesSize = ulong.MaxValue,
            MaxInflightMessages = options.MaxInflightMessages,
            CheckQuorum = options.CheckQuorum,
            PreVote = options.PreVote,
            ReadOnlyOption = options.ReadOnlyOption,
            Logger = _output,
            TraceSink = TraceSink,
            DisableConfChangeValidation = options.DisableConfChangeValidation,
            StepDownOnRemoval = options.StepDownOnRemoval,
        };
        var rawNode = new DotnetRaft.RawNode(config, _ => 0);
        return new InteractionNode(rawNode, storage, config, options, applicationSnapshot);
    }

    internal void Campaign(int nodeIndex)
    {
        GetNode(nodeIndex).RawNode.Campaign();
    }

    internal void TickElection(int nodeIndex)
    {
        InteractionNode node = GetNode(nodeIndex);
        Tick(node, node.Config.ElectionTick);
    }

    internal void TickHeartbeat(int nodeIndex)
    {
        InteractionNode node = GetNode(nodeIndex);
        Tick(node, node.Config.HeartbeatTick);
    }

    internal void Propose(
        int nodeIndex,
        ReadOnlySpan<byte> data)
    {
        GetNode(nodeIndex).RawNode.Propose(data);
    }

    internal void ProposeConfiguration(
        int nodeIndex,
        ProtocolConfChange change)
    {
        ArgumentNullException.ThrowIfNull(change);
        GetNode(nodeIndex)
            .RawNode
            .ProposeConfChange(change);
    }

    internal void ProposeConfiguration(
        int nodeIndex,
        ConfChangeV2 change)
    {
        ArgumentNullException.ThrowIfNull(change);
        GetNode(nodeIndex)
            .RawNode
            .ProposeConfChange(change);
    }

    internal void ProcessReady(int nodeIndex)
    {
        InteractionNode node = GetNode(nodeIndex);
        Ready ready = node.RawNode.Ready();
        _output.Write(
            ProtobufFixtureEncoding.DescribeReady(ready));
        foreach (ReadState readState in ready.ReadStates)
        {
            ReadStateDelivered?.Invoke(node.Config.Id, readState);
        }

        if (!node.Config.AsyncStorageWrites)
        {
            ProcessSynchronousReady(node, ready);
            return;
        }

        EnqueueMessages(ready.Messages);
        if (ready.AppendWork is { } append)
        {
            node.AppendWork.Enqueue(append);
        }

        if (ready.ApplyWork is { } apply)
        {
            node.ApplyWork.Enqueue(apply);
        }
    }

    private void ProcessSynchronousReady(InteractionNode node, Ready ready)
    {
        if (ready.Snapshot is { } snapshot)
        {
            node.Storage.ApplySnapshot(snapshot);
        }
        node.Storage.Append(ready.Entries);
        if (ready.HardState is { } hardState)
        {
            node.Storage.SetHardState(hardState);
        }

        if (ready.Snapshot is { } applicationSnapshot)
        {
            node.SetApplicationSnapshot(applicationSnapshot);
        }

        foreach (Entry entry in ready.CommittedEntries)
        {
            ApplyEntry(node, entry);
        }

        EnqueueMessages(ready.Messages);
        node.RawNode.Advance(ready);
    }

    internal void ProcessAppendThread(int nodeIndex)
    {
        InteractionNode node = GetNode(nodeIndex);
        if (!node.AppendWork.TryDequeue(out StorageAppendWork? request))
        {
            _output.Write(
                "no append work to perform");
            return;
        }

        DescribeWorkerRequest(request.DescribeWork());
        PersistAppend(node.Storage, request);

        if (request.Snapshot is { } snapshot)
        {
            node.SetApplicationSnapshot(snapshot);
        }

        _output.WriteLine("Responses:");
        foreach (Message response in request.MessagesAfterPersist)
        {
            _output.WriteLine(ProtobufFixtureEncoding.DescribeMessage(response));
            EnqueueMessage(response);
        }

        if (request.Completion is { } completion)
        {
            EnqueueCompletion(node.Config.Id, completion);
        }
    }

    internal void ProcessApplyThread(int nodeIndex)
    {
        InteractionNode node = GetNode(nodeIndex);
        if (!node.ApplyWork.TryDequeue(out StorageApplyWork? request))
        {
            _output.Write(
                "no apply work to perform");
            return;
        }

        DescribeWorkerRequest(request.DescribeWork());
        foreach (Entry entry in request.Entries)
        {
            ApplyEntry(node, entry);
        }

        _output.WriteLine("Responses:");
        EnqueueCompletion(node.Config.Id, request.Completion);
    }

    private void DescribeWorkerRequest(Message description)
    {
        description.Responses.Clear();
        _output.WriteLine("Processing:");
        _output.WriteLine(ProtobufFixtureEncoding.DescribeMessage(description));
    }

    private void EnqueueMessage(Message message) => _deliveries.Add(new InteractionDelivery(message));

    private void EnqueueMessages(IReadOnlyList<Message> messages)
    {
        foreach (Message message in messages)
        {
            EnqueueMessage(message);
        }
    }

    private void EnqueueCompletion(ulong nodeId, StorageCompletion completion)
    {
        var delivery = new InteractionDelivery(nodeId, completion);
        _output.WriteLine(ProtobufFixtureEncoding.DescribeMessage(delivery.Frame));
        _deliveries.Add(delivery);
    }

    internal int DeliverMessages(
        MessageType? type,
        params InteractionRecipient[] recipients)
    {
        return DeliverMessagesCore(
            type,
            recipients,
            beforeHandle: null);
    }

    internal void DeliverNetworkMessage(int index, bool drop = false, bool duplicate = false)
    {
        InteractionDelivery delivery = _deliveries[index];
        if (delivery.IsLocal)
        {
            throw new ArgumentException("Only network messages can be reordered, dropped, or duplicated.", nameof(index));
        }

        drop |= !CanDeliver(delivery);
        if (!drop && duplicate)
        {
            EnqueueMessage(delivery.Frame);
        }

        FinishDelivery(index, drop);
    }

    internal void Stabilize(params int[] nodeIndexes)
    {
        int[] selected = nodeIndexes.Length == 0
            ? Enumerable.Range(0, _nodes.Count).ToArray()
            : [.. nodeIndexes];
        ValidateNodeIndexes(selected);

        var handled = 0;
        void ConsumeWork()
        {
            if (handled >= _stabilizationLimit)
            {
                ThrowStabilizationLimit(
                    selected);
            }

            handled++;
        }

        while (true)
        {
            var didWork = false;
            foreach (int nodeIndex in selected)
            {
                InteractionNode node =
                    GetNode(nodeIndex);
                if (_crashed.Contains(node.Config.Id) || !node.RawNode.HasReady())
                {
                    continue;
                }

                ConsumeWork();
                _output.WriteLine(
                    $"> {node.Config.Id} handling Ready");
                _output.WithIndent(
                    () => ProcessReady(nodeIndex));
                didWork = true;
            }

            foreach (int nodeIndex in selected)
            {
                InteractionNode node =
                    GetNode(nodeIndex);
                if (!_deliveries.Any(
                        delivery =>
                            delivery.To
                            == node.Config.Id))
                {
                    continue;
                }

                _output.WriteLine(
                    $"> {node.Config.Id} receiving messages");
                _output.WithIndent(() =>
                    DeliverMessagesCore(
                        type: null,
                        [new InteractionRecipient(
                            node.Config.Id)],
                        ConsumeWork));
                didWork = true;
            }

            foreach (int nodeIndex in selected)
            {
                InteractionNode node =
                    GetNode(nodeIndex);
                if (_crashed.Contains(node.Config.Id) || node.AppendWorkCount == 0)
                {
                    continue;
                }

                _output.WriteLine(
                    $"> {node.Config.Id} processing append thread");
                while (node.AppendWorkCount > 0)
                {
                    ConsumeWork();
                    _output.WithIndent(
                        () => ProcessAppendThread(
                            nodeIndex));
                }

                didWork = true;
            }

            foreach (int nodeIndex in selected)
            {
                InteractionNode node =
                    GetNode(nodeIndex);
                if (_crashed.Contains(node.Config.Id) || node.ApplyWorkCount == 0)
                {
                    continue;
                }

                _output.WriteLine(
                    $"> {node.Config.Id} processing apply thread");
                while (node.ApplyWorkCount > 0)
                {
                    ConsumeWork();
                    _output.WithIndent(
                        () => ProcessApplyThread(
                            nodeIndex));
                }

                didWork = true;
            }

            if (!didWork)
            {
                return;
            }
        }
    }

    internal void Compact(
        int nodeIndex,
        ulong compactThrough)
    {
        InteractionNode node = GetNode(nodeIndex);
        ulong appliedIndex =
            node.GetApplicationSnapshot()
                .Metadata.Index;
        if (compactThrough > appliedIndex)
        {
            throw new InvalidOperationException(
                $"Compaction index {compactThrough} exceeds physical application snapshot {appliedIndex}.");
        }

        node.Storage.Compact(compactThrough);
        RaftLog(nodeIndex);
    }

    internal void TransferLeadership(
        ulong from,
        ulong to)
    {
        InteractionNode source =
            GetNodeById(from);
        _ = GetNodeById(to);
        source.RawNode.TransferLeader(to);
    }

    internal void ForgetLeader(int nodeIndex)
    {
        GetNode(nodeIndex).RawNode.ForgetLeader();
    }

    internal void ReportUnreachable(
        int nodeIndex,
        ulong peerId)
    {
        GetNode(nodeIndex)
            .RawNode
            .ReportUnreachable(peerId);
    }

    internal void SendSnapshot(
        int fromIndex,
        int toIndex)
    {
        InteractionNode from = GetNode(fromIndex);
        InteractionNode to = GetNode(toIndex);
        var message = new Message
        {
            From = from.Config.Id,
            To = to.Config.Id,
            Term =
                from.RawNode.GetBasicStatus().Term,
            Type = MessageType.MsgSnap,
            Snapshot =
                from.GetApplicationSnapshot(),
        };
        EnqueueMessage(message);
        _output.Write(
            ProtobufFixtureEncoding.DescribeMessage(message));
    }

    internal void SetRandomizedElectionTimeout(
        int nodeIndex,
        int timeout)
    {
        GetNode(nodeIndex)
            .RawNode
            .SetRandomizedElectionTimeoutForTesting(
                timeout);
    }

    internal void QueueMessageForTesting(
        Message message)
    {
        ArgumentNullException.ThrowIfNull(message);
        EnqueueMessage(message);
    }

    internal void RaftLog(int nodeIndex)
    {
        InteractionNode node = GetNode(nodeIndex);
        ulong first = node.Storage.GetFirstIndex();
        ulong last = node.Storage.GetLastIndex();
        if (last < first)
        {
            _output.Write(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"log is empty: first index={first}, last index={last}"));
            return;
        }

        IReadOnlyList<Entry> entries =
            node.Storage.GetEntries(
                first,
                last + 1,
                ulong.MaxValue);
        _output.Write(
            ProtobufFixtureEncoding.DescribeEntries(
                entries));
    }

    internal void Status(int nodeIndex)
    {
        Status status =
            GetNode(nodeIndex).RawNode.GetStatus();
        foreach ((ulong id, ProgressStatus progress) in
                 status.Progress)
        {
            var builder = new StringBuilder();
            builder.Append(
                id.ToString(
                    CultureInfo.InvariantCulture));
            builder.Append(": ");
            builder.Append(progress.State);
            builder.Append(" match=");
            builder.Append(
                progress.Match.ToString(
                    CultureInfo.InvariantCulture));
            builder.Append(" next=");
            builder.Append(
                progress.Next.ToString(
                    CultureInfo.InvariantCulture));
            if (progress.IsLearner)
            {
                builder.Append(" learner");
            }

            if (progress.IsPaused)
            {
                builder.Append(" paused");
            }

            if (progress.PendingSnapshot > 0)
            {
                builder.Append(" pendingSnap=");
                builder.Append(
                    progress.PendingSnapshot.ToString(
                        CultureInfo.InvariantCulture));
            }

            if (!progress.RecentActive)
            {
                builder.Append(" inactive");
            }

            if (progress.InflightCount > 0)
            {
                builder.Append(" inflight=");
                builder.Append(
                    progress.InflightCount.ToString(
                        CultureInfo.InvariantCulture));
                bool full =
                    progress.InflightCount
                        == progress.InflightCapacity
                    || (progress.MaxInflightBytes
                            != 0
                        && progress.InflightBytes
                            >= progress.MaxInflightBytes);
                if (full)
                {
                    builder.Append("[full]");
                }
            }

            _output.WriteLine(builder.ToString());
        }
    }

    internal void RaftState()
    {
        foreach (InteractionNode node in _nodes)
        {
            Status status = node.RawNode.GetStatus();
            bool voter =
                status.Configuration.Voters.Contains(
                    status.Basic.Id)
                || status.Configuration.VotersOutgoing
                    .Contains(status.Basic.Id);
            _output.WriteLine(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"{status.Basic.Id}: {status.Basic.Role} {(voter ? "(Voter)" : "(Non-Voter)")} Term:{status.Basic.Term} Lead:{status.Basic.LeaderId}"));
        }
    }

    private void Execute(
        InteractionDirective directive,
        string input)
    {
        ArgumentNullException.ThrowIfNull(input);
        switch (directive.Command)
        {
            case "add-nodes":
                HandleAddNodes(directive);
                return;
            case "campaign":
                Campaign(
                    OneNodeIndex(directive));
                return;
            case "compact":
                HandleCompact(directive);
                return;
            case "deliver-msgs":
                HandleDeliverMessages(directive);
                return;
            case "process-ready":
                HandleProcessReady(directive);
                return;
            case "process-append-thread":
                HandleProcessAppendThread(directive);
                return;
            case "process-apply-thread":
                HandleProcessApplyThread(directive);
                return;
            case "log-level":
                HandleLogLevel(directive);
                return;
            case "raft-log":
                RaftLog(
                    OneNodeIndex(directive));
                return;
            case "raft-state":
                RequireNoArguments(directive);
                RaftState();
                return;
            case "set-randomized-election-timeout":
                HandleSetRandomizedElectionTimeout(
                    directive);
                return;
            case "stabilize":
                HandleStabilize(directive);
                return;
            case "status":
                Status(
                    OneNodeIndex(directive));
                return;
            case "tick-election":
                TickElection(
                    OneNodeIndex(directive));
                return;
            case "tick-heartbeat":
                TickHeartbeat(
                    OneNodeIndex(directive));
                return;
            case "transfer-leadership":
                HandleTransferLeadership(directive);
                return;
            case "forget-leader":
                ForgetLeader(
                    OneNodeIndex(directive));
                return;
            case "send-snapshot":
                HandleSendSnapshot(directive);
                return;
            case "propose":
                HandlePropose(directive);
                return;
            case "propose-conf-change":
                HandleProposeConfiguration(
                    directive,
                    input);
                return;
            case "report-unreachable":
                HandleReportUnreachable(directive);
                return;
            default:
                throw new InvalidOperationException(
                    $"Unknown interaction command {directive.Command}.");
        }
    }

    private void HandleAddNodes(
        InteractionDirective directive)
    {
        string[] bare = GetBare(directive);
        if (bare.Length != 1)
        {
            throw new ArgumentException(
                "add-nodes requires exactly one count.");
        }

        int count = InteractionParsing.ParseInt(
            bare[0],
            "count");
        ulong[] voters = [];
        ulong[] learners = [];
        ulong snapshotIndex = 0;
        RaftBytes snapshotData = RaftBytes.Empty;
        var inflight = int.MaxValue;
        ulong maxCommitted = 0;
        var preVote = false;
        var checkQuorum = false;
        var disableValidation = false;
        var stepDownOnRemoval = false;
        var asyncStorageWrites = false;
        var readOnly = ReadOnlyOption.Safe;

        foreach (InteractionArgument option in
                 GetOptions(directive))
        {
            switch (option.Key)
            {
                case "voters":
                    voters = ParseUInt64Values(option);
                    break;
                case "learners":
                    learners = ParseUInt64Values(option);
                    break;
                case "inflight":
                    inflight = InteractionParsing.ParseInt(
                        OneValue(option),
                        option.Key);
                    break;
                case "index":
                    snapshotIndex =
                        InteractionParsing.ParseUInt64(
                            OneValue(option),
                            option.Key);
                    break;
                case "content":
                    snapshotData =
                        RaftBytes.CopyFromUtf8(
                            OneValue(option));
                    break;
                case "prevote":
                    preVote =
                        InteractionParsing.ParseBoolean(
                            OneValue(option),
                            option.Key);
                    break;
                case "checkquorum":
                    checkQuorum =
                        InteractionParsing.ParseBoolean(
                            OneValue(option),
                            option.Key);
                    break;
                case "max-committed-size-per-ready":
                    maxCommitted =
                        InteractionParsing.ParseUInt64(
                            OneValue(option),
                            option.Key);
                    break;
                case "disable-conf-change-validation":
                    disableValidation =
                        InteractionParsing.ParseBoolean(
                            OneValue(option),
                            option.Key);
                    break;
                case "read-only":
                    readOnly = OneValue(option) switch
                    {
                        "safe" => ReadOnlyOption.Safe,
                        "lease-based" =>
                            ReadOnlyOption.LeaseBased,
                        string value =>
                            throw new FormatException(
                                $"Unknown read-only option {value}."),
                    };
                    break;
                case "step-down-on-removal":
                    stepDownOnRemoval =
                        InteractionParsing.ParseBoolean(
                            OneValue(option),
                            option.Key);
                    break;
                case "async-storage-writes":
                    asyncStorageWrites =
                        InteractionParsing.ParseBoolean(
                            OneValue(option),
                            option.Key);
                    break;
                default:
                    throw new ArgumentException(
                        $"Unknown add-nodes option {option.Key}.");
            }
        }

        AddNodes(
            count,
            new InteractionNodeOptions
            {
                Voters = voters,
                Learners = learners,
                SnapshotIndex = snapshotIndex,
                SnapshotData = snapshotData,
                MaxInflightMessages = inflight,
                MaxCommittedSizePerReady =
                    maxCommitted,
                PreVote = preVote,
                CheckQuorum = checkQuorum,
                DisableConfChangeValidation =
                    disableValidation,
                StepDownOnRemoval =
                    stepDownOnRemoval,
                ReadOnlyOption = readOnly,
                AsyncStorageWrites =
                    asyncStorageWrites,
            });
    }

    private void HandleCompact(
        InteractionDirective directive)
    {
        string[] bare = GetBare(directive);
        if (bare.Length != 2)
        {
            throw new ArgumentException(
                "compact requires a node and index.");
        }

        if (GetOptions(directive).Length > 0)
        {
            throw new ArgumentException(
                "compact does not accept keyed options.");
        }

        Compact(
            ParseNodeIndex(bare[0]),
            InteractionParsing.ParseUInt64(
                bare[1],
                "compact index"));
    }

    private void HandleDeliverMessages(
        InteractionDirective directive)
    {
        var recipients =
            new List<InteractionRecipient>();
        MessageType? type = null;
        foreach (InteractionArgument argument in
                 directive.Arguments)
        {
            if (argument.Values.Count == 0)
            {
                recipients.Add(
                    new InteractionRecipient(
                        ParseNodeId(argument.Key)));
                continue;
            }

            switch (argument.Key)
            {
                case "drop":
                    recipients.AddRange(
                        argument.Values.Select(value =>
                            new InteractionRecipient(
                                ParseNodeId(value),
                                Drop: true)));
                    break;
                case "type":
                    string symbol = OneValue(argument);
                    if (!Enum.TryParse(
                            symbol,
                            ignoreCase: false,
                            out MessageType parsed)
                        || !Enum.IsDefined(parsed)
                        || !string.Equals(
                            parsed.ToString(),
                            symbol,
                            StringComparison.Ordinal))
                    {
                        throw new FormatException(
                            $"Unknown MessageType {symbol}.");
                    }

                    type = parsed;
                    break;
                default:
                    throw new ArgumentException(
                        $"Unknown deliver-msgs option {argument.Key}.");
            }
        }

        if (recipients.Count == 0)
        {
            throw new ArgumentException(
                "deliver-msgs requires at least one recipient.");
        }

        if (DeliverMessages(
                type,
                [.. recipients]) == 0)
        {
            _output.WriteLine("no messages");
        }
    }

    private void HandleProcessReady(
        InteractionDirective directive)
    {
        string[] bare = GetBare(directive);
        if (bare.Length == 0
            || GetOptions(directive).Length > 0)
        {
            throw new ArgumentException(
                "process-ready requires one or more nodes.");
        }

        foreach (string value in bare)
        {
            int index = ParseNodeIndex(value);
            if (bare.Length > 1)
            {
                _output.WriteLine(
                    $"> {index + 1} handling Ready");
                _output.WithIndent(
                    () => ProcessReady(index));
            }
            else
            {
                ProcessReady(index);
            }
        }
    }

    private void HandleProcessAppendThread(
        InteractionDirective directive)
    {
        HandleWorkerCommand(
            directive,
            "processing append thread",
            ProcessAppendThread);
    }

    private void HandleProcessApplyThread(
        InteractionDirective directive)
    {
        HandleWorkerCommand(
            directive,
            "processing apply thread",
            ProcessApplyThread);
    }

    private void HandleWorkerCommand(
        InteractionDirective directive,
        string description,
        Action<int> process)
    {
        string[] bare = GetBare(directive);
        if (bare.Length == 0
            || GetOptions(directive).Length > 0)
        {
            throw new ArgumentException(
                $"{directive.Command} requires one or more nodes.");
        }

        foreach (string value in bare)
        {
            int index = ParseNodeIndex(value);
            if (bare.Length > 1)
            {
                _output.WriteLine(
                    $"> {index + 1} {description}");
                _output.WithIndent(
                    () => process(index));
            }
            else
            {
                process(index);
            }
        }
    }

    private void HandleLogLevel(
        InteractionDirective directive)
    {
        string[] bare = GetBare(directive);
        if (bare.Length != 1
            || GetOptions(directive).Length > 0)
        {
            throw new ArgumentException(
                "log-level requires exactly one level.");
        }

        _output.Level = ParseLogLevel(bare[0]);
    }

    private void HandleSetRandomizedElectionTimeout(
        InteractionDirective directive)
    {
        string[] bare = GetBare(directive);
        InteractionArgument[] options =
            GetOptions(directive);
        if (bare.Length != 1
            || options.Length != 1
            || options[0].Key != "timeout")
        {
            throw new ArgumentException(
                "set-randomized-election-timeout requires one node and timeout=<value>.");
        }

        int timeout = InteractionParsing.ParseInt(
            OneValue(options[0]),
            "timeout");
        SetRandomizedElectionTimeout(
            ParseNodeIndex(bare[0]),
            timeout);
    }

    private void HandleStabilize(
        InteractionDirective directive)
    {
        int[] indexes =
        [
            .. GetBare(directive)
                .Select(ParseNodeIndex),
        ];
        InteractionLogLevel previous =
            _output.Level;
        try
        {
            foreach (InteractionArgument option in
                     GetOptions(directive))
            {
                if (option.Key != "log-level")
                {
                    throw new ArgumentException(
                        $"Unknown stabilize option {option.Key}.");
                }

                _output.Level =
                    ParseLogLevel(
                        OneValue(option));
            }

            Stabilize(indexes);
        }
        finally
        {
            _output.Level = previous;
        }
    }

    private void HandleTransferLeadership(
        InteractionDirective directive)
    {
        RequireNoBareArguments(directive);
        ulong? from = null;
        ulong? to = null;
        foreach (InteractionArgument option in
                 GetOptions(directive))
        {
            switch (option.Key)
            {
                case "from":
                    from = ParseNodeId(
                        OneValue(option));
                    break;
                case "to":
                    to = ParseNodeId(
                        OneValue(option));
                    break;
                default:
                    throw new ArgumentException(
                        $"Unknown transfer-leadership option {option.Key}.");
            }
        }

        if (!from.HasValue || !to.HasValue)
        {
            throw new ArgumentException(
                "transfer-leadership requires from and to.");
        }

        TransferLeadership(
            from.Value,
            to.Value);
    }

    private void HandleSendSnapshot(
        InteractionDirective directive)
    {
        string[] bare = GetBare(directive);
        if (bare.Length != 2
            || GetOptions(directive).Length > 0)
        {
            throw new ArgumentException(
                "send-snapshot requires source and destination nodes.");
        }

        SendSnapshot(
            ParseNodeIndex(bare[0]),
            ParseNodeIndex(bare[1]));
    }

    private void HandlePropose(
        InteractionDirective directive)
    {
        string[] bare = GetBare(directive);
        if (bare.Length != 2
            || GetOptions(directive).Length > 0)
        {
            throw new ArgumentException(
                "propose requires exactly one payload token.");
        }

        Propose(
            ParseNodeIndex(bare[0]),
            Encoding.UTF8.GetBytes(bare[1]));
    }

    private void HandleProposeConfiguration(
        InteractionDirective directive,
        string input)
    {
        string[] bare = GetBare(directive);
        if (bare.Length != 1)
        {
            throw new ArgumentException(
                "propose-conf-change requires exactly one node.");
        }

        var v1 = false;
        var transition =
            ConfChangeTransition.Auto;
        foreach (InteractionArgument option in
                 GetOptions(directive))
        {
            switch (option.Key)
            {
                case "v1":
                    v1 =
                        InteractionParsing.ParseBoolean(
                            OneValue(option),
                            option.Key);
                    break;
                case "transition":
                    transition = OneValue(option) switch
                    {
                        "auto" =>
                            ConfChangeTransition.Auto,
                        "implicit" =>
                            ConfChangeTransition.JointImplicit,
                        "explicit" =>
                            ConfChangeTransition.JointExplicit,
                        string value =>
                            throw new FormatException(
                                $"Unknown transition {value}."),
                    };
                    break;
                default:
                    throw new ArgumentException(
                        $"Unknown propose-conf-change option {option.Key}.");
            }
        }

        ConfChangeSingle[] changes =
            ParseChanges(input);
        int nodeIndex = ParseNodeIndex(bare[0]);
        if (v1)
        {
            if (changes.Length != 1
                || transition
                    != ConfChangeTransition.Auto)
            {
                throw new ArgumentException(
                    "V1 configuration changes require exactly one operation and transition=auto.");
            }

            ProposeConfiguration(
                nodeIndex,
                new ProtocolConfChange
                {
                    Type = changes[0].Type,
                    NodeId = changes[0].NodeId,
                });
            return;
        }

        var change = new ConfChangeV2
        {
            Transition = transition,
        };
        change.Changes.AddRange(changes);
        ProposeConfiguration(nodeIndex, change);
    }

    private void HandleReportUnreachable(
        InteractionDirective directive)
    {
        string[] bare = GetBare(directive);
        if (bare.Length != 2
            || GetOptions(directive).Length > 0)
        {
            throw new ArgumentException(
                "report-unreachable requires two nodes.");
        }

        int source = ParseNodeIndex(bare[0]);
        InteractionNode peer =
            GetNode(ParseNodeIndex(bare[1]));
        ReportUnreachable(
            source,
            peer.Config.Id);
    }

    private int DeliverMessagesCore(
        MessageType? type,
        IReadOnlyList<InteractionRecipient> recipients,
        Action? beforeHandle)
    {
        ArgumentNullException.ThrowIfNull(recipients);
        ValidateRecipients(recipients);
        foreach (InteractionRecipient recipient in
                 recipients)
        {
            if (!recipient.Drop)
            {
                _ = GetNodeById(recipient.Id);
            }
        }

        var handled = 0;
        foreach (InteractionRecipient recipient in
                 recipients)
        {
            var index = 0;
            while (index < _deliveries.Count)
            {
                InteractionDelivery delivery = _deliveries[index];
                bool matches =
                    delivery.To == recipient.Id
                    && (!type.HasValue
                        || delivery.Type == type.Value)
                    && !(recipient.Drop && delivery.IsLocal);
                if (!matches)
                {
                    index++;
                    continue;
                }

                beforeHandle?.Invoke();
                FinishDelivery(index, recipient.Drop || !CanDeliver(delivery));
                handled++;
            }
        }

        return handled;
    }

    private bool CanDeliver(InteractionDelivery delivery)
    {
        return !_crashed.Contains(delivery.From)
            && !_crashed.Contains(delivery.To)
            && (delivery.IsLocal || NetworkLinkIsOpen?.Invoke(delivery.Frame) != false);
    }

    private void FinishDelivery(int index, bool drop)
    {
        InteractionDelivery delivery = _deliveries[index];
        Message message = delivery.Frame;
        if (drop)
        {
            _output.Write("dropped: ");
        }

        _output.WriteLine(ProtobufFixtureEncoding.DescribeMessage(message));
        if (!drop)
        {
            InteractionNode destination = GetNodeById(delivery.To);
            if (delivery.Completion is { } completion)
            {
                destination.RawNode.CompleteStorageWork(completion);
            }
            else if (IsSafelyRejectedUnknownResponse(destination, message))
            {
                _output.WriteLine($"Response sender {message.From} is not a known peer.");
            }
            else
            {
                destination.RawNode.Step(message);
            }
        }

        _deliveries.RemoveAt(index);
    }

    private static bool IsSafelyRejectedUnknownResponse(
        InteractionNode destination,
        Message message)
    {
        if (destination.RawNode.IsFaultedForTesting
            || RaftMessageTargets.IsLocal(message.From)
            || MessageClassifier.IsLocal(message.Type)
            || !MessageClassifier.IsResponse(message.Type))
        {
            return false;
        }

        var known = false;
        destination.RawNode.VisitProgress(
            (id, _) => known |= id == message.From);
        return !known;
    }

    private void ApplyEntry(
        InteractionNode node,
        Entry entry)
    {
        EntryDelivered?.Invoke(node.Config.Id, entry.Clone());
        Snapshot previous =
            node.GetApplicationSnapshot();
        if (entry.Index
            <= previous.Metadata.Index)
        {
            return;
        }

        ConfState configuration;
        RaftBytes update;
        switch (entry.Type)
        {
            case EntryType.EntryConfChange:
                ProtocolConfChange v1 =
                    ProtobufFixtureEncoding.DecodeLegacy(
                        entry.Data);
                configuration =
                    node.RawNode.ApplyConfChange(v1);
                update = v1.Context;
                break;
            case EntryType.EntryConfChangeV2:
                ConfChangeV2 v2 =
                    ProtobufFixtureEncoding.DecodeV2(
                        entry.Data);
                configuration =
                    node.RawNode.ApplyConfChange(v2);
                update = v2.Context;
                break;
            default:
                configuration =
                    previous.Metadata.ConfState.Clone();
                update = entry.Data;
                break;
        }

        byte[] data =
        [
            .. previous.Data.Span,
            .. update.Span,
        ];
        Snapshot snapshot =
            node.Storage.CreateSnapshot(
                entry.Index,
                configuration,
                RaftBytes.CopyFrom(data));
        node.SetApplicationSnapshot(snapshot);
        EntryApplied?.Invoke(node.Config.Id, entry.Clone());
    }

    private static void Tick(
        InteractionNode node,
        int count)
    {
        for (var tick = 0; tick < count; tick++)
        {
            node.RawNode.Tick();
        }
    }

    private InteractionNode GetNode(int index)
    {
        if (index < 0 || index >= _nodes.Count)
        {
            throw new ArgumentOutOfRangeException(
                nameof(index),
                index,
                "Node index is not instantiated.");
        }

        return _nodes[index];
    }

    private InteractionNode GetNodeById(ulong id)
    {
        if (id == 0
            || id > (ulong)_nodes.Count)
        {
            throw new InvalidOperationException(
                $"Node {id} is not instantiated.");
        }

        return _nodes[checked((int)id - 1)];
    }

    private void ValidateNodeIndexes(
        IEnumerable<int> indexes)
    {
        var seen = new HashSet<int>();
        foreach (int index in indexes)
        {
            _ = GetNode(index);
            if (!seen.Add(index))
            {
                throw new ArgumentException(
                    $"Node {index + 1} is selected more than once.");
            }
        }
    }

    private static void ValidateRecipients(
        IReadOnlyList<InteractionRecipient> recipients)
    {
        if (recipients.Count == 0)
        {
            throw new ArgumentException(
                "At least one recipient is required.",
                nameof(recipients));
        }

        var modes = new Dictionary<ulong, bool>();
        foreach (InteractionRecipient recipient in
                 recipients)
        {
            if (modes.TryGetValue(
                    recipient.Id,
                    out bool drop)
                && drop != recipient.Drop)
            {
                throw new ArgumentException(
                    $"Recipient {recipient.Id} cannot be both delivered and dropped.",
                    nameof(recipients));
            }

            modes[recipient.Id] = recipient.Drop;
        }
    }

    private void ThrowStabilizationLimit(
        IReadOnlyList<int> selected)
    {
        ulong[] ids =
        [
            .. selected.Select(
                index => _nodes[index].Config.Id),
        ];
        ulong[] ready =
        [
            .. selected
                .Where(index =>
                    _nodes[index].RawNode.HasReady())
                .Select(index =>
                    _nodes[index].Config.Id),
        ];
        var idSet = ids.ToHashSet();
        int messages = _deliveries.Count(delivery => idSet.Contains(delivery.To));
        throw new InvalidOperationException(
            $"Stabilization exceeded limit {_stabilizationLimit}; selected IDs: {string.Join(',', ids)}; ready IDs: {string.Join(',', ready)}; queued selected messages: {messages}.");
    }

    private static int OneNodeIndex(
        InteractionDirective directive)
    {
        string[] bare = GetBare(directive);
        if (bare.Length != 1
            || GetOptions(directive).Length > 0)
        {
            throw new ArgumentException(
                $"{directive.Command} requires exactly one node.");
        }

        return ParseNodeIndex(bare[0]);
    }

    private static int ParseNodeIndex(string value)
    {
        ulong id = ParseNodeId(value);
        if (id > int.MaxValue)
        {
            throw new FormatException(
                $"Node value {value} exceeds Int32.");
        }

        return (int)id - 1;
    }

    private static ulong ParseNodeId(string value)
    {
        ulong id = InteractionParsing.ParseUInt64(
            value,
            "node");
        if (id == 0)
        {
            throw new FormatException(
                "Node IDs are one-based.");
        }

        return id;
    }

    private static string[] GetBare(
        InteractionDirective directive)
    {
        return
        [
            .. directive.Arguments
                .Where(argument =>
                    argument.Values.Count == 0)
                .Select(argument => argument.Key),
        ];
    }

    private static InteractionArgument[] GetOptions(
        InteractionDirective directive)
    {
        return
        [
            .. directive.Arguments
                .Where(argument =>
                    argument.Values.Count > 0),
        ];
    }

    private static void RequireNoArguments(
        InteractionDirective directive)
    {
        if (directive.Arguments.Count != 0)
        {
            throw new ArgumentException(
                $"{directive.Command} accepts no arguments.");
        }
    }

    private static void RequireNoBareArguments(
        InteractionDirective directive)
    {
        if (GetBare(directive).Length != 0)
        {
            throw new ArgumentException(
                $"{directive.Command} accepts only keyed arguments.");
        }
    }

    private static string OneValue(
        InteractionArgument option)
    {
        if (option.Values.Count != 1)
        {
            throw new ArgumentException(
                $"{option.Key} requires exactly one value.");
        }

        return option.Values[0];
    }

    private static ulong[] ParseUInt64Values(
        InteractionArgument option)
    {
        return
        [
            .. option.Values.Select(value =>
                InteractionParsing.ParseUInt64(
                    value,
                    option.Key)),
        ];
    }

    private static InteractionLogLevel ParseLogLevel(
        string value)
    {
        return value.ToUpperInvariant() switch
        {
            "DEBUG" => InteractionLogLevel.Debug,
            "INFO" => InteractionLogLevel.Information,
            "WARN" => InteractionLogLevel.Warning,
            "ERROR" => InteractionLogLevel.Error,
            "NONE" => InteractionLogLevel.None,
            _ => throw new FormatException(
                $"Unknown log level {value}."),
        };
    }

    private static ConfChangeSingle[] ParseChanges(
        string input)
    {
        if (string.IsNullOrWhiteSpace(input))
        {
            return [];
        }

        return input.Split(
                (char[]?)null,
                StringSplitOptions.RemoveEmptyEntries)
            .Select(ParseChange)
            .ToArray();
    }

    private static ConfChangeSingle ParseChange(
        string token)
    {
        if (token.Length < 2)
        {
            throw new FormatException(
                $"Invalid configuration token {token}.");
        }

        ConfChangeType type = token[0] switch
        {
            'v' => ConfChangeType.ConfChangeAddNode,
            'l' =>
                ConfChangeType.ConfChangeAddLearnerNode,
            'r' =>
                ConfChangeType.ConfChangeRemoveNode,
            'u' =>
                ConfChangeType.ConfChangeUpdateNode,
            _ => throw new FormatException(
                $"Unknown configuration token {token}."),
        };
        return new ConfChangeSingle
        {
            Type = type,
            NodeId =
                InteractionParsing.ParseUInt64(
                    token[1..],
                    "configuration node"),
        };
    }
}
