using System.Text.Json;

using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Read;
using DotnetRaft.Storage;

using Google.Protobuf;

namespace DotnetRaft.Tests.Core;

public sealed class RaftCoreDispatchCharacterizationTests
{
    [Fact]
    public void RoleMessageTermMatrixMatchesApproval()
    {
        string actual = GenerateTranscript();
        string path = Path.Combine(
            AppContext.BaseDirectory,
            "Core",
            "TestData",
            "raftcore-dispatch.approved.txt");
        Assert.True(
            File.Exists(path),
            $"Missing dispatch approval {path}.");
        Assert.Equal(
            File.ReadAllText(path)
                .Replace(
                    "\r\n",
                    "\n",
                    StringComparison.Ordinal),
            actual);
    }

    private static string GenerateTranscript()
    {
        var lines = new List<string>();
        foreach (RaftRole role in Enum.GetValues<RaftRole>())
        {
            foreach (MessageType type in
                     Enum.GetValues<MessageType>())
            {
                foreach (MessageVariant variant in
                         Variants(type))
                {
                    lines.Add(
                        Execute(role, type, variant));
                }
            }
        }

        return string.Join('\n', lines) + "\n";
    }

    private static string Execute(
        RaftRole role,
        MessageType type,
        MessageVariant variant)
    {
        var trace = new TranscriptTraceSink();
        var logger = new TranscriptLogger();
        RaftCore core = CreateCore(
            role,
            trace,
            logger);
        Message message = CreateMessage(
            core,
            type,
            variant);
        trace.Events.Clear();
        logger.Events.Clear();
        Exception? failure = null;
        try
        {
            core.Step(message);
        }
        catch (Exception exception)
        {
            failure = exception;
        }

        BasicStatus status = core.GetBasicStatus();
        var record = new
        {
            key =
                $"{role}/{type}/{variant.Name}",
            exception = failure is null
                ? null
                : $"{failure.GetType().FullName}:{failure.Message}",
            status = new
            {
                status.Id,
                status.Term,
                status.Vote,
                status.Commit,
                status.LeaderId,
                role = status.Role.ToString(),
                status.Applied,
                status.LeaderTransferee,
            },
            log = new
            {
                core.Log.Committed,
                core.Log.Applying,
                core.Log.Applied,
                core.Log.LastIndex,
                core.Log.Unstable.Offset,
                core.Log.Unstable.OffsetInProgress,
                core.Log.ApplyingEntriesSize,
            },
            configuration =
                RaftDescriptions.DescribeConfState(
                    core.Tracker.ToConfState()),
            progress = core.GetProgressStatuses()
                .Select(pair =>
                    $"{pair.Key}:{pair.Value.State}/{pair.Value.Match}/{pair.Value.Next}/{pair.Value.PendingSnapshot}/{pair.Value.RecentActive}/{pair.Value.IsPaused}/{pair.Value.InflightCount}")
                .ToArray(),
            core.PendingConfigurationIndex,
            core.UncommittedSize,
            pendingReads =
                core.GatedReadCountForTesting,
            immediate = core.PeekMessages()
                .Select(message =>
                    RaftDescriptions.DescribeMessage(
                        message))
                .ToArray(),
            afterAppend =
                core.PeekMessagesAfterAppend()
                    .Select(message =>
                        RaftDescriptions.DescribeMessage(
                            message))
                    .ToArray(),
            readStates = core.PeekReadStates()
                .Select(state =>
                    $"{state.Index}/{Convert.ToHexString(state.RequestContext.Span).ToLowerInvariant()}")
                .ToArray(),
            traces = trace.Events.Select(FormatTrace)
                .ToArray(),
            logs = logger.Events.ToArray(),
        };
        return JsonSerializer.Serialize(record);
    }

    private static RaftCore CreateCore(
        RaftRole role,
        TranscriptTraceSink trace,
        TranscriptLogger logger)
    {
        var state = new ConfState();
        state.Voters.Add([1UL, 2UL, 3UL]);
        var storage = new MemoryStorage();
        storage.ApplySnapshot(
            new Snapshot
            {
                Metadata = new SnapshotMetadata
                {
                    Index = 2,
                    Term = 1,
                    ConfState = state,
                },
            });
        storage.SetHardState(
            new HardState
            {
                Term = 1,
                Commit = 2,
            });
        var core = new RaftCore(
            new RaftConfig
            {
                Id = 1,
                ElectionTick = 10,
                HeartbeatTick = 1,
                Storage = storage,
                Applied = 2,
                CheckQuorum = true,
                PreVote = true,
                ReadOnlyOption = ReadOnlyOption.Safe,
                TraceSink = trace,
                Logger = logger,
            },
            _ => 0);
        switch (role)
        {
            case RaftRole.Follower:
                break;
            case RaftRole.PreCandidate:
                core.BecomePreCandidate();
                break;
            case RaftRole.Candidate:
                core.BecomeCandidate();
                break;
            case RaftRole.Leader:
                core.BecomeCandidate();
                core.BecomeLeader();
                break;
            default:
                throw new InvalidOperationException(
                    $"Unknown role {role}.");
        }

        core.TakeMessages();
        core.TakeMessagesAfterAppend();
        core.TakeReadStates();
        return core;
    }

    private static Message CreateMessage(
        RaftCore core,
        MessageType type,
        MessageVariant variant)
    {
        ulong lastIndex = core.Log.LastIndex;
        ulong lastTerm = core.Log.GetTerm(lastIndex);
        ulong term = variant.Term switch
        {
            TermRelation.Zero => 0,
            TermRelation.Lower =>
                core.Term == 0 ? 0 : core.Term - 1,
            TermRelation.Equal => core.Term,
            TermRelation.Higher =>
                checked(core.Term + 1),
            _ => throw new InvalidOperationException(),
        };
        var message = new Message
        {
            From = 2,
            To = 1,
            Type = type,
            Term = term,
        };
        switch (type)
        {
            case MessageType.MsgHup:
            case MessageType.MsgBeat:
            case MessageType.MsgCheckQuorum:
            case MessageType.MsgForgetLeader:
                message.From = 1;
                message.Term = 0;
                break;
            case MessageType.MsgProp:
                message.From = 1;
                message.Term = 0;
                message.Entries.Add(
                    new Entry
                    {
                        Data =
                            ByteString.CopyFromUtf8(
                                "proposal"),
                    });
                break;
            case MessageType.MsgApp:
                message.Index = lastIndex;
                message.LogTerm = lastTerm;
                message.Commit = core.Log.Committed;
                message.Entries.Add(
                    new Entry
                    {
                        Index = lastIndex + 1,
                        Term =
                            term == 0
                                ? core.Term
                                : term,
                        Data =
                            ByteString.CopyFromUtf8(
                                "append"),
                    });
                break;
            case MessageType.MsgAppResp:
                message.Index = lastIndex;
                break;
            case MessageType.MsgVote:
            case MessageType.MsgPreVote:
                message.Index = lastIndex;
                message.LogTerm = lastTerm;
                break;
            case MessageType.MsgSnap:
                var snapshotState = new ConfState();
                snapshotState.Voters.Add(
                    [1UL, 2UL, 3UL]);
                message.Snapshot = new Snapshot
                {
                    Metadata = new SnapshotMetadata
                    {
                        Index = lastIndex + 1,
                        Term =
                            Math.Max(lastTerm, 1),
                        ConfState = snapshotState,
                    },
                };
                break;
            case MessageType.MsgHeartbeat:
                message.Commit = core.Log.Committed;
                message.Context =
                    ByteString.CopyFromUtf8("hb");
                break;
            case MessageType.MsgHeartbeatResp:
                break;
            case MessageType.MsgUnreachable:
            case MessageType.MsgSnapStatus:
            case MessageType.MsgTransferLeader:
            case MessageType.MsgTimeoutNow:
                message.Term = 0;
                break;
            case MessageType.MsgReadIndex:
                message.Term = 0;
                message.Entries.Add(
                    new Entry
                    {
                        Data =
                            ByteString.CopyFromUtf8(
                                "read"),
                    });
                break;
            case MessageType.MsgReadIndexResp:
                message.Term = 0;
                message.Index = core.Log.Committed;
                message.Entries.Add(
                    new Entry
                    {
                        Data =
                            ByteString.CopyFromUtf8(
                                "read"),
                    });
                break;
            case MessageType.MsgStorageAppend:
                message.From = 1;
                message.To =
                    RaftLocalMessageTargets.AppendThread;
                message.Term = 0;
                break;
            case MessageType.MsgStorageAppendResp:
                message.From =
                    RaftLocalMessageTargets.AppendThread;
                if (variant.Snapshot)
                {
                    var responseState = new ConfState();
                    responseState.Voters.Add(
                        [1UL, 2UL, 3UL]);
                    message.Snapshot = new Snapshot
                    {
                        Metadata = new SnapshotMetadata
                        {
                            Index = core.Log.Committed,
                            Term = lastTerm,
                            ConfState = responseState,
                        },
                    };
                }

                break;
            case MessageType.MsgStorageApply:
                message.From = 1;
                message.To =
                    RaftLocalMessageTargets.ApplyThread;
                message.Term = 0;
                break;
            case MessageType.MsgStorageApplyResp:
                message.From =
                    RaftLocalMessageTargets.ApplyThread;
                message.Term = 0;
                break;
        }

        return message;
    }

    private static List<MessageVariant> Variants(
        MessageType type)
    {
        bool termBearing = type is
            MessageType.MsgApp
            or MessageType.MsgAppResp
            or MessageType.MsgVote
            or MessageType.MsgVoteResp
            or MessageType.MsgSnap
            or MessageType.MsgHeartbeat
            or MessageType.MsgHeartbeatResp
            or MessageType.MsgPreVote
            or MessageType.MsgPreVoteResp
            or MessageType.MsgStorageAppendResp;
        if (!termBearing)
        {
            return
            [
                new MessageVariant(
                    "zero",
                    TermRelation.Zero,
                    Snapshot: false),
            ];
        }

        var variants =
            new List<MessageVariant>();
        foreach (TermRelation relation in
                 Enum.GetValues<TermRelation>())
        {
            variants.Add(
                new MessageVariant(
                    relation.ToString()
                        .ToLowerInvariant(),
                    relation,
                    Snapshot: false));
            if (type
                == MessageType.MsgStorageAppendResp)
            {
                variants.Add(
                    new MessageVariant(
                        relation.ToString()
                            .ToLowerInvariant()
                        + "-snapshot",
                        relation,
                        Snapshot: true));
            }
        }

        return variants;
    }

    private static string FormatTrace(
        RaftTraceEvent traceEvent)
    {
        RaftTraceMessage? message =
            traceEvent.Message;
        return string.Join(
            '|',
            traceEvent.Type,
            $"{traceEvent.Status.Term}/{traceEvent.Status.Vote}/{traceEvent.Status.Commit}/{traceEvent.Status.LeaderId}/{traceEvent.Status.Role}/{traceEvent.Status.Applied}/{traceEvent.Status.LeaderTransferee}",
            traceEvent.LastLogIndex,
            $"{string.Join(',', traceEvent.Configuration.Voters)};{string.Join(',', traceEvent.Configuration.VotersOutgoing)};{string.Join(',', traceEvent.Configuration.Learners)};{string.Join(',', traceEvent.Configuration.LearnersNext)};{traceEvent.Configuration.AutoLeave}",
            message.HasValue
                ? $"{message.Value.Type}/{message.Value.From}/{message.Value.To}/{message.Value.Term}/{message.Value.LogTerm}/{message.Value.Index}/{message.Value.Commit}/{message.Value.Vote}/{message.Value.Reject}/{message.Value.RejectHint}/{message.Value.EntryCount}/{message.Value.SnapshotIndex}"
                : "-",
            traceEvent.Detail ?? "-");
    }

    private enum TermRelation
    {
        Zero,
        Lower,
        Equal,
        Higher,
    }

    private sealed record MessageVariant(
        string Name,
        TermRelation Term,
        bool Snapshot);

    private sealed class TranscriptTraceSink
        : IRaftTraceSink
    {
        internal List<RaftTraceEvent> Events { get; } = [];

        public void Trace(RaftTraceEvent traceEvent)
        {
            Events.Add(traceEvent);
        }
    }

    private sealed class TranscriptLogger : IRaftLogger
    {
        internal List<string> Events { get; } = [];

        public bool IsEnabled(RaftLogLevel level)
        {
            return true;
        }

        public void Log(
            RaftLogLevel level,
            string message)
        {
            Events.Add($"{level}:{message}");
        }
    }
}
