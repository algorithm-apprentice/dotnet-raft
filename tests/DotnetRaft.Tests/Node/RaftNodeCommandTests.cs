using System.Collections.Concurrent;

using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

using static DotnetRaft.Tests.Node.RaftNodeTestSupport;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Node;

public sealed class RaftNodeCommandTests
{
    [Fact]
    public async Task ProposalBlocksUntilLeaderAndOwnsBytes()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode();
        try
        {
            byte[] payload = "original"u8.ToArray();
            Task proposal = node.ProposeAsync(payload)
                .AsTask();
            payload[0] = (byte)'X';
            await Task.Yield();
            Assert.False(proposal.IsCompleted);

            await node.CampaignAsync();
            Ready election = await WaitReadyAsync(node);
            await PersistAndAdvanceAsync(
                node,
                storage,
                election);
            await proposal.WaitAsync(
                TimeSpan.FromSeconds(2));

            Ready leadership = await WaitReadyAsync(node);
            Assert.Equal(2, leadership.Entries.Count);
            Assert.Equal(
                ByteString.CopyFromUtf8("original"),
                leadership.Entries[^1].Data);
            await PersistAndAdvanceAsync(
                node,
                storage,
                leadership);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task ProposalDropRemainsNonterminal()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode(
                maxUncommittedEntriesSize: 5);
        try
        {
            await BecomeSingletonLeaderAsync(
                node,
                storage);
            await node.ProposeAsync(
                "oversized"u8.ToArray());

            await Assert.ThrowsAsync<ProposalDroppedException>(
                async () => await node.ProposeAsync(
                        "drop"u8.ToArray())
                    .AsTask());
            Assert.False(node.Completion.IsCompleted);
            Assert.Equal(
                RaftRole.Leader,
                (await node.GetStatusAsync()).Basic.Role);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task InvalidNetworkMessageRemainsNonterminal()
    {
        (RaftNode node, _) =
            RestartNode(voters: [1, 2]);
        try
        {
            await Assert.ThrowsAsync<
                ArgumentException>(
                async () => await node.StepAsync(
                        new Message())
                    .AsTask());
            await Assert.ThrowsAsync<
                ArgumentException>(
                async () => await node.StepAsync(
                        new Message
                        {
                            From = 2,
                            To = 1,
                            Type =
                                MessageType.MsgVote,
                        })
                    .AsTask());

            Assert.False(
                node.Completion.IsCompleted);
            _ = await node.GetStatusAsync();
            node.Tick();
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task InvalidConfigurationProposalRemainsNonterminal()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode();
        try
        {
            await BecomeSingletonLeaderAsync(
                node,
                storage);

            await Assert.ThrowsAsync<
                ArgumentException>(
                async () => await node
                    .ProposeConfChangeAsync(
                        new ConfChangeV2
                        {
                            Transition =
                                (ConfChangeTransition)99,
                        })
                    .AsTask());

            Assert.False(
                node.Completion.IsCompleted);
            Assert.Equal(
                RaftRole.Leader,
                (await node.GetStatusAsync())
                    .Basic.Role);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task CanceledBlockedProposalNeverDispatches()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode();
        try
        {
            using var cancellation =
                new CancellationTokenSource();
            Task proposal = node.ProposeAsync(
                    "value"u8.ToArray(),
                    cancellation.Token)
                .AsTask();
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await proposal);

            await node.CampaignAsync();
            Ready election = await WaitReadyAsync(node);
            await PersistAndAdvanceAsync(
                node,
                storage,
                election);

            Ready leadership = await WaitReadyAsync(node);
            Assert.Single(leadership.Entries);
            await PersistAndAdvanceAsync(
                node,
                storage,
                leadership);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task BlockedProposalBacklogDoesNotDelayControlOrTicks()
    {
        (RaftNode node, _) = RestartNode(
            electionTick: 2);
        using var cancellation =
            new CancellationTokenSource();
        try
        {
            Task[] proposals =
                Enumerable.Range(0, 500)
                    .Select(index =>
                        node.ProposeAsync(
                                BitConverter.GetBytes(index),
                                cancellation.Token)
                            .AsTask())
                    .ToArray();

            Status status = await node.GetStatusAsync()
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(2));
            Assert.Equal(RaftRole.Follower, status.Basic.Role);

            for (var tick = 0; tick < 4; tick++)
            {
                node.Tick();
            }

            Ready ready = await WaitReadyAsync(node);
            Assert.Equal(
                RaftRole.Candidate,
                ready.SoftState?.Role);

            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await Task.WhenAll(proposals));
            Assert.All(
                proposals,
                proposal => Assert.True(proposal.IsCanceled));
        }
        finally
        {
            cancellation.Cancel();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task ClaimWinsOverLaterCancellation()
    {
        var trace = new BlockingTraceSink();
        (RaftNode node, _) = RestartNode(
            traceSink: trace);
        try
        {
            using var cancellation =
                new CancellationTokenSource();
            Task campaign = node.CampaignAsync(
                    cancellation.Token)
                .AsTask();
            Assert.True(
                trace.Entered.Wait(
                    TimeSpan.FromSeconds(2)));

            cancellation.Cancel();
            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    TimeSpan.FromSeconds(2)));

            await campaign.WaitAsync(
                TimeSpan.FromSeconds(2));
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task CancellationBeforeClaimPreventsDispatch()
    {
        var trace = new BlockingTraceSink();
        (RaftNode node, _) = RestartNode(
            traceSink: trace);
        using var cancellation =
            new CancellationTokenSource();
        try
        {
            Task claimed =
                node.CampaignAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    TimeSpan.FromSeconds(2)));
            Task canceled = node.StepAsync(
                    new Message
                    {
                        From = 2,
                        To = 1,
                        Type = MessageType.MsgHeartbeat,
                        Term = 50,
                    },
                    cancellation.Token)
                .AsTask();

            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await canceled);
            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    TimeSpan.FromSeconds(2)));
            await claimed.WaitAsync(
                TimeSpan.FromSeconds(2));

            Status status = await node.GetStatusAsync();
            Assert.NotEqual(50UL, status.Basic.Term);
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task GenericRequestCancellationBeforeClaimWins()
    {
        var trace = new BlockingTraceSink();
        (RaftNode node, _) = RestartNode(
            traceSink: trace);
        using var cancellation =
            new CancellationTokenSource();
        try
        {
            Task claimed =
                node.CampaignAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    TimeSpan.FromSeconds(2)));
            Task<Status> canceled =
                node.GetStatusAsync(
                        cancellation.Token)
                    .AsTask();

            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await canceled);
            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    TimeSpan.FromSeconds(2)));
            await claimed.WaitAsync(
                TimeSpan.FromSeconds(2));

            _ = await node.GetStatusAsync();
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task PreCanceledRequestsNeverEnterAnyLane()
    {
        (RaftNode node, _) = RestartNode();
        using var cancellation =
            new CancellationTokenSource();
        cancellation.Cancel();
        try
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await node.CampaignAsync(
                        cancellation.Token)
                    .AsTask());
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await node.ProposeAsync(
                        "value"u8.ToArray(),
                        cancellation.Token)
                    .AsTask());
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await node.WaitForReadyAsync(
                        cancellation.Token)
                    .AsTask());
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await node.AdvanceAsync(
                        cancellation.Token)
                    .AsTask());
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await node.GetStatusAsync(
                        cancellation.Token)
                    .AsTask());

            Status status = await node.GetStatusAsync();
            Assert.Equal(
                RaftRole.Follower,
                status.Basic.Role);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task NullMutableInputsAreRejectedBeforeEnqueue()
    {
        (RaftNode node, _) = RestartNode();
        try
        {
            ArgumentNullException v1Proposal =
                Assert.Throws<ArgumentNullException>(
                    () =>
                    {
                        _ = node.ProposeConfChangeAsync(
                            (ProtocolConfChange)null!)
                            .AsTask();
                    });
            Assert.Equal(
                "change",
                v1Proposal.ParamName);
            ArgumentNullException v2Proposal =
                Assert.Throws<ArgumentNullException>(
                    () =>
                    {
                        _ = node.ProposeConfChangeAsync(
                            (ConfChangeV2)null!)
                            .AsTask();
                    });
            Assert.Equal(
                "change",
                v2Proposal.ParamName);
            ArgumentNullException step =
                Assert.Throws<ArgumentNullException>(
                    () =>
                    {
                        _ = node.StepAsync(null!)
                            .AsTask();
                    });
            Assert.Equal(
                "message",
                step.ParamName);
            ArgumentNullException v1Apply =
                Assert.Throws<ArgumentNullException>(
                    () =>
                    {
                        _ = node.ApplyConfChangeAsync(
                            (ProtocolConfChange)null!)
                            .AsTask();
                    });
            Assert.Equal(
                "change",
                v1Apply.ParamName);
            ArgumentNullException v2Apply =
                Assert.Throws<ArgumentNullException>(
                    () =>
                    {
                        _ = node.ApplyConfChangeAsync(
                            (ConfChangeV2)null!)
                            .AsTask();
                    });
            Assert.Equal(
                "change",
                v2Apply.ParamName);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task MessageProposalWaitsInProposalLaneWithoutLeader()
    {
        (RaftNode node, _) = RestartNode();
        using var cancellation =
            new CancellationTokenSource();
        try
        {
            var proposal = new Message
            {
                From = 99,
                To = 1,
                Type = MessageType.MsgProp,
            };
            proposal.Entries.Add(
                new Entry
                {
                    Data =
                        ByteString.CopyFromUtf8("blocked"),
                });
            Task blocked = node.StepAsync(
                    proposal,
                    cancellation.Token)
                .AsTask();

            await node.GetStatusAsync();
            await node.GetStatusAsync();
            await node.GetStatusAsync();
            Assert.False(blocked.IsCompleted);

            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await blocked);
        }
        finally
        {
            cancellation.Cancel();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task AdministrativeCommandsUseTheOwnerLoop()
    {
        var messages =
            new ConcurrentQueue<RaftTraceMessage>();
        var trace = new CallbackTraceSink
        {
            OnTrace = traceEvent =>
            {
                if (traceEvent.Type
                        == RaftTraceEventType.MessageReceived
                    && traceEvent.Message.HasValue)
                {
                    messages.Enqueue(
                        traceEvent.Message.Value);
                }
            },
        };
        (RaftNode node, _) = RestartNode(
            voters: [1, 2],
            traceSink: trace);
        try
        {
            await node.ForgetLeaderAsync();
            await node.TransferLeadershipAsync(2);
            await node.ReportUnreachableAsync(2);
            await node.ReportSnapshotAsync(
                2,
                SnapshotStatus.Success);
            await node.ReportSnapshotAsync(
                2,
                SnapshotStatus.Failure);

            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                async () => await node.ReportSnapshotAsync(
                        2,
                        (SnapshotStatus)99)
                    .AsTask());
            Assert.False(node.Completion.IsCompleted);

            RaftTraceMessage[] actual =
                messages.ToArray();
            Assert.Equal(
                [
                    MessageType.MsgForgetLeader,
                    MessageType.MsgTransferLeader,
                    MessageType.MsgUnreachable,
                    MessageType.MsgSnapStatus,
                    MessageType.MsgSnapStatus,
                ],
                actual.Select(message => message.Type));
            Assert.False(actual[^2].Reject);
            Assert.True(actual[^1].Reject);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task ConfigurationProposalsRemainOwnedAndFifo()
    {
        var block = 0;
        var trace = new BlockingTraceSink(
            traceEvent =>
                Volatile.Read(ref block) != 0
                && traceEvent.Type
                    == RaftTraceEventType.MessageReceived
                && traceEvent.Message?.Type
                    == MessageType.MsgAppResp);
        (RaftNode node, MemoryStorage storage) =
            RestartNode(
                voters: [1, 2],
                traceSink: trace);
        try
        {
            await node.StepAsync(
                new Message
                {
                    From = 2,
                    To = 1,
                    Type = MessageType.MsgHeartbeat,
                    Term = 1,
                });
            await PersistAndAdvanceAsync(
                node,
                storage,
                await WaitReadyAsync(node));

            Volatile.Write(ref block, 1);
            Task blocker = node.StepAsync(
                    new Message
                    {
                        From = 2,
                        To = 1,
                        Type = MessageType.MsgAppResp,
                        Term = 1,
                    })
                .AsTask();
            Assert.True(
                trace.Entered.Wait(
                    TimeSpan.FromSeconds(2)));

            var v1 = new ProtocolConfChange
            {
                Type = ConfChangeType.ConfChangeAddNode,
                NodeId = 3,
                Context = ByteString.CopyFromUtf8("v1"),
            };
            var v2 = new ConfChangeV2
            {
                Context = ByteString.CopyFromUtf8("v2"),
            };
            v2.Changes.Add(new ConfChangeSingle
            {
                Type =
                    ConfChangeType.ConfChangeAddLearnerNode,
                NodeId = 4,
            });
            byte[] context = "read-context"u8.ToArray();

            Task first =
                node.ProposeConfChangeAsync(v1).AsTask();
            Task second =
                node.ProposeConfChangeAsync(v2).AsTask();
            Task read =
                node.ReadIndexAsync(context).AsTask();
            v1.NodeId = 99;
            v2.Changes[0].NodeId = 99;
            context[0] = (byte)'X';

            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    TimeSpan.FromSeconds(2)));
            await Task.WhenAll(
                    blocker,
                    first,
                    second,
                    read)
                .WaitAsync(TimeSpan.FromSeconds(2));

            Ready ready = await WaitReadyAsync(node);
            Message[] proposals =
            [
                .. ready.Messages.Where(
                    message =>
                        message.Type
                        == MessageType.MsgProp),
            ];
            Assert.Equal(2, proposals.Length);
            Entry firstEntry =
                Assert.Single(proposals[0].Entries);
            Entry secondEntry =
                Assert.Single(proposals[1].Entries);
            Assert.Equal(
                EntryType.EntryConfChange,
                firstEntry.Type);
            Assert.Equal(
                3UL,
                ProtocolConfChange.Parser
                    .ParseFrom(firstEntry.Data)
                    .NodeId);
            Assert.Equal(
                EntryType.EntryConfChangeV2,
                secondEntry.Type);
            Assert.Equal(
                4UL,
                ConfChangeV2.Parser
                    .ParseFrom(secondEntry.Data)
                    .Changes[0]
                    .NodeId);

            Message readRequest = Assert.Single(
                ready.Messages,
                message =>
                    message.Type
                    == MessageType.MsgReadIndex);
            Assert.Equal(
                ByteString.CopyFromUtf8("read-context"),
                Assert.Single(readRequest.Entries).Data);
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task V2ConfigurationApplicationOwnsInputAndOutput()
    {
        var trace = new BlockingTraceSink();
        (RaftNode node, _) = RestartNode(
            traceSink: trace);
        try
        {
            Task campaign =
                node.CampaignAsync().AsTask();
            Assert.True(
                trace.Entered.Wait(
                    TimeSpan.FromSeconds(2)));
            var change = new ConfChangeV2();
            change.Changes.Add(new ConfChangeSingle
            {
                Type =
                    ConfChangeType.ConfChangeAddLearnerNode,
                NodeId = 2,
            });

            Task<ConfState> apply =
                node.ApplyConfChangeAsync(change)
                    .AsTask();
            change.Changes[0].NodeId = 99;
            trace.Release.Set();
            Assert.True(
                trace.Exited.Wait(
                    TimeSpan.FromSeconds(2)));

            await campaign.WaitAsync(
                TimeSpan.FromSeconds(2));
            ConfState result = await apply.WaitAsync(
                TimeSpan.FromSeconds(2));
            Assert.Equal([2UL], result.Learners);
            result.Learners[0] = 99;

            Status status = await node.GetStatusAsync();
            Assert.Equal(
                [2UL],
                status.Configuration.Learners);
        }
        finally
        {
            trace.Release.Set();
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task InvalidV2ApplicationFaultsGenericRequest()
    {
        (RaftNode node, _) = RestartNode();
        try
        {
            var invalid = new ConfChangeV2
            {
                Transition =
                    (ConfChangeTransition)999,
            };
            invalid.Changes.Add(new ConfChangeSingle
            {
                Type = ConfChangeType.ConfChangeAddNode,
                NodeId = 2,
            });

            InvalidOperationException trigger =
                await Assert.ThrowsAnyAsync<InvalidOperationException>(
                    async () => await node.ApplyConfChangeAsync(
                            invalid)
                        .AsTask());
            RaftNodeFaultedException completion =
                await Assert.ThrowsAsync<RaftNodeFaultedException>(
                    async () => await node.Completion);
            Assert.Same(
                trigger,
                completion.InnerException);
            Assert.NotNull(node.TerminalStatus);
            await Assert.ThrowsAsync<RaftNodeFaultedException>(
                async () => await node.GetStatusAsync()
                    .AsTask());
        }
        finally
        {
            try
            {
                await node.StopAsync();
            }
            catch (RaftNodeFaultedException)
            {
            }
        }
    }

    [Fact]
    public async Task LocalMessagesAndUnknownResponsesAreIgnored()
    {
        (RaftNode node, _) = RestartNode(
            voters: [1, 2]);
        try
        {
            await node.StepAsync(
                new Message
                {
                    From = 2,
                    To = 1,
                    Type = MessageType.MsgHup,
                });
            await node.StepAsync(
                new Message
                {
                    From = 99,
                    To = 1,
                    Type = MessageType.MsgAppResp,
                    Term = 10,
                });

            Status status =
                await node.GetStatusAsync();
            Assert.Equal(0UL, status.Basic.Term);
            Assert.Equal(RaftRole.Follower, status.Basic.Role);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task ReservedStorageSenderFailsUntilD25()
    {
        (RaftNode node, _) = RestartNode();
        try
        {
            NotSupportedException storageResponse =
                await Assert.ThrowsAsync<NotSupportedException>(
                async () => await node.StepAsync(
                    new Message
                    {
                        From = ulong.MaxValue,
                        To = 1,
                        Type =
                            MessageType.MsgStorageAppendResp,
                    }).AsTask());
            Assert.Equal(
                "Storage-thread responses require asynchronous storage writes.",
                storageResponse.Message);
            NotSupportedException reservedSender =
                await Assert.ThrowsAsync<NotSupportedException>(
                async () => await node.StepAsync(
                    new Message
                    {
                        From =
                            RaftLocalMessageTargets
                                .AppendThread,
                        To = 1,
                        Type = MessageType.MsgAppResp,
                    }).AsTask());
            Assert.Equal(
                "Storage-thread responses require asynchronous storage writes.",
                reservedSender.Message);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task StepOwnsMessageBeforeDispatch()
    {
        (RaftNode node, _) = RestartNode(
            voters: [1, 2]);
        try
        {
            var message = new Message
            {
                From = 2,
                To = 1,
                Type = MessageType.MsgHeartbeat,
                Term = 1,
            };
            ValueTask step = node.StepAsync(message);
            message.From = 1;
            message.Type = MessageType.MsgHup;
            message.Term = 0;
            await step;

            Status status =
                await node.GetStatusAsync();
            Assert.Equal(1UL, status.Basic.Term);
            Assert.Equal(2UL, status.Basic.LeaderId);
            Assert.Equal(RaftRole.Follower, status.Basic.Role);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task MsgPropNormalizesSenderAndWaitsForDispatch()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode(voters: [1, 2]);
        try
        {
            await node.StepAsync(
                new Message
                {
                    From = 2,
                    To = 1,
                    Type = MessageType.MsgHeartbeat,
                    Term = 1,
                });
            Ready heartbeat = await WaitReadyAsync(node);
            await PersistAndAdvanceAsync(
                node,
                storage,
                heartbeat);

            var proposal = new Message
            {
                From = 99,
                To = 1,
                Type = MessageType.MsgProp,
            };
            proposal.Entries.Add(
                new Entry
                {
                    Data =
                        ByteString.CopyFromUtf8("value"),
                });
            await node.StepAsync(proposal);

            Ready ready = await WaitReadyAsync(node);
            Message forwarded = Assert.Single(
                ready.Messages,
                message =>
                    message.Type
                    == MessageType.MsgProp);
            Assert.Equal(1UL, forwarded.From);
            Assert.Equal(2UL, forwarded.To);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task LearnerProgressDoesNotDisableProposals()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode(
                voters: [2],
                learners: [1]);
        try
        {
            await node.StepAsync(
                new Message
                {
                    From = 2,
                    To = 1,
                    Type = MessageType.MsgHeartbeat,
                    Term = 1,
                });
            Ready heartbeat = await WaitReadyAsync(node);
            await PersistAndAdvanceAsync(
                node,
                storage,
                heartbeat);

            await node.ProposeAsync("value"u8.ToArray());
            Ready ready = await WaitReadyAsync(node);
            Assert.Contains(
                ready.Messages,
                message =>
                    message.Type
                    == MessageType.MsgProp
                    && message.To == 2);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task RemovalBlocksAndReadditionEnablesProposals()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode(voters: [1, 2]);
        try
        {
            await node.StepAsync(
                new Message
                {
                    From = 2,
                    To = 1,
                    Type = MessageType.MsgHeartbeat,
                    Term = 1,
                });
            Ready heartbeat = await WaitReadyAsync(node);
            await PersistAndAdvanceAsync(
                node,
                storage,
                heartbeat);
            await node.ApplyConfChangeAsync(
                new ProtocolConfChange
                {
                    Type =
                        ConfChangeType.ConfChangeRemoveNode,
                    NodeId = 1,
                });

            using var cancellation =
                new CancellationTokenSource();
            Task blocked = node.ProposeAsync(
                    "blocked"u8.ToArray(),
                    cancellation.Token)
                .AsTask();
            await Task.Yield();
            Assert.False(blocked.IsCompleted);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await blocked);

            await node.ApplyConfChangeAsync(
                new ProtocolConfChange
                {
                    Type =
                        ConfChangeType.ConfChangeAddNode,
                    NodeId = 1,
                });
            await node.ProposeAsync(
                "accepted"u8.ToArray());
            Ready forwarded = await WaitReadyAsync(node);
            Assert.Contains(
                forwarded.Messages,
                message =>
                    message.Type
                    == MessageType.MsgProp
                    && message.To == 2);
        }
        finally
        {
            await node.StopAsync();
        }
    }

    [Fact]
    public async Task InitialProgressObservationDetectsImmediateRemoval()
    {
        (RaftNode node, MemoryStorage storage) =
            RestartNode(voters: [1, 2]);
        using var cancellation =
            new CancellationTokenSource();
        try
        {
            await node.ApplyConfChangeAsync(
                new ProtocolConfChange
                {
                    Type =
                        ConfChangeType.ConfChangeRemoveNode,
                    NodeId = 1,
                });
            await node.StepAsync(
                new Message
                {
                    From = 2,
                    To = 1,
                    Type = MessageType.MsgHeartbeat,
                    Term = 1,
                });
            await PersistAndAdvanceAsync(
                node,
                storage,
                await WaitReadyAsync(node));

            Task blocked = node.ProposeAsync(
                    "blocked"u8.ToArray(),
                    cancellation.Token)
                .AsTask();
            await node.GetStatusAsync();
            await node.GetStatusAsync();
            await node.GetStatusAsync();
            Assert.False(blocked.IsCompleted);

            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await blocked);
        }
        finally
        {
            cancellation.Cancel();
            await node.StopAsync();
        }
    }
}
