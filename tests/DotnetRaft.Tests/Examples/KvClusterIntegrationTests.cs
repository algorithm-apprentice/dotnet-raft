using System.Collections.Concurrent;
using System.Threading.Channels;

using DotnetRaft.Examples.KvCluster;
using DotnetRaft.Protocol;
using DotnetRaft.Storage.Sqlite;

using Google.Protobuf;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;

namespace DotnetRaft.Tests.Examples;

public sealed class KvClusterIntegrationTests
{
    private static readonly TimeSpan PhaseTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task ThreeNodeClusterReplicatesFollowerProposalAndRead()
    {
        var network = new InMemoryNetwork();
        NodeHarness[] nodes =
        [
            CreateNode(1, network),
            CreateNode(2, network),
            CreateNode(3, network),
        ];
        try
        {
            await StartAndElectAsync(network, nodes);

            ProposalResponse proposal =
                await nodes[1].Host.PutAsync(
                    "color",
                    "blue",
                    CancellationToken.None);
            Assert.True(proposal.AppliedIndex >= 4);

            await WaitForApplicationAsync(nodes, proposal.AppliedIndex);
            Assert.All(nodes, node => Assert.Equal("blue", node.State.ReadLocal("color").Value));

            ReadResponse read =
                await nodes[2].Host.ReadAsync(
                    "color",
                    CancellationToken.None);
            Assert.True(read.Linearizable);
            Assert.True(read.Found);
            Assert.Equal("blue", read.Value);
            Assert.True(
                read.PhysicalApplied >=
                read.RequiredIndex);

            ProposalResponse deletion = await nodes[1].Host.DeleteAsync(
                "color",
                requestId: null,
                CancellationToken.None);
            await WaitForApplicationAsync(nodes, deletion.AppliedIndex);
            Assert.All(nodes, node => Assert.False(node.State.ReadLocal("color").Found));
        }
        finally
        {
            await StopAndDisposeNodesAsync(nodes, disposeDirectories: true);
        }
    }

    [Fact]
    public async Task ThreeNodeClusterRestartsDurably()
    {
        TemporaryNodeDirectory[] directories =
        [
            new(),
            new(),
            new(),
        ];
        Guid requestId = Guid.Parse(
            "11111111-2222-3333-4444-555555555555");
        try
        {
            ProposalResponse original;
            {
                var network = new InMemoryNetwork();
                NodeHarness[] nodes =
                [
                    CreateNode(
                        1,
                        network,
                        directory: directories[0]),
                    CreateNode(
                        2,
                        network,
                        directory: directories[1]),
                    CreateNode(
                        3,
                        network,
                        directory: directories[2]),
                ];
                try
                {
                    await StartAndElectAsync(network, nodes);
                    original =
                        await nodes[1].Host.PutAsync(
                            "color",
                            "blue",
                            requestId,
                            CancellationToken.None);
                    await WaitForApplicationAsync(nodes, original.AppliedIndex);
                    Assert.All(nodes, node => Assert.Equal("blue", node.State.ReadLocal("color").Value));
                }
                finally
                {
                    await StopAndDisposeNodesAsync(
                        nodes,
                        disposeDirectories: false);
                }
            }

            {
                var network = new InMemoryNetwork();
                NodeHarness[] nodes =
                [
                    CreateNode(
                        1,
                        network,
                        directory: directories[0]),
                    CreateNode(
                        2,
                        network,
                        directory: directories[1]),
                    CreateNode(
                        3,
                        network,
                        directory: directories[2]),
                ];
                try
                {
                    await StartNodesAsync(network, nodes);
                    await WaitForApplicationAsync(nodes, original.AppliedIndex);
                    Assert.All(nodes, node => Assert.Equal("blue", node.State.ReadLocal("color").Value));
                    await ElectAsync(nodes[0]);
                    await nodes[1].Host.PutAsync(
                        "shape",
                        "circle",
                        Guid.Parse(
                            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                        CancellationToken.None);
                    ReadResponse freshRead =
                        await nodes[2].Host.ReadAsync(
                            "shape",
                            CancellationToken.None);
                    Assert.Equal(
                        "circle",
                        freshRead.Value);
                    ProposalResponse duplicate =
                        await nodes[2].Host.PutAsync(
                            "color",
                            "blue",
                            requestId,
                            CancellationToken.None);
                    Assert.True(duplicate.Duplicate);
                    Assert.Equal(
                        original.AppliedIndex,
                        duplicate.AppliedIndex);
                }
                finally
                {
                    await StopAndDisposeNodesAsync(
                        nodes,
                        disposeDirectories: false);
                }
            }
        }
        finally
        {
            foreach (TemporaryNodeDirectory directory in
                     directories)
            {
                directory.Dispose();
            }
        }
    }

    [Fact]
    public async Task ConcurrentConflictingRequestIdsReturnOneConflict()
    {
        var network = new InMemoryNetwork();
        NodeHarness[] nodes =
        [
            CreateNode(1, network),
            CreateNode(2, network),
            CreateNode(3, network),
        ];
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        try
        {
            await StartAndElectAsync(network, nodes);

            Guid requestId = Guid.Parse(
                "11111111-2222-3333-4444-555555555555");
            var first = new KvCommand(requestId, KvCommandType.Set, "first", "one");
            var second = new KvCommand(requestId, KvCommandType.Set, "second", "two");
            ProposalResponse?[] results = await Task.WhenAll(
                ObserveProposalAsync(nodes[1].Host, first), ObserveProposalAsync(nodes[2].Host, second));
            ProposalResponse winner = Assert.Single(results.OfType<ProposalResponse>());
            (KvCommand winning, KvCommand losing) = results[0] is not null ? (first, second) : (second, first);

            ReadResponse barrier = await nodes[0].Host.ReadAsync(winning.Key, guard.Token);
            Assert.True(barrier.Linearizable);
            Assert.True(barrier.RequiredIndex >= winner.AppliedIndex);
            await WaitForApplicationAsync(nodes, barrier.RequiredIndex);
            Assert.All(nodes, node =>
            {
                Assert.Equal(winning.Value, node.State.ReadLocal(winning.Key).Value);
                Assert.False(node.State.ReadLocal(losing.Key).Found);
                Assert.Equal(new KvRequestResolution(winner.AppliedIndex, false), node.State.ResolveRequest(winning));
                Assert.Equal(new KvRequestResolution(winner.AppliedIndex, true), node.State.ResolveRequest(losing));
            });
        }
        finally
        {
            await StopAndDisposeNodesAsync(nodes, disposeDirectories: true);
        }

        async Task<ProposalResponse?> ObserveProposalAsync(RaftClusterHost host, KvCommand command)
        {
            try
            {
                return await host.PutAsync(command.Key, command.Value!, command.RequestId, guard.Token);
            }
            catch (KvRequestConflictException)
            {
                return null;
            }
        }
    }

    [Fact]
    public async Task ConcurrentClientsProduceALinearizableSingleKeyHistory()
    {
        var network = new InMemoryNetwork();
        NodeHarness[] nodes = [CreateNode(1, network), CreateNode(2, network), CreateNode(3, network)];
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var history = new List<RegisterOperation>();
        var order = 0;
        try
        {
            await StartAndElectAsync(network, nodes);
            for (var round = 0; round < 4; round++)
            {
                var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                Task<RegisterOperation>[] attempts =
                [
                    MutateAsync(start.Task, nodes[1].Host, $"round-{round}"),
                    ReadAsync(start.Task, nodes[0].Host),
                    MutateAsync(start.Task, nodes[2].Host, null),
                ];
                start.SetResult();
                history.AddRange(await Task.WhenAll(attempts));
            }

            history.Add(await ReadAsync(Task.CompletedTask, nodes[2].Host));
            Assert.True(RegisterHistory.IsLinearizable(history), string.Join(Environment.NewLine, history));
        }
        finally
        {
            await StopAndDisposeNodesAsync(nodes, disposeDirectories: true);
        }

        async Task<RegisterOperation> MutateAsync(Task start, RaftClusterHost host, string? value)
        {
            await start;
            int invoked = Interlocked.Increment(ref order);
            if (value is null)
            {
                await host.DeleteAsync("register", Guid.NewGuid(), guard.Token);
            }
            else
            {
                await host.PutAsync("register", value, Guid.NewGuid(), guard.Token);
            }

            return new RegisterOperation(invoked, Interlocked.Increment(ref order), IsRead: false, value);
        }

        async Task<RegisterOperation> ReadAsync(Task start, RaftClusterHost host)
        {
            await start;
            int invoked = Interlocked.Increment(ref order);
            ReadResponse read = await host.ReadAsync("register", guard.Token);
            int completed = Interlocked.Increment(ref order);
            Assert.True(read.Linearizable);
            Assert.Equal(read.Value is not null, read.Found);
            return new RegisterOperation(invoked, completed, IsRead: true, read.Value);
        }
    }

    [Fact]
    public async Task ConfirmedFollowerReadWaitsForItsLocalApplication()
    {
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var readCancellation = CancellationTokenSource.CreateLinkedTokenSource(guard.Token);
        var transport = new ScriptedTransport();
        var reads = new PendingReadRegistry();
        var target = CreateNode(1, new InMemoryNetwork(), transportOverride: transport, readRegistry: reads);
        Task<ReadResponse>? read = null;
        try
        {
            await target.Host.StartAsync(guard.Token);
            await target.Host.GetStatusAsync(guard.Token);
            await SendHeartbeatAsync(marker: 1);
            await ObserveHeartbeatReplyAsync(marker: 1);
            transport.ReleaseHeartbeat();

            read = target.Host.ReadAsync("color", readCancellation.Token);
            Message request = await transport.NextAsync(guard.Token);
            Assert.Equal(MessageType.MsgReadIndex, request.Type);
            Assert.Equal(2UL, request.To);
            Entry context = Assert.Single(request.Entries);

            // Hold the consumer in Ready 2 while the owner accepts the read
            // result and heartbeat 3. Ready 3 sends its heartbeat before it
            // handles the read; Ready 4 is the post-Advance fence for Ready 3.
            await SendHeartbeatAsync(marker: 2);
            await ObserveHeartbeatReplyAsync(marker: 2);
            var response = new Message { Type = MessageType.MsgReadIndexResp, From = 2, To = 1, Term = 2, Index = 4 };
            response.Entries.Add(context.Clone());
            await target.Host.ReceiveAsync(response, guard.Token);
            await SendHeartbeatAsync(marker: 3);
            transport.ReleaseHeartbeat();
            await ObserveHeartbeatReplyAsync(marker: 3);
            await SendHeartbeatAsync(marker: 4);
            transport.ReleaseHeartbeat();
            await ObserveHeartbeatReplyAsync(marker: 4);

            Assert.Equal(3UL, target.State.PhysicalApplied);
            Assert.False(target.State.ReadLocal("color").Found);
            // Releasing the barrier removes its registration synchronously;
            // the client task alone can race its asynchronous continuation.
            Guid requestId = new(context.Data.Span);
            Assert.Throws<InvalidOperationException>(() => { _ = reads.Register(requestId); });
            Assert.False(read.IsCompleted, "A confirmed read must not complete before local application.");

            var command = new KvCommand(Guid.NewGuid(), KvCommandType.Set, "color", "blue");
            var append = new Message
            {
                Type = MessageType.MsgApp,
                From = 2,
                To = 1,
                Term = 2,
                Index = 3,
                LogTerm = 1,
                Commit = 4,
            };
            append.Entries.Add(new Entry
            {
                Index = 4,
                Term = 2,
                Data = ByteString.CopyFrom(DurableKvCommandCodec.Encode(command)),
            });
            await target.Host.ReceiveAsync(append, guard.Token);
            transport.ReleaseHeartbeat();

            ReadResponse result = await read.WaitAsync(guard.Token);
            Assert.True(result.Linearizable);
            Assert.True(result.Found);
            Assert.Equal("blue", result.Value);
            Assert.Equal(4UL, result.RequiredIndex);
            Assert.Equal(4UL, result.PhysicalApplied);
        }
        finally
        {
            readCancellation.Cancel();
            transport.ReleaseHeartbeat();
            try
            {
                await StopAndDisposeNodesAsync([target], disposeDirectories: true);
            }
            finally
            {
                if (read is not null)
                {
                    await ((Task)read).WaitAsync(PhaseTimeout).ConfigureAwait(
                        ConfigureAwaitOptions.ContinueOnCapturedContext | ConfigureAwaitOptions.SuppressThrowing);
                }
            }
        }

        ValueTask SendHeartbeatAsync(byte marker) => target.Host.ReceiveAsync(new Message
        {
            Type = MessageType.MsgHeartbeat,
            From = 2,
            To = 1,
            Term = 2,
            Commit = 3,
            Context = ByteString.CopyFrom(marker),
        }, guard.Token);

        async Task ObserveHeartbeatReplyAsync(byte marker)
        {
            Message reply = await transport.NextAsync(guard.Token);
            Assert.Equal(MessageType.MsgHeartbeatResp, reply.Type);
            Assert.Equal(ByteString.CopyFrom(marker), reply.Context);
        }
    }

    [Fact]
    public async Task OversizeSnapshotRetriesAfterThreshold()
    {
        var network = new InMemoryNetwork();
        NodeHarness[] nodes =
        [
            CreateNode(
                1,
                network,
                maxTransportMessageBytes:
                    256 * 1024,
                snapshotThresholdEntries: 3),
            CreateNode(
                2,
                network,
                maxTransportMessageBytes:
                    256 * 1024,
                snapshotThresholdEntries: 3),
            CreateNode(
                3,
                network,
                maxTransportMessageBytes:
                    256 * 1024,
                snapshotThresholdEntries: 3),
        ];
        try
        {
            await StartAndElectAsync(network, nodes);

            string large = new(
                'x',
                100 * 1024);
            for (var index = 0;
                 index < 3;
                 index++)
            {
                await nodes[1].Host.PutAsync(
                    $"large-{index}",
                    large,
                    Guid.NewGuid(),
                    CancellationToken.None);
            }

            await WaitForStatusAsync(nodes, "all replicas to attempt an oversized snapshot",
                (node, status) => status.LogicalApplied >= 6 && SnapshotAttemptIndex(node.Host) >= 6);
            Assert.All(
                nodes,
                node => Assert.Equal(
                    3UL,
                    node.Storage.GetSnapshot()
                        .Metadata.Index));

            await nodes[1].Host.PutAsync(
                "small",
                "value",
                Guid.NewGuid(),
                CancellationToken.None);
            await WaitForStatusAsync(nodes, "all replicas to acknowledge application through index 7",
                (_, status) => status.LogicalApplied >= 7);

            Assert.All(
                nodes,
                node => Assert.Equal(
                    6UL,
                    SnapshotAttemptIndex(
                        node.Host)));
        }
        finally
        {
            await StopAndDisposeNodesAsync(
                nodes,
                disposeDirectories: true);
        }
    }

    [Fact]
    public async Task AutomaticTickLoopConsumesManualTicks()
    {
        var network = new InMemoryNetwork();
        NodeHarness node = CreateNode(
            1,
            network,
            automaticTicks: true);
        network.Register(node.Host);

        try
        {
            await node.Host.StartAsync(
                CancellationToken.None);
            await WaitForApplicationAsync([node], 3);

            for (var tick = 0; tick < 20; tick++)
            {
                node.TickSource.Tick();
            }

            await WaitForStatusAsync([node], "manual ticks to trigger pre-vote", (_, status) => status.Role == RaftRole.PreCandidate);
        }
        finally
        {
            await StopAndDisposeNodesAsync([node], disposeDirectories: true);
        }
    }

    [Fact]
    public async Task ReadyLoopFaultStopsApplicationAndNode()
    {
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var transport = new FaultingTransport();
        NodeHarness target = CreateNode(1, new InMemoryNetwork(), transportOverride: transport);
        try
        {
            await target.Host.StartAsync(guard.Token);
            await target.Host.CampaignAsync(guard.Token);
            await transport.Called.WaitAsync(guard.Token);
            InvalidOperationException fault = await Assert.ThrowsAsync<InvalidOperationException>(
                () => target.Host.ExecuteTask!.WaitAsync(guard.Token));
            Assert.Equal("transport fault", fault.Message);
            Assert.True(target.Lifetime.ApplicationStopping.IsCancellationRequested);
            await Assert.ThrowsAsync<RaftNodeStoppedException>(
                () => target.Host.GetStatusAsync(guard.Token).WaitAsync(guard.Token));
        }
        finally
        {
            await StopAndDisposeNodesAsync([target], disposeDirectories: true);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task IncomingSnapshotIsAcknowledgedOnlyAfterSuccessfulApplicationRestore(bool validPayload)
    {
        const ulong snapshotIndex = 5;
        const ulong snapshotTerm = 2;
        using var directory = new TemporaryNodeDirectory();
        using var donorDirectory = new TemporaryNodeDirectory();
        using var donor = new SqliteKeyValueStateMachine(donorDirectory.ApplicationPath, nodeId: 2);
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var membership = new FixedMembershipConfiguration([1UL, 2UL, 3UL]);
        for (ulong index = 1; index <= 3; index++)
        {
            donor.ApplyConfiguration(index, membership.ExpectedConfState(index));
        }

        donor.AdvanceNoOp(4);
        var command = new KvCommand(Guid.NewGuid(), KvCommandType.Set, "color", "blue");
        donor.ApplyCommand(snapshotIndex, ByteString.CopyFrom(DurableKvCommandCodec.Encode(command)));
        var transport = new GatedTransport();
        var proposals = new PendingProposalRegistry();
        NodeHarness target = CreateNode(1, new InMemoryNetwork(), directory: directory,
            transportOverride: transport, proposalRegistry: proposals);
        using var pending = proposals.Register(command);
        try
        {
            await target.Host.StartAsync(guard.Token);
            await target.Host.GetStatusAsync(guard.Token);
            await target.Host.ReceiveAsync(new Message
            {
                Type = MessageType.MsgSnap,
                From = 2,
                To = 1,
                Term = snapshotTerm,
                Snapshot = new Snapshot
                {
                    Metadata = new SnapshotMetadata
                    {
                        Index = snapshotIndex,
                        Term = snapshotTerm,
                        ConfState = donor.ConfState,
                    },
                    Data = validPayload ? donor.CreateSnapshotData() : ByteString.CopyFromUtf8("not JSON"),
                },
            }, guard.Token);

            Message response = await transport.Entered.WaitAsync(guard.Token);
            Assert.Equal(MessageType.MsgAppResp, response.Type);
            Assert.Equal(2UL, response.To);
            Assert.Equal(snapshotIndex, response.Index);
            Assert.Equal(snapshotIndex, target.Storage.GetPendingApplicationSnapshot()?.Metadata.Index);
            Assert.Equal(snapshotIndex, target.Storage.GetHardState()?.Commit);
            Assert.Equal(3UL, target.State.PhysicalApplied);
            Assert.False(pending.Task.IsCompleted);

            transport.Release();
            if (validPayload)
            {
                KvApplyResult result = await pending.Task.WaitAsync(guard.Token);
                Assert.True(result.Duplicate);
                Assert.False(result.Conflict);
                Assert.Equal(snapshotIndex, result.ResultIndex);
                Assert.Null(target.Storage.GetPendingApplicationSnapshot());
                Assert.Equal(snapshotIndex, target.State.PhysicalApplied);
                Assert.Equal("blue", target.State.ReadLocal("color").Value);
            }
            else
            {
                await Assert.ThrowsAsync<InvalidDataException>(async () =>
                    await target.Host.ExecuteTask!.WaitAsync(guard.Token));
                Assert.True(target.Lifetime.ApplicationStopping.IsCancellationRequested);
                Assert.Equal(snapshotIndex, target.Storage.GetPendingApplicationSnapshot()?.Metadata.Index);
                Assert.Equal(3UL, target.State.PhysicalApplied);
                Assert.False(pending.Task.IsCompleted);
            }
        }
        finally
        {
            transport.Release();
            await StopAndDisposeNodesAsync([target], disposeDirectories: false);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CommittedBatchReleasesWaitersOnlyAfterTheApplicationTransaction(bool malformed)
    {
        using var directory = new TemporaryNodeDirectory();
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var transport = new GatedTransport();
        var proposals = new PendingProposalRegistry();
        NodeHarness target = CreateNode(1, new InMemoryNetwork(), directory: directory,
            transportOverride: transport, proposalRegistry: proposals);
        var command = new KvCommand(Guid.NewGuid(), KvCommandType.Set, "color", "blue");
        var last = new KvCommand(Guid.NewGuid(), KvCommandType.Set, "color", "green");
        using var pending = proposals.Register(command);
        using var lastPending = proposals.Register(last);
        try
        {
            await target.Host.StartAsync(guard.Token);
            await target.Host.GetStatusAsync(guard.Token);
            var append = new Message
            {
                Type = MessageType.MsgApp,
                From = 2,
                To = 1,
                Term = 2,
                Index = 3,
                LogTerm = 1,
                Commit = 5,
            };
            append.Entries.Add(new Entry
            {
                Index = 4,
                Term = 2,
                Data = ByteString.CopyFrom(DurableKvCommandCodec.Encode(command)),
            });
            append.Entries.Add(new Entry
            {
                Index = 5,
                Term = 2,
                Data = malformed ? ByteString.CopyFromUtf8("not JSON") : ByteString.CopyFrom(DurableKvCommandCodec.Encode(last)),
            });
            if (!malformed)
            {
                // Later spellings of the same request ID cannot replace its
                // winner, including when they are in the same transaction.
                string encoded = System.Text.Encoding.UTF8.GetString(DurableKvCommandCodec.Encode(command));
                append.Entries.Add(new Entry { Index = 6, Term = 2, Data = ByteString.CopyFromUtf8($" {encoded} ") });
                append.Entries.Add(new Entry { Index = 7, Term = 2 });
                append.Entries.Add(new Entry
                {
                    Index = 8,
                    Term = 2,
                    Data = ByteString.CopyFrom(DurableKvCommandCodec.Encode(command with { Value = "red" })),
                });
                append.Commit = 8;
            }

            await target.Host.ReceiveAsync(append, guard.Token);

            Message response = await transport.Entered.WaitAsync(guard.Token);
            Assert.Equal(MessageType.MsgAppResp, response.Type);
            Assert.Equal(append.Commit, target.Storage.GetHardState()?.Commit);
            Assert.Equal(3UL, target.State.PhysicalApplied);
            Assert.False(pending.Task.IsCompleted);
            Assert.False(lastPending.Task.IsCompleted);

            transport.Release();
            if (malformed)
            {
                await Assert.ThrowsAsync<InvalidDataException>(() => target.Host.ExecuteTask!.WaitAsync(guard.Token));
                Assert.False(pending.Task.IsCompleted);
                Assert.False(lastPending.Task.IsCompleted);
                Assert.Null(target.State.ResolveRequest(command));
                Assert.Equal(3UL, target.State.PhysicalApplied);
            }
            else
            {
                KvApplyResult first = await pending.Task.WaitAsync(guard.Token);
                KvApplyResult second = await lastPending.Task.WaitAsync(guard.Token);
                Assert.Equal(4UL, first.ResultIndex);
                Assert.Equal(5UL, second.ResultIndex);
                Assert.Equal(8UL, first.PhysicalApplied);
                Assert.Equal(8UL, second.PhysicalApplied);
                Assert.False(first.Duplicate);
                Assert.False(first.Conflict);
                Assert.Equal("green", target.State.ReadAtLeast("color", 8).Value);
            }
        }
        finally
        {
            transport.Release();
            await StopAndDisposeNodesAsync([target], disposeDirectories: false);
        }

        using var reopened = new SqliteKeyValueStateMachine(directory.ApplicationPath, nodeId: 1);
        Assert.Equal(malformed ? 3UL : 8UL, reopened.PhysicalApplied);
        Assert.Equal(malformed ? null : "green", reopened.ReadLocal("color").Value);
        Assert.Equal(malformed ? null : new KvRequestResolution(4, false), reopened.ResolveRequest(command));
        if (!malformed)
        {
            Assert.Equal(new KvRequestResolution(4, true), reopened.ResolveRequest(command with { Value = "red" }));
        }
    }

    [Fact]
    public async Task CommittedBatchesStopAtTheSizeLimitAndConfigurationEntries()
    {
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var proposals = new PendingProposalRegistry();
        var target = CreateNode(1, new InMemoryNetwork(), proposalRegistry: proposals);
        var first = new KvCommand(Guid.NewGuid(), KvCommandType.Set, "cap", "one");
        var beforeConfiguration = new KvCommand(Guid.NewGuid(), KvCommandType.Set, "config", "before");
        var afterConfiguration = new KvCommand(Guid.NewGuid(), KvCommandType.Set, "config", "after");
        using var firstWaiter = proposals.Register(first);
        using var beforeWaiter = proposals.Register(beforeConfiguration);
        using var afterWaiter = proposals.Register(afterConfiguration);

        try
        {
            await target.Host.StartAsync(guard.Token);
            await target.Host.GetStatusAsync(guard.Token);
            var append = new Message
            {
                Type = MessageType.MsgApp,
                From = 2,
                To = 1,
                Term = 2,
                Index = 3,
                LogTerm = 1,
                Commit = 71,
            };
            for (ulong index = 4; index <= append.Commit; index++)
            {
                KvCommand? command = index switch
                {
                    4 => first,
                    68 => beforeConfiguration,
                    70 => afterConfiguration,
                    _ => null,
                };
                append.Entries.Add(new Entry
                {
                    Index = index,
                    Term = 2,
                    Type = index == 69 ? EntryType.EntryConfChangeV2 : EntryType.EntryNormal,
                    Data = command is null ? ByteString.Empty : ByteString.CopyFrom(DurableKvCommandCodec.Encode(command)),
                });
            }

            await target.Host.ReceiveAsync(append, guard.Token);

            Assert.Equal(67UL, (await firstWaiter.Task.WaitAsync(guard.Token)).PhysicalApplied);
            Assert.Equal(68UL, (await beforeWaiter.Task.WaitAsync(guard.Token)).PhysicalApplied);
            Assert.Equal(71UL, (await afterWaiter.Task.WaitAsync(guard.Token)).PhysicalApplied);
            Assert.Equal("after", target.State.ReadAtLeast("config", 71).Value);
            Assert.Equal([1UL, 2UL, 3UL], target.State.ConfState.Voters);
        }
        finally
        {
            await StopAndDisposeNodesAsync([target], disposeDirectories: true);
        }
    }

    private static async Task StartAndElectAsync(InMemoryNetwork network, NodeHarness[] nodes)
    {
        await StartNodesAsync(network, nodes);
        await WaitForApplicationAsync(nodes, 3);
        await ElectAsync(nodes[0]);
    }

    private static async Task StartNodesAsync(InMemoryNetwork network, NodeHarness[] nodes)
    {
        foreach (NodeHarness node in nodes)
        {
            network.Register(node.Host);
        }

        using var timeout = new CancellationTokenSource(PhaseTimeout);
        await Task.WhenAll(nodes.Select(node => node.Host.StartAsync(timeout.Token))).WaitAsync(timeout.Token);
    }

    private static async Task ElectAsync(NodeHarness candidate)
    {
        using var timeout = new CancellationTokenSource(PhaseTimeout);
        await candidate.Host.CampaignAsync(timeout.Token).AsTask().WaitAsync(timeout.Token);
        await WaitForStatusAsync([candidate], $"node {candidate.Host.NodeId} to become leader",
            (_, status) => status.Role == RaftRole.Leader);
    }

    private static NodeHarness CreateNode(
        ulong id,
        InMemoryNetwork network,
        bool automaticTicks = false,
        TemporaryNodeDirectory? directory = null,
        int maxTransportMessageBytes =
            64 * 1024 * 1024,
        int snapshotThresholdEntries = 10,
        IRaftMessageTransport? transportOverride = null,
        PendingProposalRegistry? proposalRegistry = null,
        PendingReadRegistry? readRegistry = null)
    {
        TemporaryNodeDirectory actualDirectory =
            directory
            ?? new TemporaryNodeDirectory();
        ClusterOptions configured =
            KvClusterComponentTests.Options(
                id,
                automaticTicks,
                dataDirectory:
                    actualDirectory.Path,
                maxTransportMessageBytes:
                    maxTransportMessageBytes,
                snapshotThresholdEntries:
                    snapshotThresholdEntries);
        ValidatedClusterOptions options =
            configured.Validate();
        var storage = new SqliteStorage(
            actualDirectory.RaftPath);
        var state =
            DurableHostRecovery.OpenApplication(
                storage,
                actualDirectory.ApplicationPath,
                id);
        var proposals =
            proposalRegistry ?? new PendingProposalRegistry();
        var reads = readRegistry ?? new PendingReadRegistry();
        var ticks = new ManualTickSource();
        IRaftMessageTransport transport =
            transportOverride ?? network.CreateTransport(id);
        var lifetime =
            new TestApplicationLifetime();
        var host = new RaftClusterHost(
            options,
            storage,
            state,
            proposals,
            reads,
            ticks,
            transport,
            lifetime,
            NullLogger<RaftClusterHost>.Instance);
        return new NodeHarness(
            host,
            state,
            transport,
            ticks,
            lifetime,
            storage,
            actualDirectory);
    }

    private static ulong SnapshotAttemptIndex(
        RaftClusterHost host)
    {
        return Assert.IsType<ulong>(
            typeof(RaftClusterHost)
                .GetField(
                    "lastSnapshotAttemptIndex",
                    System.Reflection.BindingFlags
                        .Instance
                    | System.Reflection.BindingFlags
                        .NonPublic)
                ?.GetValue(host));
    }

    private static Task WaitForApplicationAsync(NodeHarness[] nodes, ulong index) =>
        WaitForStatusAsync(nodes, $"replicas to apply through index {index}", (_, status) => status.PhysicalApplied >= index);

    private static async Task WaitForStatusAsync(
        NodeHarness[] nodes,
        string expectation,
        Func<NodeHarness, ClusterStatusResponse, bool> condition)
    {
        var last = new ConcurrentDictionary<ulong, ClusterStatusResponse>();
        using var timeout = new CancellationTokenSource(PhaseTimeout);
        try
        {
            while (true)
            {
                bool[] satisfied = await Task.WhenAll(nodes.Select(ObserveAsync)).WaitAsync(timeout.Token);
                if (satisfied.All(result => result))
                {
                    return;
                }

                await Task.Delay(10, timeout.Token);
            }
        }
        catch (OperationCanceledException exception) when (timeout.IsCancellationRequested)
        {
            string observations = string.Join(Environment.NewLine, nodes.Select(node =>
                last.TryGetValue(node.Host.NodeId, out ClusterStatusResponse? status)
                    ? status.ToString()
                    : $"Node {node.Host.NodeId}: no status response (host task: {node.Host.ExecuteTask?.Status})"));
            throw new TimeoutException($"Timed out waiting for {expectation}. Last observations:\n{observations}", exception);
        }

        async Task<bool> ObserveAsync(NodeHarness node)
        {
            ClusterStatusResponse status = await node.Host.GetStatusAsync(timeout.Token);
            last[node.Host.NodeId] = status;
            return condition(node, status);
        }
    }

    private static async Task DisposeNodeAsync(
        NodeHarness node,
        bool disposeDirectory)
    {
        node.Host.Dispose();
        await node.Transport.DisposeAsync();
        await node.TickSource.DisposeAsync();
        node.State.Dispose();
        node.Storage.Dispose();
        node.Lifetime.Dispose();
        if (disposeDirectory)
        {
            node.Directory.Dispose();
        }
    }

    private static async Task StopAndDisposeNodesAsync(
        IEnumerable<NodeHarness> nodes,
        bool disposeDirectories)
    {
        var failures = new List<Exception>();
        foreach (NodeHarness node in nodes.Reverse())
        {
            using var timeout = new CancellationTokenSource(PhaseTimeout);
            try
            {
                await node.Host.StopAsync(timeout.Token).WaitAsync(timeout.Token);
                if (node.Host.ExecuteTask is { IsCompleted: false })
                {
                    throw new TimeoutException("The host returned from StopAsync before its loop stopped.");
                }
            }
            catch (Exception exception)
            {
                failures.Add(new InvalidOperationException($"Failed to stop node {node.Host.NodeId} ({node.Directory.Path}).", exception));
            }

            if (node.Host.ExecuteTask is { IsCompleted: false })
            {
                // A failed guard does not give the test ownership of live databases.
                node.Directory.Preserve = true;
                continue;
            }

            try
            {
                await DisposeNodeAsync(node, disposeDirectories);
            }
            catch (Exception exception)
            {
                failures.Add(new InvalidOperationException($"Failed to dispose node {node.Host.NodeId} ({node.Directory.Path}).", exception));
            }
        }

        if (failures.Count > 0)
        {
            throw new AggregateException("KV test cleanup failed.", failures);
        }
    }

    private sealed record NodeHarness(
    RaftClusterHost Host,
    SqliteKeyValueStateMachine State,
    IRaftMessageTransport Transport,
    ManualTickSource TickSource,
    TestApplicationLifetime Lifetime,
    SqliteStorage Storage,
    TemporaryNodeDirectory Directory);

    private sealed class TemporaryNodeDirectory
        : IDisposable
    {
        internal TemporaryNodeDirectory()
        {
            Path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "dotnet-raft-kv-cluster-tests",
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Path);
        }

        internal string Path { get; }
        internal bool Preserve { get; set; }

        internal string RaftPath =>
            System.IO.Path.Combine(
                Path,
                "raft.db");

        internal string ApplicationPath =>
            System.IO.Path.Combine(
                Path,
                "application.db");

        public void Dispose()
        {
            if (!Preserve && Directory.Exists(Path))
            {
                Directory.Delete(
                    Path,
                    recursive: true);
            }
        }
    }

    private sealed class InMemoryNetwork
    {
        private readonly ConcurrentDictionary<
            ulong,
            IRaftMessageReceiver> receivers = [];

        internal InMemoryTransport CreateTransport(
            ulong senderId)
        {
            return new InMemoryTransport(
                senderId,
                receivers);
        }

        internal void Register(
            RaftClusterHost receiver)
        {
            Assert.True(
                receivers.TryAdd(
                    receiver.NodeId,
                    receiver));
        }
    }

    private sealed class InMemoryTransport
        : IRaftMessageTransport
    {
        private readonly ConcurrentDictionary<
            ulong,
            IRaftMessageReceiver> receivers;
        private readonly ulong senderId;

        internal InMemoryTransport(
            ulong senderId,
            ConcurrentDictionary<
                ulong,
                IRaftMessageReceiver> receivers)
        {
            this.senderId = senderId;
            this.receivers = receivers;
        }

        public async ValueTask<RaftSendResult> SendAsync(
            Message message,
            CancellationToken cancellationToken)
        {
            Assert.Equal(senderId, message.From);
            if (!receivers.TryGetValue(
                    message.To,
                    out IRaftMessageReceiver? receiver))
            {
                return RaftSendResult.Unavailable;
            }

            await receiver.ReceiveAsync(
                    message.Clone(),
                    cancellationToken)
                .ConfigureAwait(false);
            return RaftSendResult.Delivered;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class GatedTransport : IRaftMessageTransport
    {
        private readonly TaskCompletionSource<Message> entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task<Message> Entered => entered.Task;

        internal void Release() => released.TrySetResult();

        public async ValueTask<RaftSendResult> SendAsync(Message message, CancellationToken cancellationToken)
        {
            entered.TrySetResult(message.Clone());
            await released.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return RaftSendResult.Delivered;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    // Used by the follower application-barrier schedule. Release tokens apply
    // only to heartbeat replies, not the forwarded request or append reply.
    private sealed class ScriptedTransport : IRaftMessageTransport
    {
        private readonly Channel<Message> sent = Channel.CreateUnbounded<Message>();
        private readonly Channel<bool> releases = Channel.CreateUnbounded<bool>();

        internal ValueTask<Message> NextAsync(CancellationToken cancellationToken) => sent.Reader.ReadAsync(cancellationToken);

        internal void ReleaseHeartbeat() => releases.Writer.TryWrite(true);

        public async ValueTask<RaftSendResult> SendAsync(Message message, CancellationToken cancellationToken)
        {
            await sent.Writer.WriteAsync(message.Clone(), cancellationToken).ConfigureAwait(false);
            if (message.Type == MessageType.MsgHeartbeatResp)
            {
                await releases.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            }

            return RaftSendResult.Delivered;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FaultingTransport
        : IRaftMessageTransport
    {
        private readonly TaskCompletionSource called =
            new(
                TaskCreationOptions
                    .RunContinuationsAsynchronously);

        internal Task Called => called.Task;

        public ValueTask<RaftSendResult> SendAsync(
            Message message,
            CancellationToken cancellationToken)
        {
            called.TrySetResult();
            return ValueTask.FromException<
                RaftSendResult>(
                new InvalidOperationException(
                    "transport fault"));
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ManualTickSource
        : IRaftTickSource
    {
        private readonly Channel<bool> ticks =
            Channel.CreateUnbounded<bool>();

        internal void Tick()
        {
            Assert.True(
                ticks.Writer.TryWrite(true));
        }

        public ValueTask<bool> WaitForNextTickAsync(
            CancellationToken cancellationToken)
        {
            return ticks.Reader.ReadAsync(
                cancellationToken);
        }

        public ValueTask DisposeAsync()
        {
            ticks.Writer.TryComplete();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestApplicationLifetime
        : IHostApplicationLifetime,
          IDisposable
    {
        private readonly CancellationTokenSource
            started = new();
        private readonly CancellationTokenSource
            stopped = new();
        private readonly CancellationTokenSource
            stopping = new();

        internal TestApplicationLifetime()
        {
            started.Cancel();
        }

        public CancellationToken ApplicationStarted =>
            started.Token;

        public CancellationToken ApplicationStopping =>
            stopping.Token;

        public CancellationToken ApplicationStopped =>
            stopped.Token;

        public void StopApplication()
        {
            stopping.Cancel();
            stopped.Cancel();
        }

        public void Dispose()
        {
            started.Dispose();
            stopping.Dispose();
            stopped.Dispose();
        }
    }
}
