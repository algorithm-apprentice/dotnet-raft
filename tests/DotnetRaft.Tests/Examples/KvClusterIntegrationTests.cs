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

            await WaitUntilAsync(
                () => nodes.All(node =>
                {
                    KeyValueReadResult result =
                        node.State.ReadLocal(
                            "color");
                    return result.Found
                        && result.Value == "blue";
                }));

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

            await nodes[1].Host.DeleteAsync(
                "color",
                requestId: null,
                CancellationToken.None);
            await WaitUntilAsync(
                () => nodes.All(node =>
                    !node.State.ReadLocal(
                        "color").Found));
        }
        finally
        {
            foreach (NodeHarness node in
                     nodes.Reverse())
            {
                await node.Host.StopAsync(
                    CancellationToken.None);
                await DisposeNodeAsync(node);
            }
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
                foreach (NodeHarness node in nodes)
                {
                    network.Register(node.Host);
                    await node.Host.StartAsync(
                        CancellationToken.None);
                }

                try
                {
                    await WaitUntilAsync(
                        () => nodes.All(node =>
                            node.State
                                .PhysicalApplied
                            >= 3));
                    await nodes[0].Host
                        .CampaignAsync(
                            CancellationToken.None);
                    await WaitUntilAsync(async () =>
                        (await nodes[0].Host
                            .GetStatusAsync(
                                CancellationToken.None))
                        .Role == RaftRole.Leader);
                    original =
                        await nodes[1].Host.PutAsync(
                            "color",
                            "blue",
                            requestId,
                            CancellationToken.None);
                    await WaitUntilAsync(
                        () => nodes.All(node =>
                            node.State.ReadLocal(
                                "color").Value
                            == "blue"));
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
                foreach (NodeHarness node in nodes)
                {
                    network.Register(node.Host);
                    await node.Host.StartAsync(
                        CancellationToken.None);
                }

                try
                {
                    await WaitUntilAsync(
                        () => nodes.All(node =>
                            node.State.ReadLocal(
                                "color").Value
                            == "blue"));
                    await nodes[0].Host
                        .CampaignAsync(
                            CancellationToken.None);
                    await WaitUntilAsync(async () =>
                        (await nodes[0].Host
                            .GetStatusAsync(
                                CancellationToken.None))
                        .Role == RaftRole.Leader);
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
        try
        {
            await StartAndElectAsync(network, nodes);

            Guid requestId = Guid.Parse(
                "11111111-2222-3333-4444-555555555555");
            Task<ProposalResponse> first =
                nodes[1].Host.PutAsync(
                    "first",
                    "one",
                    requestId,
                    CancellationToken.None);
            Task<ProposalResponse> second =
                nodes[2].Host.PutAsync(
                    "second",
                    "two",
                    requestId,
                    CancellationToken.None);
            var successes = 0;
            var conflicts = 0;
            foreach (Task<ProposalResponse> task in
                     new[] { first, second })
            {
                try
                {
                    _ = await task;
                    successes++;
                }
                catch (KvRequestConflictException)
                {
                    conflicts++;
                }
            }

            Assert.Equal(1, successes);
            Assert.Equal(1, conflicts);
            await WaitUntilAsync(
                () => nodes.All(node =>
                {
                    bool firstFound =
                        node.State.ReadLocal(
                            "first").Found;
                    bool secondFound =
                        node.State.ReadLocal(
                            "second").Found;
                    return firstFound ^ secondFound;
                }));
        }
        finally
        {
            foreach (NodeHarness node in
                     nodes.Reverse())
            {
                await node.Host.StopAsync(
                    CancellationToken.None);
                await DisposeNodeAsync(node);
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
        foreach (NodeHarness node in nodes)
        {
            network.Register(node.Host);
        }

        try
        {
            foreach (NodeHarness node in nodes)
            {
                await node.Host.StartAsync(
                    CancellationToken.None);
            }

            await WaitUntilAsync(
                () => nodes.All(node =>
                    node.State.PhysicalApplied >= 3));
            await nodes[0].Host.CampaignAsync(
                CancellationToken.None);
            await WaitUntilAsync(async () =>
                (await nodes[0].Host.GetStatusAsync(
                    CancellationToken.None)).Role
                == RaftRole.Leader);

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

            await WaitUntilAsync(
                () => nodes.All(node =>
                    SnapshotAttemptIndex(
                        node.Host) >= 6));
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
            await WaitUntilAsync(async () =>
            {
                foreach (NodeHarness node in nodes)
                {
                    ClusterStatusResponse status =
                        await node.Host
                            .GetStatusAsync(
                                CancellationToken.None);
                    if (status.LogicalApplied < 7)
                    {
                        return false;
                    }
                }

                return true;
            });

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
            await WaitUntilAsync(
                () => node.State.PhysicalApplied >= 3);

            for (var tick = 0; tick < 20; tick++)
            {
                node.TickSource.Tick();
            }

            await WaitUntilAsync(async () =>
                (await node.Host.GetStatusAsync(
                    CancellationToken.None)).Role
                == RaftRole.PreCandidate);
        }
        finally
        {
            await node.Host.StopAsync(
                CancellationToken.None);
            await DisposeNodeAsync(node);
        }
    }

    [Fact]
    public async Task ReadyLoopFaultStopsApplicationAndNode()
    {
        using var directory =
            new TemporaryNodeDirectory();
        ValidatedClusterOptions options =
            KvClusterComponentTests.Options(
                    nodeId: 1,
                    dataDirectory:
                        directory.Path)
                .Validate();
        var lifetime =
            new TestApplicationLifetime();
        var tickSource = new ManualTickSource();
        var transport = new FaultingTransport();
        var storage = new SqliteStorage(
            directory.RaftPath);
        var state =
            new SqliteKeyValueStateMachine(
                directory.ApplicationPath,
                nodeId: 1);
        var host = new RaftClusterHost(
            options,
            storage,
            state,
            new PendingProposalRegistry(),
            new PendingReadRegistry(),
            tickSource,
            transport,
            lifetime,
            NullLogger<RaftClusterHost>.Instance);

        try
        {
            await host.StartAsync(
                CancellationToken.None);
            await WaitUntilAsync(
                () => state.PhysicalApplied >= 3);
            await host.CampaignAsync(
                CancellationToken.None);
            await transport.Called.WaitAsync(
                TimeSpan.FromSeconds(5));
            await WaitUntilAsync(() =>
                host.ExecuteTask?.IsCompleted
                == true);

            Assert.True(
                lifetime.ApplicationStopping
                    .IsCancellationRequested);
            await Assert.ThrowsAsync<
                InvalidOperationException>(
                async () => await host.ExecuteTask!);
            await Assert.ThrowsAnyAsync<Exception>(
                async () => await host.GetStatusAsync(
                    CancellationToken.None));
        }
        finally
        {
            try
            {
                await host.StopAsync(
                    CancellationToken.None);
            }
            catch (InvalidOperationException)
            {
            }

            await tickSource.DisposeAsync();
            lifetime.Dispose();
            state.Dispose();
            storage.Dispose();
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
        foreach (NodeHarness node in nodes)
        {
            network.Register(node.Host);
        }

        foreach (NodeHarness node in nodes)
        {
            await node.Host.StartAsync(CancellationToken.None);
        }

        await WaitUntilAsync(() => nodes.All(node => node.State.PhysicalApplied >= 3));
        await nodes[0].Host.CampaignAsync(CancellationToken.None);
        await WaitUntilAsync(async () =>
            (await nodes[0].Host.GetStatusAsync(CancellationToken.None)).Role == RaftRole.Leader);
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
        PendingProposalRegistry? proposalRegistry = null)
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
        var reads = new PendingReadRegistry();
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

    private static async Task WaitUntilAsync(
        Func<bool> condition)
    {
        using var timeout =
            new CancellationTokenSource(
                TimeSpan.FromSeconds(5));
        while (!condition())
        {
            await Task.Delay(
                10,
                timeout.Token);
        }
    }

    private static async Task WaitUntilAsync(
        Func<Task<bool>> condition)
    {
        using var timeout =
            new CancellationTokenSource(
                TimeSpan.FromSeconds(5));
        while (!await condition())
        {
            await Task.Delay(
                10,
                timeout.Token);
        }
    }

    private static async Task DisposeNodeAsync(
        NodeHarness node,
        bool disposeDirectory = true)
    {
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
        foreach (NodeHarness node in nodes.Reverse())
        {
            try
            {
                await node.Host.StopAsync(
                    CancellationToken.None);
            }
            finally
            {
                await DisposeNodeAsync(
                    node,
                    disposeDirectories);
            }
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
            if (Directory.Exists(Path))
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
