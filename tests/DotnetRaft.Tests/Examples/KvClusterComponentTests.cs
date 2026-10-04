using DotnetRaft.Examples.KvCluster;
using DotnetRaft.Protocol;

using Google.Protobuf;

using Microsoft.Extensions.Logging.Abstractions;

using ProtocolConfChange =
    DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft.Tests.Examples;

public sealed class KvClusterComponentTests
{
    [Fact]
    public void OptionsValidateThreeLoopbackPeers()
    {
        ValidatedClusterOptions options =
            Options(nodeId: 2).Validate();

        Assert.Equal(2UL, options.NodeId);
        Assert.Equal(3, options.Peers.Count);
        Assert.Equal(7202, options.GrpcPort);
        Assert.False(options.AutomaticTicks);
        Assert.True(
            Path.IsPathFullyQualified(
                options.DataDirectory));
        Assert.Equal(
            10,
            options.SnapshotThresholdEntries);
        Assert.Equal(
            64 * 1024 * 1024,
            options.MaxTransportMessageBytes);
    }

    [Fact]
    public void OptionsRejectUnsafeOrIncompleteCluster()
    {
        ClusterOptions missingId =
            Options(nodeId: 0);
        Assert.Throws<InvalidOperationException>(
            missingId.Validate);

        ClusterOptions missingPeer =
            Options(nodeId: 1);
        missingPeer.Peers.Remove("3");
        Assert.Throws<InvalidOperationException>(
            missingPeer.Validate);

        ClusterOptions remote =
            Options(nodeId: 1);
        remote.Peers["2"] =
            "http://example.com:7202";
        Assert.Throws<InvalidOperationException>(
            remote.Validate);

        ClusterOptions tinyTransport =
            Options(
                nodeId: 1,
                maxTransportMessageBytes:
                    64 * 1024);
        Assert.Throws<InvalidOperationException>(
            tinyTransport.Validate);
    }

    [Fact]
    public async Task PendingRegistryCompletesAndRemovesRequests()
    {
        var pending = new PendingReadRegistry();
        Guid requestId = Guid.NewGuid();
        Task<ulong> completion =
            pending.Register(requestId);

        Assert.True(
            pending.Complete(requestId, 7));
        Assert.Equal(7UL, await completion);
        Assert.False(
            pending.Complete(requestId, 8));

        Guid removed = Guid.NewGuid();
        _ = pending.Register(removed);
        Assert.True(pending.Remove(removed));
    }

    [Fact]
    public async Task PendingReadRegistryDefersUntilApplied()
    {
        var pending = new PendingReadRegistry();
        Guid requestId = Guid.NewGuid();
        Task<ulong> completion =
            pending.Register(requestId);

        pending.CompleteOrDefer(
            requestId,
            index: 9,
            physicalApplied: 7);
        Assert.False(completion.IsCompleted);
        pending.CompleteThrough(8);
        Assert.False(completion.IsCompleted);
        pending.CompleteThrough(9);
        Assert.Equal(9UL, await completion);

        Guid removed = Guid.NewGuid();
        _ = pending.Register(removed);
        pending.CompleteOrDefer(
            removed,
            index: 12,
            physicalApplied: 10);
        Assert.True(pending.Remove(removed));
        pending.CompleteThrough(12);
        Assert.False(
            pending.Complete(
                removed,
                12));
    }

    [Fact]
    public void NetworkValidatorRejectsLocalAndMisdirectedMessages()
    {
        ValidatedClusterOptions options =
            Options(nodeId: 1).Validate();
        var valid = new Message
        {
            From = 2,
            To = 1,
            Term = 1,
            Type = MessageType.MsgHeartbeat,
        };

        NetworkMessageValidator.Validate(
            valid,
            options);

        Message forget = valid.Clone();
        forget.Type =
            MessageType.MsgForgetLeader;
        NetworkMessageValidator.Validate(
            forget,
            options);

        Message local = valid.Clone();
        local.Type = MessageType.MsgHup;
        Assert.Throws<ArgumentException>(
            () => NetworkMessageValidator.Validate(
                local,
                options));

        Message wrongTarget = valid.Clone();
        wrongTarget.To = 3;
        Assert.Throws<ArgumentException>(
            () => NetworkMessageValidator.Validate(
                wrongTarget,
                options));

        Assert.Throws<ArgumentException>(
            () => NetworkMessageValidator.Validate(
                new Message(),
                options));
    }

    [Fact]
    public async Task TransportRejectsOversizeMessageWithoutFault()
    {
        ValidatedClusterOptions options =
            Options(
                    nodeId: 1,
                    maxTransportMessageBytes:
                        256 * 1024)
                .Validate();
        var transport =
            new GrpcRaftMessageTransport(
                options,
                NullLogger<
                    GrpcRaftMessageTransport>
                    .Instance);
        var message = new Message
        {
            From = 1,
            To = 2,
            Type = MessageType.MsgApp,
        };
        message.Entries.Add(
            new Entry
            {
                Data = ByteString.CopyFrom(
                    new byte[300 * 1024]),
            });

        Assert.Equal(
            RaftSendResult.Unavailable,
            await transport.SendAsync(
                message,
                CancellationToken.None));
        await transport.DisposeAsync();
    }

    [Fact]
    public void FixedMembershipRequiresExactBootstrapPrefix()
    {
        var membership =
            new FixedMembershipConfiguration(
                [1UL, 2UL, 3UL]);
        var change = new ProtocolConfChange
        {
            Type =
                ConfChangeType.ConfChangeAddNode,
            NodeId = 1,
        };
        var valid = new Entry
        {
            Index = 1,
            Term = 1,
            Type = EntryType.EntryConfChange,
            Data = change.ToByteString(),
        };

        Assert.Equal(
            change,
            membership.ValidateBootstrapEntry(
                valid));
        Assert.False(
            membership.IsBootstrapIndex(4));

        Entry normal = valid.Clone();
        normal.Type = EntryType.EntryNormal;
        Assert.Throws<InvalidDataException>(
            () => membership
                .ValidateBootstrapEntry(normal));
    }

    internal static ClusterOptions Options(
        ulong nodeId,
        bool automaticTicks = false,
        string? dataDirectory = null,
        int maxTransportMessageBytes =
            64 * 1024 * 1024,
        int snapshotThresholdEntries = 10)
    {
        return new ClusterOptions
        {
            NodeId = nodeId,
            HttpPort =
                7100 + checked((int)nodeId),
            GrpcPort =
                7200 + checked((int)nodeId),
            AutomaticTicks = automaticTicks,
            TickIntervalMilliseconds = 10,
            RequestTimeoutSeconds = 5,
            TransportTimeoutMilliseconds = 100,
            DataDirectory = dataDirectory,
            SnapshotThresholdEntries =
                snapshotThresholdEntries,
            MaxTransportMessageBytes =
                maxTransportMessageBytes,
            Peers = new Dictionary<string, string>
            {
                ["1"] =
                    "http://127.0.0.1:7201",
                ["2"] =
                    "http://127.0.0.1:7202",
                ["3"] =
                    "http://127.0.0.1:7203",
            },
        };
    }
}
