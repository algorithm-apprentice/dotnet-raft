using DotnetRaft.Examples.KvCluster;
using DotnetRaft.Protocol;

using Google.Protobuf;

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
    }

    [Fact]
    public void CommandCodecIsDeterministicAndStrict()
    {
        var command = new KvSetCommand(
            Guid.Parse(
                "11111111-2222-3333-4444-555555555555"),
            "color",
            "blue");

        byte[] first =
            KvCommandCodec.Encode(command);
        byte[] second =
            KvCommandCodec.Encode(command);

        Assert.Equal(first, second);
        Assert.Equal(
            command,
            KvCommandCodec.Decode(first));
        Assert.Throws<InvalidDataException>(
            () => KvCommandCodec.Decode(
                "{\"key\":\"missing-id\"}"u8));
    }

    [Fact]
    public void StateMachineAdvancesEveryIndexAndRestoresSnapshot()
    {
        var state = new KeyValueStateMachine();
        var command = new KvSetCommand(
            Guid.NewGuid(),
            "color",
            "blue");

        KvSetCommand applied = state.ApplySet(
            1,
            ByteString.CopyFrom(
                KvCommandCodec.Encode(command)));
        state.AdvanceNoOp(2);

        Assert.Equal(command, applied);
        KeyValueReadResult value =
            state.ReadAtLeast("color", 2);
        Assert.True(value.Found);
        Assert.Equal("blue", value.Value);
        Assert.Equal(2UL, value.PhysicalApplied);
        Assert.Throws<InvalidOperationException>(
            () => state.AdvanceNoOp(4));

        ByteString snapshot =
            state.CreateSnapshotData();
        var restored = new KeyValueStateMachine();
        restored.Restore(2, snapshot);

        KeyValueReadResult restoredValue =
            restored.ReadAtLeast("color", 2);
        Assert.Equal("blue", restoredValue.Value);
        Assert.Equal(2UL, restoredValue.PhysicalApplied);
    }

    [Fact]
    public async Task PendingRegistryCompletesAndRemovesRequests()
    {
        var pending = new PendingProposalRegistry();
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
        bool automaticTicks = false)
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
