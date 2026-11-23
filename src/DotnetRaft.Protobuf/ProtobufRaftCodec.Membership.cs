using DotnetRaft.Protocol;

using DomainConfChange = DotnetRaft.Protocol.ConfChange;
using Wire = DotnetRaft.Protobuf.Wire;

namespace DotnetRaft.Protobuf;

public sealed partial class ProtobufRaftCodec
{
    private static DomainConfChange FromWire(Wire.ConfChange source)
    {
        var change = new DomainConfChange { ExtensionData = CaptureExtensions(source) };
        if (source.HasId) change.Id = source.Id;
        if (source.HasNodeId) change.NodeId = source.NodeId;
        if (source.HasType) change.Type = (ConfChangeType)source.Type;
        if (source.HasContext) change.Context = FromWire(source.Context);
        return change;
    }

    private static Wire.ConfChange ToWire(DomainConfChange change)
    {
        Wire.ConfChange wire = CreateWire(Wire.ConfChange.Parser, change.ExtensionData);
        if (change.HasId) wire.Id = change.Id;
        if (change.HasNodeId) wire.NodeId = change.NodeId;
        if (change.HasType) wire.Type = (Wire.ConfChangeType)change.Type;
        if (change.HasContext) wire.Context = ToWire(change.Context);
        return wire;
    }

    private static ConfChangeV2 FromWire(Wire.ConfChangeV2 source)
    {
        var change = new ConfChangeV2 { ExtensionData = CaptureExtensions(source) };
        if (source.HasTransition) change.Transition = (ConfChangeTransition)source.Transition;
        if (source.HasContext) change.Context = FromWire(source.Context);
        change.Changes.AddRange(source.Changes.Select(FromWire));
        return change;
    }

    private static Wire.ConfChangeV2 ToWire(ConfChangeV2 change)
    {
        Wire.ConfChangeV2 wire = CreateWire(Wire.ConfChangeV2.Parser, change.ExtensionData);
        if (change.HasTransition) wire.Transition = (Wire.ConfChangeTransition)change.Transition;
        if (change.HasContext) wire.Context = ToWire(change.Context);
        wire.Changes.AddRange(change.Changes.Select(ToWire));
        return wire;
    }

    private static ConfChangeSingle FromWire(Wire.ConfChangeSingle source)
    {
        var change = new ConfChangeSingle { ExtensionData = CaptureExtensions(source) };
        if (source.HasType) change.Type = (ConfChangeType)source.Type;
        if (source.HasNodeId) change.NodeId = source.NodeId;
        return change;
    }

    private static Wire.ConfChangeSingle ToWire(ConfChangeSingle change)
    {
        Wire.ConfChangeSingle wire = CreateWire(Wire.ConfChangeSingle.Parser, change.ExtensionData);
        if (change.HasType) wire.Type = (Wire.ConfChangeType)change.Type;
        if (change.HasNodeId) wire.NodeId = change.NodeId;
        return wire;
    }
}
