using DotnetRaft.Protocol;

using Wire = DotnetRaft.Protobuf.Wire;

namespace DotnetRaft.Protobuf;

public sealed partial class ProtobufRaftCodec
{
    private static Snapshot FromWire(Wire.Snapshot source)
    {
        var snapshot = new Snapshot { ExtensionData = CaptureExtensions(source) };
        if (source.HasData) snapshot.Data = FromWire(source.Data);
        if (source.Metadata is { } metadata) snapshot.Metadata = FromWire(metadata);
        return snapshot;
    }

    private static Wire.Snapshot ToWire(Snapshot snapshot)
    {
        Wire.Snapshot wire = CreateWire(Wire.Snapshot.Parser, snapshot.ExtensionData);
        if (snapshot.HasData) wire.Data = ToWire(snapshot.Data);
        if (snapshot.Metadata is { } metadata) wire.Metadata = ToWire(metadata);
        return wire;
    }

    private static SnapshotMetadata FromWire(Wire.SnapshotMetadata source)
    {
        var metadata = new SnapshotMetadata { ExtensionData = CaptureExtensions(source) };
        if (source.HasTerm) metadata.Term = source.Term;
        if (source.HasIndex) metadata.Index = source.Index;
        if (source.ConfState is { } state) metadata.ConfState = FromWire(state);
        return metadata;
    }

    private static Wire.SnapshotMetadata ToWire(SnapshotMetadata metadata)
    {
        Wire.SnapshotMetadata wire = CreateWire(Wire.SnapshotMetadata.Parser, metadata.ExtensionData);
        if (metadata.HasTerm) wire.Term = metadata.Term;
        if (metadata.HasIndex) wire.Index = metadata.Index;
        if (metadata.ConfState is { } state) wire.ConfState = ToWire(state);
        return wire;
    }

    private static ConfState FromWire(Wire.ConfState source)
    {
        var state = new ConfState { ExtensionData = CaptureExtensions(source) };
        state.Voters.AddRange(source.Voters);
        state.Learners.AddRange(source.Learners);
        state.VotersOutgoing.AddRange(source.VotersOutgoing);
        state.LearnersNext.AddRange(source.LearnersNext);
        if (source.HasAutoLeave) state.AutoLeave = source.AutoLeave;
        return state;
    }

    private static Wire.ConfState ToWire(ConfState state)
    {
        Wire.ConfState wire = CreateWire(Wire.ConfState.Parser, state.ExtensionData);
        wire.Voters.AddRange(state.Voters);
        wire.Learners.AddRange(state.Learners);
        wire.VotersOutgoing.AddRange(state.VotersOutgoing);
        wire.LearnersNext.AddRange(state.LearnersNext);
        if (state.HasAutoLeave) wire.AutoLeave = state.AutoLeave;
        return wire;
    }
}
