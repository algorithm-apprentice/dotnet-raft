using DotnetRaft.Protocol;

using Wire = DotnetRaft.Protobuf.Wire;

namespace DotnetRaft.Protobuf;

public sealed partial class ProtobufRaftCodec
{
    private static Message FromWire(Wire.Message source)
    {
        var message = new Message { ExtensionData = CaptureExtensions(source) };
        if (source.HasType) message.Type = (MessageType)source.Type;
        if (source.HasTo) message.To = source.To;
        if (source.HasFrom) message.From = source.From;
        if (source.HasTerm) message.Term = source.Term;
        if (source.HasLogTerm) message.LogTerm = source.LogTerm;
        if (source.HasIndex) message.Index = source.Index;
        if (source.HasCommit) message.Commit = source.Commit;
        if (source.HasVote) message.Vote = source.Vote;
        if (source.HasReject) message.Reject = source.Reject;
        if (source.HasRejectHint) message.RejectHint = source.RejectHint;
        if (source.HasContext) message.Context = FromWire(source.Context);
        if (source.Snapshot is { } snapshot) message.Snapshot = FromWire(snapshot);
        message.Entries.AddRange(source.Entries.Select(FromWire));
        message.Responses.AddRange(source.Responses.Select(FromWire));
        return message;
    }

    private static Wire.Message ToWire(Message message)
    {
        Wire.Message wire = CreateWire(Wire.Message.Parser, message.ExtensionData);
        if (message.HasType) wire.Type = (Wire.MessageType)message.Type;
        if (message.HasTo) wire.To = message.To;
        if (message.HasFrom) wire.From = message.From;
        if (message.HasTerm) wire.Term = message.Term;
        if (message.HasLogTerm) wire.LogTerm = message.LogTerm;
        if (message.HasIndex) wire.Index = message.Index;
        if (message.HasCommit) wire.Commit = message.Commit;
        if (message.HasVote) wire.Vote = message.Vote;
        if (message.HasReject) wire.Reject = message.Reject;
        if (message.HasRejectHint) wire.RejectHint = message.RejectHint;
        if (message.HasContext) wire.Context = ToWire(message.Context);
        if (message.Snapshot is { } snapshot) wire.Snapshot = ToWire(snapshot);
        wire.Entries.AddRange(message.Entries.Select(ToWire));
        wire.Responses.AddRange(message.Responses.Select(ToWire));
        return wire;
    }
}
