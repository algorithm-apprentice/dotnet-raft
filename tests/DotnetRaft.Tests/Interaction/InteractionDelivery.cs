using DotnetRaft.Protocol;

namespace DotnetRaft.Tests.Interaction;

// A local receipt participates in the same delivery schedule as peer messages.
// Its synthetic frame is only for the existing selectors and transcript.
internal sealed class InteractionDelivery
{
    internal InteractionDelivery(Message message)
    {
        Frame = message.Clone();
        To = Frame.To;
    }

    internal InteractionDelivery(ulong nodeId, StorageCompletion completion)
    {
        Frame = completion.DescribeReceipt();
        To = nodeId;
        Completion = completion;
    }

    internal Message Frame { get; }
    internal StorageCompletion? Completion { get; }
    internal ulong From => Frame.From;
    internal ulong To { get; }
    internal MessageType Type => Frame.Type;

    internal bool IsLocal => Completion is not null || From == To
        || RaftLocalMessageTargets.IsLocal(From) || RaftLocalMessageTargets.IsLocal(To);
}
