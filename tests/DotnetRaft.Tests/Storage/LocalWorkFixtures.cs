using DotnetRaft.Core;
using DotnetRaft.Protocol;

namespace DotnetRaft.Tests;

// Format tests can inspect diagnostic frames. Only explicitly injected
// storage frames use the fixture ingress; issued work keeps its receipt.
internal static class LocalWorkFixtures
{
    internal static IReadOnlyList<Message> FixtureMessages(this Ready ready) => ready.DescribeMessages().ToArray();

    internal static void StepFixture(this DotnetRaft.RawNode node, Message message)
    {
        if (message is not null && NetworkMessageValidation.IsStorageInput(message)) node.StepFixtureStorageResponse(message);
        else node.Step(message!);
    }

    internal static ValueTask StepFixtureAsync(this RaftNode node, Message message, CancellationToken cancellationToken = default) =>
        message is not null && NetworkMessageValidation.IsStorageInput(message)
            ? node.StepFixtureStorageResponseAsync(message, cancellationToken)
            : node.StepAsync(message!, cancellationToken);
}
