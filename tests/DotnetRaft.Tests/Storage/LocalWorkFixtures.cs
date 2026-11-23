using DotnetRaft.Core;
using DotnetRaft.Protocol;

namespace DotnetRaft.Tests;

// Historic schedules and malformed-frame tests can still express the
// diagnostic frames; hosts and the main worker tests use typed work instead.
internal static class LocalWorkFixtures
{
    internal static IReadOnlyList<Message> FixtureMessages(this Ready ready) => ready.DescribeMessages().ToArray();

    internal static void StepFixture(this DotnetRaft.RawNode node, Message message)
    {
        if (message is not null && IsStorageFrame(message)) node.StepFixtureStorageResponse(message);
        else node.Step(message!);
    }

    internal static ValueTask StepFixtureAsync(this RaftNode node, Message message, CancellationToken cancellationToken = default) =>
        message is not null && IsStorageFrame(message)
            ? node.StepFixtureStorageResponseAsync(message, cancellationToken)
            : node.StepAsync(message!, cancellationToken);

    private static bool IsStorageFrame(Message message) =>
        MessageClassifier.IsStorageResponse(message.Type) || RaftLocalMessageTargets.IsLocal(message.From);
}
