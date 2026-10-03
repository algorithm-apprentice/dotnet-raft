using System.Collections.Concurrent;

namespace DotnetRaft.Examples.KvCluster;

public abstract class PendingOperationRegistry
{
    private readonly ConcurrentDictionary<
        Guid,
        TaskCompletionSource<ulong>> pending = [];

    public Task<ulong> Register(Guid requestId)
    {
        if (requestId == Guid.Empty)
        {
            throw new ArgumentException(
                "Request ID must be nonempty.",
                nameof(requestId));
        }

        var completion =
            new TaskCompletionSource<ulong>(
                TaskCreationOptions
                    .RunContinuationsAsynchronously);
        if (!pending.TryAdd(
                requestId,
                completion))
        {
            throw new InvalidOperationException(
                $"Request {requestId} is already pending.");
        }

        return completion.Task;
    }

    public bool Complete(
        Guid requestId,
        ulong index)
    {
        return pending.TryRemove(
                requestId,
                out TaskCompletionSource<ulong>?
                    completion)
            && completion.TrySetResult(index);
    }

    public bool Remove(Guid requestId)
    {
        return pending.TryRemove(
            requestId,
            out _);
    }
}

public sealed class PendingProposalRegistry
    : PendingOperationRegistry;

public sealed class PendingReadRegistry
    : PendingOperationRegistry;
