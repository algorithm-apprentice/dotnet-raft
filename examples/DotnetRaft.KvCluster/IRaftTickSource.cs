namespace DotnetRaft.Examples.KvCluster;

public interface IRaftTickSource : IAsyncDisposable
{
    ValueTask<bool> WaitForNextTickAsync(
        CancellationToken cancellationToken);
}
