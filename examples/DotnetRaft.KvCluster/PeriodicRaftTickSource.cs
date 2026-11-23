namespace DotnetRaft.Examples.KvCluster;

public sealed class PeriodicRaftTickSource
    : IRaftTickSource
{
    private readonly PeriodicTimer timer;

    public PeriodicRaftTickSource(
        ValidatedClusterOptions options)
    {
        timer = new PeriodicTimer(
            options.TickInterval);
    }

    public ValueTask<bool> WaitForNextTickAsync(
        CancellationToken cancellationToken)
    {
        return timer.WaitForNextTickAsync(
            cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        timer.Dispose();
        return ValueTask.CompletedTask;
    }
}
