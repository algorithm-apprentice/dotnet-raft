namespace DotnetRaft.Examples.KvCluster;

public sealed class LocalPortEndpointFilter(
    int expectedPort) : IEndpointFilter
{
    public ValueTask<object?> InvokeAsync(
        EndpointFilterInvocationContext context,
        EndpointFilterDelegate next)
    {
        if (context.HttpContext
                .Connection.LocalPort
            != expectedPort)
        {
            return ValueTask.FromResult<object?>(
                Results.NotFound());
        }

        return next(context);
    }
}
