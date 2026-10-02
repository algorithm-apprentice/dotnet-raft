namespace DotnetRaft.Diagnostics;

public sealed class RaftTracingException : Exception
{
    public RaftTracingException(
        string message,
        Exception innerException)
        : base(message, innerException)
    {
    }
}
