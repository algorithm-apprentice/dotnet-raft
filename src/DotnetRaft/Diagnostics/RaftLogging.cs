namespace DotnetRaft.Diagnostics;

internal sealed class RaftLoggingException(
    RaftLogLevel level,
    Exception innerException)
    : Exception(
        $"Raft logger failed while writing {level}.",
        innerException);

internal static class RaftLogging
{
    internal static void Write(
        IRaftLogger logger,
        RaftLogLevel level,
        string message)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(message);
        try
        {
            if (logger.IsEnabled(level))
            {
                logger.Log(level, message);
            }
        }
        catch (Exception exception)
        {
            throw new RaftLoggingException(
                level,
                exception);
        }
    }
}
