namespace DotnetRaft.ConfChange;

internal sealed class ConfigurationChangeException(string message)
    : InvalidOperationException(message);
