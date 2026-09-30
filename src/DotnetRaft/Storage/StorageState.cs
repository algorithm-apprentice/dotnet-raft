using DotnetRaft.Protocol;

namespace DotnetRaft.Storage;

public sealed record StorageState(
    HardState? HardState,
    ConfState ConfState);
