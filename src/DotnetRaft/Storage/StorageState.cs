using DotnetRaft.Protocol;

namespace DotnetRaft.Storage;

public sealed record StorageState(
    RaftHardState? HardState,
    ConfState ConfState);
