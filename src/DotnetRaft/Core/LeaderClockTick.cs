namespace DotnetRaft.Core;

internal readonly record struct LeaderClockTick(
    bool ElectionDue,
    bool HeartbeatDue);
