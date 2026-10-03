namespace DotnetRaft.Core;

internal sealed class RaftClock
{
    private readonly Func<int, int> randomOffset;

    internal RaftClock(
        int electionTick,
        int heartbeatTick,
        Func<int, int> randomOffset)
    {
        ArgumentNullException.ThrowIfNull(randomOffset);

        ElectionTick = electionTick;
        HeartbeatTick = heartbeatTick;
        this.randomOffset = randomOffset;
    }

    internal int ElectionTick { get; }

    internal int HeartbeatTick { get; }

    internal int ElectionElapsed { get; set; }

    internal int HeartbeatElapsed { get; set; }

    internal int RandomizedElectionTimeout { get; private set; }

    internal bool PastElectionTimeout =>
        ElectionElapsed >= RandomizedElectionTimeout;

    internal bool TickElection(bool promotable)
    {
        ElectionElapsed = IncrementElapsed(
            ElectionElapsed,
            "election");
        if (!promotable || !PastElectionTimeout)
        {
            return false;
        }

        ElectionElapsed = 0;
        return true;
    }

    internal LeaderClockTick TickLeader()
    {
        int nextElectionElapsed = IncrementElapsed(
            ElectionElapsed,
            "election");
        int nextHeartbeatElapsed = IncrementElapsed(
            HeartbeatElapsed,
            "heartbeat");

        bool electionDue =
            nextElectionElapsed >= ElectionTick;
        bool heartbeatDue =
            nextHeartbeatElapsed >= HeartbeatTick;
        ElectionElapsed =
            electionDue ? 0 : nextElectionElapsed;
        HeartbeatElapsed =
            heartbeatDue ? 0 : nextHeartbeatElapsed;
        return new LeaderClockTick(
            electionDue,
            heartbeatDue);
    }

    internal void Reset()
    {
        int randomizedTimeout =
            NextRandomizedElectionTimeout();

        ElectionElapsed = 0;
        HeartbeatElapsed = 0;
        RandomizedElectionTimeout = randomizedTimeout;
    }

    internal void SetRandomizedElectionTimeoutForTesting(
        int timeout)
    {
        if (timeout <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout),
                timeout,
                "Randomized election timeout must be positive.");
        }

        RandomizedElectionTimeout = timeout;
    }

    private static int IncrementElapsed(
        int elapsed,
        string clockName)
    {
        if (elapsed == int.MaxValue)
        {
            throw new RaftInvariantException(
                $"{clockName} elapsed counter overflowed.");
        }

        return elapsed + 1;
    }

    private int NextRandomizedElectionTimeout()
    {
        int offset = randomOffset(ElectionTick);
        if (offset < 0 || offset >= ElectionTick)
        {
            throw new RaftInvariantException(
                $"Random election offset {offset} is outside [0, {ElectionTick}).");
        }

        try
        {
            return checked(ElectionTick + offset);
        }
        catch (OverflowException exception)
        {
            throw new RaftInvariantException(
                $"Randomized election timeout overflowed: {exception.Message}");
        }
    }
}
