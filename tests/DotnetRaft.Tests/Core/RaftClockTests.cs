using DotnetRaft.Core;

namespace DotnetRaft.Tests.Core;

public sealed class RaftClockTests
{
    [Fact]
    public void ElectionTickPreservesElapsedUntilPromotable()
    {
        var clock = new RaftClock(
            electionTick: 5,
            heartbeatTick: 2,
            _ => 1);
        clock.Reset();

        for (var tick = 0; tick < 6; tick++)
        {
            Assert.False(
                clock.TickElection(
                    promotable: false));
        }

        Assert.Equal(6, clock.ElectionElapsed);
        Assert.True(
            clock.TickElection(
                promotable: true));
        Assert.Equal(0, clock.ElectionElapsed);
    }

    [Fact]
    public void LeaderTickAdvancesAndResetsExactCadences()
    {
        var clock = new RaftClock(
            electionTick: 4,
            heartbeatTick: 2,
            _ => 0);
        clock.Reset();

        Assert.Equal(
            new LeaderClockTick(false, false),
            clock.TickLeader());
        Assert.Equal(
            new LeaderClockTick(false, true),
            clock.TickLeader());
        Assert.Equal(2, clock.ElectionElapsed);
        Assert.Equal(0, clock.HeartbeatElapsed);
        Assert.Equal(
            new LeaderClockTick(false, false),
            clock.TickLeader());
        Assert.Equal(
            new LeaderClockTick(true, true),
            clock.TickLeader());
        Assert.Equal(0, clock.ElectionElapsed);
        Assert.Equal(0, clock.HeartbeatElapsed);
    }

    [Fact]
    public void LeaderTickOverflowDoesNotPartiallyAdvance()
    {
        var clock = new RaftClock(
            electionTick: 4,
            heartbeatTick: 2,
            _ => 0);
        clock.Reset();
        clock.ElectionElapsed = 2;
        clock.HeartbeatElapsed = int.MaxValue;

        RaftInvariantException exception =
            Assert.Throws<RaftInvariantException>(
                () => clock.TickLeader());

        Assert.Equal(
            "heartbeat elapsed counter overflowed.",
            exception.Message);
        Assert.Equal(2, clock.ElectionElapsed);
        Assert.Equal(
            int.MaxValue,
            clock.HeartbeatElapsed);
    }

    [Fact]
    public void ElectionOverflowReportsClockName()
    {
        var clock = new RaftClock(
            electionTick: 4,
            heartbeatTick: 2,
            _ => 0);
        clock.Reset();
        clock.ElectionElapsed = int.MaxValue;

        RaftInvariantException election =
            Assert.Throws<RaftInvariantException>(
                () => clock.TickElection(
                    promotable: true));
        Assert.Equal(
            "election elapsed counter overflowed.",
            election.Message);

        RaftInvariantException leader =
            Assert.Throws<RaftInvariantException>(
                () => clock.TickLeader());
        Assert.Equal(
            "election elapsed counter overflowed.",
            leader.Message);
    }

    [Fact]
    public void InvalidRandomOffsetLeavesClockStateUntouched()
    {
        var offset = 0;
        var clock = new RaftClock(
            electionTick: 5,
            heartbeatTick: 2,
            _ => offset);
        clock.Reset();
        clock.ElectionElapsed = 3;
        clock.HeartbeatElapsed = 1;
        offset = 5;

        RaftInvariantException exception =
            Assert.Throws<RaftInvariantException>(
                clock.Reset);

        Assert.Equal(
            "Random election offset 5 is outside [0, 5).",
            exception.Message);
        Assert.Equal(3, clock.ElectionElapsed);
        Assert.Equal(1, clock.HeartbeatElapsed);
        Assert.Equal(
            5,
            clock.RandomizedElectionTimeout);
    }

    [Fact]
    public void DeterministicTimeoutHookRequiresPositiveValue()
    {
        var clock = new RaftClock(
            electionTick: 5,
            heartbeatTick: 2,
            _ => 0);
        clock.Reset();

        clock.SetRandomizedElectionTimeoutForTesting(9);
        Assert.Equal(
            9,
            clock.RandomizedElectionTimeout);

        ArgumentOutOfRangeException exception =
            Assert.Throws<ArgumentOutOfRangeException>(
                () => clock
                    .SetRandomizedElectionTimeoutForTesting(0));
        Assert.Equal("timeout", exception.ParamName);
        Assert.Contains(
            "Randomized election timeout must be positive.",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void RandomizedTimeoutOverflowFailsExplicitly()
    {
        var clock = new RaftClock(
            electionTick: int.MaxValue,
            heartbeatTick: 1,
            _ => 1);

        RaftInvariantException exception =
            Assert.Throws<RaftInvariantException>(
                clock.Reset);

        Assert.StartsWith(
            "Randomized election timeout overflowed:",
            exception.Message,
            StringComparison.Ordinal);
    }
}
