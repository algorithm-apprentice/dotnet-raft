using DotnetRaft.Core;
using DotnetRaft.Diagnostics;
using DotnetRaft.Read;

namespace DotnetRaft.Tests.Core;

public sealed class RaftConfigTests
{
    [Fact]
    public void DefaultsAreNormalizedWithoutMutatingCallerConfig()
    {
        var storage = new CoreTestStorage();
        var config = new RaftConfig
        {
            Id = 1,
            Storage = storage,
        };

        var core = new RaftCore(config, _ => 0);

        Assert.Equal(ulong.MaxValue, core.MaxMessageSize);
        Assert.Equal(ulong.MaxValue, core.MaxCommittedSizePerReady);
        Assert.Equal(ulong.MaxValue, core.MaxUncommittedEntriesSize);
        Assert.Equal(256, core.Tracker.MaxInflightMessages);
        Assert.Equal(ulong.MaxValue, core.Tracker.MaxInflightBytes);
        Assert.Same(NullRaftLogger.Instance, core.Logger);

        Assert.Equal(ulong.MaxValue, config.MaxSizePerMessage);
        Assert.Equal(0UL, config.MaxCommittedSizePerReady);
        Assert.Equal(0UL, config.MaxUncommittedEntriesSize);
        Assert.Equal(0UL, config.MaxInflightBytes);
        Assert.Null(config.Logger);
    }

    [Theory]
    [InlineData(0UL)]
    [InlineData(ulong.MaxValue)]
    [InlineData(ulong.MaxValue - 1)]
    public void InvalidNodeIdsFailBeforeStorageAccess(ulong id)
    {
        var storage = new CoreTestStorage();
        var config = new RaftConfig
        {
            Id = id,
            Storage = storage,
        };

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new RaftCore(config, _ => 0));
        Assert.Equal(0, storage.CallCount);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(-1, 1)]
    [InlineData(1, 1)]
    [InlineData(2, 1)]
    public void InvalidTickRelationshipsFailBeforeStorageAccess(
        int heartbeatTick,
        int electionTick)
    {
        var storage = new CoreTestStorage();
        var config = new RaftConfig
        {
            Id = 1,
            Storage = storage,
            HeartbeatTick = heartbeatTick,
            ElectionTick = electionTick,
        };

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new RaftCore(config, _ => 0));
        Assert.Equal(0, storage.CallCount);
    }

    [Fact]
    public void ElectionTickMustLeaveRoomForRandomizedUpperBound()
    {
        const int maxSafe = (int.MaxValue / 2) + 1;
        var validStorage = new CoreTestStorage();
        var valid = new RaftCore(
            new RaftConfig
            {
                Id = 1,
                Storage = validStorage,
                ElectionTick = maxSafe,
                HeartbeatTick = 1,
            },
            maximum => maximum - 1);

        Assert.Equal(int.MaxValue, valid.RandomizedElectionTimeout);

        var invalidStorage = new CoreTestStorage();
        var invalid = new RaftConfig
        {
            Id = 1,
            Storage = invalidStorage,
            ElectionTick = maxSafe + 1,
            HeartbeatTick = 1,
        };

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new RaftCore(invalid, _ => 0));
        Assert.Equal(0, invalidStorage.CallCount);
    }

    [Fact]
    public void NullStorageFailsExplicitly()
    {
        var config = new RaftConfig
        {
            Id = 1,
            Storage = null,
        };

        Assert.Throws<ArgumentNullException>(
            () => new RaftCore(config, _ => 0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void NonpositiveInflightMessageLimitFailsBeforeStorageAccess(
        int maxInflightMessages)
    {
        var storage = new CoreTestStorage();
        var config = new RaftConfig
        {
            Id = 1,
            Storage = storage,
            MaxInflightMessages = maxInflightMessages,
        };

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new RaftCore(config, _ => 0));
        Assert.Equal(0, storage.CallCount);
    }

    [Fact]
    public void InflightByteLimitCannotBeBelowMessageLimit()
    {
        var storage = new CoreTestStorage();
        var config = new RaftConfig
        {
            Id = 1,
            Storage = storage,
            MaxSizePerMessage = 1024,
            MaxInflightBytes = 1023,
        };

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new RaftCore(config, _ => 0));
        Assert.Equal(0, storage.CallCount);
    }

    [Fact]
    public void LeaseReadsRequireQuorumChecking()
    {
        var storage = new CoreTestStorage();
        var config = new RaftConfig
        {
            Id = 1,
            Storage = storage,
            ReadOnlyOption = ReadOnlyOption.LeaseBased,
        };

        Assert.Throws<ArgumentException>(
            () => new RaftCore(config, _ => 0));
        Assert.Equal(0, storage.CallCount);
    }

    [Fact]
    public void FiniteLimitsAndFeatureFlagsAreCopied()
    {
        var storage = new CoreTestStorage();
        var config = new RaftConfig
        {
            Id = 1,
            Storage = storage,
            Applied = 0,
            AsyncStorageWrites = true,
            MaxSizePerMessage = 100,
            MaxCommittedSizePerReady = 200,
            MaxUncommittedEntriesSize = 300,
            MaxInflightMessages = 4,
            MaxInflightBytes = 400,
            CheckQuorum = true,
            PreVote = true,
            ReadOnlyOption = ReadOnlyOption.LeaseBased,
            Logger = NullRaftLogger.Instance,
            DisableProposalForwarding = true,
            DisableConfChangeValidation = true,
            StepDownOnRemoval = true,
        };

        var core = new RaftCore(config, _ => 0);

        Assert.True(core.AsyncStorageWrites);
        Assert.Equal(100UL, core.MaxMessageSize);
        Assert.Equal(200UL, core.MaxCommittedSizePerReady);
        Assert.Equal(300UL, core.MaxUncommittedEntriesSize);
        Assert.Equal(4, core.Tracker.MaxInflightMessages);
        Assert.Equal(400UL, core.Tracker.MaxInflightBytes);
        Assert.True(core.CheckQuorum);
        Assert.True(core.PreVote);
        Assert.Equal(ReadOnlyOption.LeaseBased, core.ReadOnly.Option);
        Assert.True(core.DisableProposalForwarding);
        Assert.True(core.DisableConfChangeValidation);
        Assert.True(core.StepDownOnRemoval);
    }
}
