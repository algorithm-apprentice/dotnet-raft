namespace DotnetRaft.Tests.Examples;

public sealed class RegisterAttemptHistoryTests
{
    private static readonly Guid First = new("10000000-0000-0000-0000-000000000001");
    private static readonly Guid Second = new("10000000-0000-0000-0000-000000000002");
    private static readonly Guid Third = new("10000000-0000-0000-0000-000000000003");

    [Theory]
    [InlineData(null)]
    [InlineData("first")]
    public void AnUnconfirmedWriteMayBeDroppedOrApplyAfterItsTimeout(string? lastValue)
    {
        Assert.True(RegisterAttemptHistory.IsLinearizable([
            new(1, 2, First, "first", Succeeded: false),
            new(3, 4, null, null, Succeeded: true),
            new(5, 6, null, lastValue, Succeeded: true),
        ]));
    }

    [Theory]
    [InlineData("second", 4UL, true)]
    [InlineData("first", 4UL, false)]
    [InlineData("second", 7UL, false)]
    public void ASuccessfulRetryMustNotReapplyTheMutationOrChangeItsResultIndex(string lastValue, ulong retryIndex, bool expected)
    {
        Assert.Equal(expected, RegisterAttemptHistory.IsLinearizable([
            new(1, 2, First, "first", Succeeded: false),
            new(3, 4, null, "first", Succeeded: true),
            new(5, 6, Second, "second", Succeeded: true, ResultIndex: 5),
            new(7, 8, First, "first", Succeeded: true, ResultIndex: 4),
            new(9, 10, First, "first", Succeeded: true, ResultIndex: retryIndex),
            new(11, 12, null, lastValue, Succeeded: true),
        ]));
    }

    [Fact]
    public void LaterTimeoutCannotHideLossOfAnAcknowledgedWrite()
    {
        Assert.False(RegisterAttemptHistory.IsLinearizable([
            new(1, 2, First, "first", Succeeded: true, ResultIndex: 4),
            new(3, 4, null, null, Succeeded: true),
            new(5, 6, First, "first", Succeeded: false),
        ]));
    }

    [Fact]
    public void ASecondIdWithTheSameValueIsANewWrite()
    {
        Assert.True(RegisterAttemptHistory.IsLinearizable([
            new(1, 2, First, "same", Succeeded: true, ResultIndex: 4),
            new(3, 4, Third, null, Succeeded: true, ResultIndex: 5),
            new(5, 6, Second, "same", Succeeded: true, ResultIndex: 6),
            new(7, 8, null, "same", Succeeded: true),
        ]));
    }

    [Theory]
    [InlineData(5UL, "second", true)]
    [InlineData(5UL, "first", false)]
    [InlineData(4UL, "second", false)]
    public void DistinctWritesHaveDistinctIndexesThatOrderOverlappingCalls(ulong secondIndex, string lastValue, bool expected)
    {
        Assert.Equal(expected, RegisterAttemptHistory.IsLinearizable([
            new(1, 4, First, "first", Succeeded: true, ResultIndex: 4),
            new(2, 3, Second, "second", Succeeded: true, ResultIndex: secondIndex),
            new(5, 6, null, lastValue, Succeeded: true),
        ]));
    }

    [Fact]
    public void AFailedReadDoesNotInventAValue()
    {
        Assert.True(RegisterAttemptHistory.IsLinearizable([
            new(1, 2, null, "ignored", Succeeded: false),
            new(3, 4, null, null, Succeeded: true),
        ]));
    }
}
