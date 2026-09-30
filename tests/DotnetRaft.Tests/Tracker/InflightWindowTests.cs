using DotnetRaft.Core;
using DotnetRaft.Tracker;

namespace DotnetRaft.Tests.Tracker;

public sealed class InflightWindowTests
{
    [Fact]
    public void AddTracksCountBytesAndCountFullness()
    {
        var window = new InflightWindow(capacity: 10, maxBytes: 0);

        for (ulong index = 0; index < 10; index++)
        {
            Assert.False(window.IsFull);
            window.Add(index, 100 + index);
        }

        Assert.Equal(10, window.Count);
        Assert.Equal(1045UL, window.Bytes);
        Assert.True(window.IsFull);
        Assert.Throws<RaftInvariantException>(() => window.Add(10, 110));
    }

    [Fact]
    public void FreeThroughIsInclusiveAndHandlesRingRotation()
    {
        var window = new InflightWindow(capacity: 10, maxBytes: 0);
        for (ulong index = 0; index < 10; index++)
        {
            window.Add(index, 100 + index);
        }

        window.FreeThrough(0);
        Assert.Equal(9, window.Count);
        Assert.Equal(945UL, window.Bytes);

        window.FreeThrough(4);
        Assert.Equal(5, window.Count);
        Assert.Equal(535UL, window.Bytes);

        window.FreeThrough(8);
        Assert.Equal(1, window.Count);
        Assert.Equal(109UL, window.Bytes);

        for (ulong index = 10; index < 15; index++)
        {
            window.Add(index, 100 + index);
        }

        window.FreeThrough(12);
        Assert.Equal(2, window.Count);
        Assert.Equal(227UL, window.Bytes);

        window.FreeThrough(14);
        Assert.Equal(0, window.Count);
        Assert.Equal(0UL, window.Bytes);

        window.Add(15, 115);
        Assert.Equal(1, window.Count);
        Assert.Equal(115UL, window.Bytes);
    }

    [Fact]
    public void OldAcknowledgementDoesNotChangeTheWindow()
    {
        var window = new InflightWindow(capacity: 3, maxBytes: 0);
        window.Add(10, 100);
        window.Add(11, 101);

        window.FreeThrough(9);

        Assert.Equal(2, window.Count);
        Assert.Equal(201UL, window.Bytes);
    }

    [Fact]
    public void FullnessMatchesCountAndSoftByteLimits()
    {
        var cases = new[]
        {
            new FullCase("always-full", 0, 0, 0, 0, 0),
            new FullCase("single-entry", 1, 0, 1, 1, 2),
            new FullCase("single-entry-overflow", 1, 10, 1, 1, 2),
            new FullCase("multi-entry", 15, 0, 15, 6, 22),
            new FullCase("slight-overflow", 8, 400, 4, 2, 7),
            new FullCase("exact-max-bytes", 8, 406, 4, 3, 8),
            new FullCase("larger-overflow", 15, 408, 5, 1, 6),
        };

        foreach (FullCase testCase in cases)
        {
            var window = new InflightWindow(
                testCase.Capacity,
                testCase.MaxBytes);

            AddUntilFull(window, 0, testCase.FullAt);
            window.FreeThrough(testCase.FreeThrough);
            AddUntilFull(window, testCase.FullAt, testCase.FullAgainAt);
            Assert.Throws<RaftInvariantException>(
                () => window.Add(100, 1024));
        }
    }

    [Fact]
    public void ResetClearsCountAndByteAccountingWithoutChangingLimits()
    {
        var window = new InflightWindow(capacity: 10, maxBytes: 1000);
        ulong index = 0;

        for (int epoch = 0; epoch < 100; epoch++)
        {
            window.Reset();
            for (int message = 0; message < 5; message++)
            {
                Assert.False(window.IsFull);
                index++;
                window.Add(index, 16);
            }

            window.FreeThrough(index - 2);
            Assert.False(window.IsFull);
            Assert.Equal(2, window.Count);
            Assert.Equal(32UL, window.Bytes);
        }

        window.FreeThrough(index);
        Assert.Equal(0, window.Count);
        Assert.Equal(0UL, window.Bytes);
        Assert.Equal(10, window.Capacity);
        Assert.Equal(1000UL, window.MaxBytes);
    }

    [Fact]
    public void CloneDoesNotShareRingStorageOrCounters()
    {
        var original = new InflightWindow(capacity: 5, maxBytes: 0);
        original.Add(1, 10);
        original.Add(2, 20);
        original.Add(3, 30);
        original.Add(4, 40);
        original.Add(5, 50);

        InflightWindow clone = original.Clone();
        clone.FreeThrough(2);
        clone.Add(6, 60);
        original.FreeThrough(1);

        Assert.Equal(4, original.Count);
        Assert.Equal(140UL, original.Bytes);
        Assert.Equal(4, clone.Count);
        Assert.Equal(180UL, clone.Bytes);
    }

    [Fact]
    public void InvalidCapacityAndNonMonotonicIndexesAreRejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new InflightWindow(-1, 0));

        var window = new InflightWindow(capacity: 3, maxBytes: 0);
        window.Add(10, 100);

        Assert.Throws<RaftInvariantException>(() => window.Add(10, 101));
        Assert.Throws<RaftInvariantException>(() => window.Add(9, 101));
        Assert.Equal(1, window.Count);
        Assert.Equal(100UL, window.Bytes);
    }

    [Fact]
    public void ByteOverflowIsRejectedWithoutMutation()
    {
        var window = new InflightWindow(capacity: 2, maxBytes: 0);
        window.Add(1, ulong.MaxValue);

        Assert.Throws<RaftInvariantException>(() => window.Add(2, 1));
        Assert.Equal(1, window.Count);
        Assert.Equal(ulong.MaxValue, window.Bytes);
    }

    [Fact]
    public void ZeroByteLimitMeansUnlimitedBytes()
    {
        var window = new InflightWindow(capacity: 2, maxBytes: 0);

        window.Add(1, ulong.MaxValue);

        Assert.False(window.IsFull);
        Assert.Equal(1, window.Count);
    }

    private static void AddUntilFull(
        InflightWindow window,
        int begin,
        int end)
    {
        for (int index = begin; index < end; index++)
        {
            Assert.False(window.IsFull);
            window.Add((ulong)index, (ulong)(100 + index));
        }

        Assert.True(window.IsFull);
    }

    private sealed record FullCase(
        string Name,
        int Capacity,
        ulong MaxBytes,
        int FullAt,
        ulong FreeThrough,
        int FullAgainAt)
    {
        public override string ToString()
        {
            return Name;
        }
    }
}
