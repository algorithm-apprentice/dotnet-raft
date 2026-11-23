namespace DotnetRaft.Tests.Examples;

public sealed class RegisterHistoryTests
{
    [Fact]
    public void ReadCannotMissAnEarlierCompletedWrite()
    {
        RegisterOperation[] stale =
        [
            new(1, 2, IsRead: false, "new"),
            new(3, 4, IsRead: true, null),
        ];
        Assert.False(RegisterHistory.IsLinearizable(stale));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("new")]
    public void AnOverlappingReadCanPrecedeOrFollowAWrite(string? value)
    {
        RegisterOperation[] overlap =
        [
            new(1, 3, IsRead: false, "new"),
            new(2, 4, IsRead: true, value),
        ];
        Assert.True(RegisterHistory.IsLinearizable(overlap));
    }
}

// Null value means absence/deletion; null completion means an optional mutation.
internal readonly record struct RegisterOperation(
    int Started, int? Completed, bool IsRead, string? Value, ulong? WriteIndex = null);

internal static class RegisterHistory
{
    internal static bool IsLinearizable(IReadOnlyList<RegisterOperation> history)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(history.Count, 64);
        var predecessors = new ulong[history.Count];
        var writeIndexes = new HashSet<ulong>();
        for (var index = 0; index < history.Count; index++)
        {
            RegisterOperation operation = history[index];
            if ((operation.IsRead && operation.Completed is null) || operation.Completed <= operation.Started)
            {
                throw new ArgumentException("An operation must have a valid interval; only mutations can be unconfirmed.", nameof(history));
            }

            if (operation.WriteIndex is { } writeIndex)
            {
                if (operation.IsRead || operation.Completed is null || writeIndex == 0)
                {
                    throw new ArgumentException("Only a confirmed mutation can carry a write index.", nameof(history));
                }

                if (!writeIndexes.Add(writeIndex))
                {
                    return false;
                }
            }

            for (var other = 0; other < history.Count; other++)
            {
                RegisterOperation prior = history[other];
                if (prior.Completed < operation.Started || prior.WriteIndex < operation.WriteIndex)
                {
                    predecessors[index] |= 1UL << other;
                }
            }
        }

        var visited = new HashSet<(ulong Remaining, string? Value)>();
        return Search((1UL << history.Count) - 1, null);

        bool Search(ulong remaining, string? value)
        {
            if (remaining == 0)
            {
                return true;
            }

            if (!visited.Add((remaining, value)))
            {
                return false;
            }

            for (var index = 0; index < history.Count; index++)
            {
                ulong bit = 1UL << index;
                if ((remaining & bit) == 0 || (remaining & predecessors[index]) != 0)
                {
                    continue;
                }

                RegisterOperation operation = history[index];
                if (operation.Completed is null && Search(remaining & ~bit, value))
                {
                    return true;
                }

                if (operation.IsRead && !string.Equals(operation.Value, value, StringComparison.Ordinal))
                {
                    continue;
                }

                if (Search(remaining & ~bit, operation.IsRead ? value : operation.Value))
                {
                    return true;
                }
            }

            return false;
        }
    }
}
