using DotnetRaft.Quorum;

namespace DotnetRaft.Tests.Quorum;

public sealed class QuorumTests
{
    [Theory]
    [InlineData(new ulong[] { }, new ulong[] { }, ulong.MaxValue)]
    [InlineData(new ulong[] { 1 }, new ulong[] { }, 0)]
    [InlineData(new ulong[] { 1 }, new ulong[] { 7 }, 7)]
    [InlineData(new ulong[] { 1, 2, 3 }, new ulong[] { 101, 102, 103 }, 102)]
    [InlineData(new ulong[] { 1, 2, 3 }, new ulong[] { 101, 0, 103 }, 101)]
    [InlineData(new ulong[] { 1, 2, 3, 4, 5 }, new ulong[] { 10, 20, 30, 0, 0 }, 10)]
    public void MajorityCommittedIndexUsesTheQuorumPosition(
        ulong[] voters,
        ulong[] indexes,
        ulong expected)
    {
        var config = new MajorityConfig(voters);
        var acknowledged = new Dictionary<ulong, ulong>();
        for (var index = 0; index < indexes.Length; index++)
        {
            if (indexes[index] != 0)
            {
                acknowledged[voters[index]] = indexes[index];
            }
        }

        Assert.Equal(expected, config.CommittedIndex(new DictionaryIndexer(acknowledged)));
    }

    [Theory]
    [MemberData(nameof(VoteCases))]
    public void MajorityVoteResultMatchesQuorumRules(
        ulong[] voters,
        IReadOnlyDictionary<ulong, bool> votes,
        int expected)
    {
        var config = new MajorityConfig(voters);

        Assert.Equal((VoteResult)expected, config.VoteResult(votes));
    }

    [Fact]
    public void JointConfigRequiresBothMajorities()
    {
        var config = new JointConfig(
            new MajorityConfig([1, 2, 3]),
            new MajorityConfig([2, 3, 4]));
        var indexes = new DictionaryIndexer(new Dictionary<ulong, ulong>
        {
            [1] = 10,
            [2] = 20,
            [3] = 30,
            [4] = 40,
        });

        Assert.Equal(20UL, config.CommittedIndex(indexes));
        Assert.Equal(
            VoteResult.Pending,
            config.VoteResult(new Dictionary<ulong, bool>
            {
                [1] = true,
                [2] = true,
            }));
        Assert.Equal(
            VoteResult.Won,
            config.VoteResult(new Dictionary<ulong, bool>
            {
                [1] = true,
                [2] = true,
                [3] = true,
            }));
    }

    [Fact]
    public void EmptyOutgoingConfigBehavesLikeTheIncomingMajority()
    {
        var incoming = new MajorityConfig([1, 2, 3]);
        var joint = new JointConfig(incoming);
        var indexes = new DictionaryIndexer(new Dictionary<ulong, ulong>
        {
            [1] = 10,
            [2] = 20,
            [3] = 30,
        });
        var votes = new Dictionary<ulong, bool>
        {
            [1] = true,
            [2] = true,
        };

        Assert.Equal(incoming.CommittedIndex(indexes), joint.CommittedIndex(indexes));
        Assert.Equal(incoming.VoteResult(votes), joint.VoteResult(votes));
    }

    [Fact]
    public void DescribeMatchesTheReferenceLayout()
    {
        var config = new MajorityConfig([1, 2, 3]);
        var indexes = new DictionaryIndexer(new Dictionary<ulong, ulong>
        {
            [1] = 101,
            [3] = 103,
        });

        var description = config.Describe(indexes);

        const string expected =
            "       idx\n" +
            "x>     101    (id=1)\n" +
            "?        0    (id=2)\n" +
            "xx>    103    (id=3)\n";

        Assert.Equal(expected, description);
    }

    [Fact]
    public void MajorityCommitMatchesAlternativeComputation()
    {
        var random = new Random(1);

        for (var iteration = 0; iteration < 20_000; iteration++)
        {
            var voterCount = random.Next(0, 11);
            var voters = Enumerable.Range(1, voterCount)
                .Select(value => (ulong)value)
                .ToArray();
            var indexes = new Dictionary<ulong, ulong>();
            foreach (var voter in voters)
            {
                if (random.Next(0, 4) != 0)
                {
                    indexes[voter] = (ulong)random.Next(0, 20);
                }
            }

            var config = new MajorityConfig(voters);
            var indexer = new DictionaryIndexer(indexes);

            Assert.Equal(
                AlternativeCommittedIndex(config, indexer),
                config.CommittedIndex(indexer));
        }
    }

    public static TheoryData<ulong[], IReadOnlyDictionary<ulong, bool>, int> VoteCases()
    {
        return new TheoryData<ulong[], IReadOnlyDictionary<ulong, bool>, int>
        {
            { [], new Dictionary<ulong, bool>(), (int)VoteResult.Won },
            { [1], new Dictionary<ulong, bool>(), (int)VoteResult.Pending },
            { [1], new Dictionary<ulong, bool> { [1] = true }, (int)VoteResult.Won },
            { [1], new Dictionary<ulong, bool> { [1] = false }, (int)VoteResult.Lost },
            { [1, 2, 3], new Dictionary<ulong, bool> { [1] = true }, (int)VoteResult.Pending },
            {
                [1, 2, 3],
                new Dictionary<ulong, bool> { [1] = true, [2] = true },
                (int)VoteResult.Won
            },
            {
                [1, 2, 3],
                new Dictionary<ulong, bool> { [1] = false, [2] = false },
                (int)VoteResult.Lost
            },
        };
    }

    private static ulong AlternativeCommittedIndex(
        MajorityConfig config,
        IAckedIndexer indexer)
    {
        if (config.Count == 0)
        {
            return ulong.MaxValue;
        }

        var quorum = (config.Count / 2) + 1;
        ulong result = 0;
        foreach (var voter in config)
        {
            if (!indexer.TryGetAckedIndex(voter, out var candidate))
            {
                continue;
            }

            var acknowledged = config.Count(id =>
                indexer.TryGetAckedIndex(id, out var index) && index >= candidate);
            if (acknowledged >= quorum)
            {
                result = Math.Max(result, candidate);
            }
        }

        return result;
    }

    private sealed class DictionaryIndexer(IReadOnlyDictionary<ulong, ulong> indexes)
        : IAckedIndexer
    {
        public bool TryGetAckedIndex(ulong voterId, out ulong index)
        {
            return indexes.TryGetValue(voterId, out index);
        }
    }
}
