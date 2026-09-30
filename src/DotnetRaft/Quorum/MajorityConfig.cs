using System.Collections;
using System.Globalization;
using System.Text;

namespace DotnetRaft.Quorum;

internal sealed class MajorityConfig : IReadOnlyCollection<ulong>
{
    private readonly HashSet<ulong> voters;

    public MajorityConfig()
        : this([])
    {
    }

    public MajorityConfig(IEnumerable<ulong> voters)
    {
        ArgumentNullException.ThrowIfNull(voters);
        this.voters = [.. voters];
    }

    public int Count => voters.Count;

    public bool Add(ulong id)
    {
        return voters.Add(id);
    }

    public bool Remove(ulong id)
    {
        return voters.Remove(id);
    }

    public bool Contains(ulong id)
    {
        return voters.Contains(id);
    }

    public MajorityConfig Clone()
    {
        return new MajorityConfig(voters);
    }

    public bool SetEquals(IEnumerable<ulong> other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return voters.SetEquals(other);
    }

    public ulong CommittedIndex(IAckedIndexer indexer)
    {
        ArgumentNullException.ThrowIfNull(indexer);

        if (voters.Count == 0)
        {
            return ulong.MaxValue;
        }

        var indexes = new ulong[voters.Count];
        var position = indexes.Length - 1;

        foreach (var id in voters)
        {
            if (indexer.TryGetAckedIndex(id, out var index))
            {
                indexes[position] = index;
                position--;
            }
        }

        Array.Sort(indexes);
        var quorumPosition = indexes.Length - ((indexes.Length / 2) + 1);
        return indexes[quorumPosition];
    }

    public VoteResult VoteResult(IReadOnlyDictionary<ulong, bool> votes)
    {
        ArgumentNullException.ThrowIfNull(votes);

        if (voters.Count == 0)
        {
            return Quorum.VoteResult.Won;
        }

        var granted = 0;
        var missing = 0;
        foreach (var id in voters)
        {
            if (!votes.TryGetValue(id, out var vote))
            {
                missing++;
            }
            else if (vote)
            {
                granted++;
            }
        }

        var quorum = (voters.Count / 2) + 1;
        if (granted >= quorum)
        {
            return Quorum.VoteResult.Won;
        }

        return granted + missing >= quorum
            ? Quorum.VoteResult.Pending
            : Quorum.VoteResult.Lost;
    }

    public string Describe(IAckedIndexer indexer)
    {
        ArgumentNullException.ThrowIfNull(indexer);
        if (voters.Count == 0)
        {
            return "<empty majority quorum>";
        }

        var info = voters
            .Select(id =>
            {
                var found = indexer.TryGetAckedIndex(id, out var index);
                return new VoterInfo(id, index, found, 0);
            })
            .OrderBy(item => item.Index)
            .ThenBy(item => item.Id)
            .ToArray();

        for (var index = 1; index < info.Length; index++)
        {
            var bar = info[index - 1].Index < info[index].Index
                ? index
                : info[index - 1].Bar;
            info[index] = info[index] with
            {
                Bar = bar,
            };
        }

        Array.Sort(info, static (left, right) => left.Id.CompareTo(right.Id));

        var builder = new StringBuilder();
        builder.Append(' ', voters.Count).Append("    idx\n");

        foreach (var item in info)
        {
            if (!item.Found)
            {
                builder.Append('?').Append(' ', voters.Count);
            }
            else
            {
                builder.Append('x', item.Bar)
                    .Append('>')
                    .Append(' ', voters.Count - item.Bar);
            }

            builder.Append(' ')
                .Append(item.Index.ToString(CultureInfo.InvariantCulture).PadLeft(5))
                .Append("    (id=")
                .Append(item.Id.ToString("x", CultureInfo.InvariantCulture))
                .Append(')')
                .Append('\n');
        }

        return builder.ToString();
    }

    public IEnumerator<ulong> GetEnumerator()
    {
        return voters.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public override string ToString()
    {
        var ids = voters
            .Order()
            .Select(id => id.ToString("x", CultureInfo.InvariantCulture));
        return $"({string.Join(' ', ids)})";
    }

    private readonly record struct VoterInfo(ulong Id, ulong Index, bool Found, int Bar);
}
