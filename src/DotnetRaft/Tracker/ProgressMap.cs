using System.Globalization;
using System.Text;

using DotnetRaft.Quorum;

namespace DotnetRaft.Tracker;

internal sealed class ProgressMap
    : Dictionary<ulong, Progress>,
      IAckedIndexer
{
    bool IAckedIndexer.TryGetAckedIndex(
        ulong voterId,
        out ulong index)
    {
        if (TryGetValue(voterId, out Progress? progress))
        {
            index = progress.Match;
            return true;
        }

        index = 0;
        return false;
    }

    public override string ToString()
    {
        var builder = new StringBuilder();
        foreach ((ulong id, Progress progress) in this.OrderBy(
                     pair => pair.Key))
        {
            builder.Append(id.ToString(CultureInfo.InvariantCulture))
                .Append(": ")
                .Append(progress)
                .Append('\n');
        }

        return builder.ToString();
    }
}
