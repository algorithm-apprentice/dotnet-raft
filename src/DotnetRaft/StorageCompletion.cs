using DotnetRaft.Core;
using DotnetRaft.Protocol;

namespace DotnetRaft;

/// <summary>A single-use acknowledgement issued to a local storage worker.</summary>
public abstract class StorageCompletion
{
    private readonly RawNode owner;
    private bool consumed;

    internal StorageCompletion(RawNode owner) => this.owner = owner;

    internal void Claim(RawNode node)
    {
        if (!ReferenceEquals(node, owner))
        {
            throw new StorageResponseValidationException("The storage completion belongs to another node instance.");
        }

        if (consumed)
        {
            throw new StorageResponseValidationException("The storage completion has already been consumed.");
        }

        consumed = true;
    }

    internal abstract void ApplyTo(RaftCore core);

    // Historical trace rendering, not a transport or completion API.
    internal abstract Message DescribeReceipt();
}
