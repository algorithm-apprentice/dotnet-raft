using DotnetRaft.Protocol;

namespace DotnetRaft.Core;

internal readonly record struct ProposalAdmissionPlan(
    bool Accepted,
    ulong UncommittedSize,
    ulong PendingConfigurationIndex);

internal sealed class ProposalAdmission
{
    internal ProposalAdmission(
        ulong maxUncommittedSize)
    {
        MaxUncommittedSize = maxUncommittedSize;
    }

    internal ulong MaxUncommittedSize { get; }

    internal ulong UncommittedSize { get; private set; }

    internal ulong PendingConfigurationIndex { get; private set; }

    internal ProposalAdmissionPlan Prepare(
        IEnumerable<Entry> entries,
        ulong pendingConfigurationIndex)
    {
        ulong payloadSize = 0;
        try
        {
            payloadSize = EntrySizing.PayloadSize(
                entries);
        }
        catch (OverflowException exception)
        {
            throw new RaftInvariantException(
                $"Proposal payload size overflowed: {exception.Message}");
        }

        ulong nextUncommittedSize =
            UncommittedSize;
        if (UncommittedSize > 0
            && payloadSize > 0)
        {
            if (payloadSize >
                ulong.MaxValue - UncommittedSize)
            {
                return new ProposalAdmissionPlan(
                    Accepted: false,
                    UncommittedSize,
                    pendingConfigurationIndex);
            }

            nextUncommittedSize =
                UncommittedSize + payloadSize;
            if (nextUncommittedSize >
                MaxUncommittedSize)
            {
                return new ProposalAdmissionPlan(
                    Accepted: false,
                    UncommittedSize,
                    pendingConfigurationIndex);
            }
        }
        else
        {
            nextUncommittedSize =
                payloadSize == 0
                    ? UncommittedSize
                    : payloadSize;
        }

        return new ProposalAdmissionPlan(
            Accepted: true,
            nextUncommittedSize,
            pendingConfigurationIndex);
    }

    internal void CommitQuota(
        ProposalAdmissionPlan plan)
    {
        UncommittedSize = plan.UncommittedSize;
    }

    internal void PublishPending(
        ProposalAdmissionPlan plan)
    {
        PendingConfigurationIndex =
            plan.PendingConfigurationIndex;
    }

    internal static bool CanAcceptConfiguration(
        ulong candidatePendingIndex,
        ulong appliedIndex,
        bool alreadyJoint,
        bool wantsLeaveJoint)
    {
        return candidatePendingIndex <= appliedIndex
            && (!alreadyJoint || wantsLeaveJoint)
            && (alreadyJoint || !wantsLeaveJoint);
    }

    internal static ulong PrepareConfigurationIndex(
        ulong lastIndex,
        int offset)
    {
        try
        {
            return checked(
                lastIndex + (ulong)offset + 1);
        }
        catch (OverflowException exception)
        {
            throw new RaftInvariantException(
                $"Configuration entry at offset {offset} has no representable log index: {exception.Message}");
        }
    }

    internal bool HasPendingConfiguration(
        ulong appliedIndex)
    {
        return PendingConfigurationIndex >
            appliedIndex;
    }

    internal bool IsPendingApplied(
        ulong appliedIndex)
    {
        return appliedIndex >=
            PendingConfigurationIndex;
    }

    internal void Release(ulong payloadSize)
    {
        UncommittedSize =
            payloadSize >= UncommittedSize
                ? 0
                : UncommittedSize - payloadSize;
    }

    internal void SetPendingConfigurationIndex(
        ulong index)
    {
        PendingConfigurationIndex = index;
    }

    internal void SetUncommittedSize(ulong size)
    {
        UncommittedSize = size;
    }

    internal void Reset()
    {
        PendingConfigurationIndex = 0;
        UncommittedSize = 0;
    }
}
