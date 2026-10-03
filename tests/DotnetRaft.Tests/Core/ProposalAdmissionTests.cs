using DotnetRaft.Core;
using DotnetRaft.Protocol;

using Google.Protobuf;

namespace DotnetRaft.Tests.Core;

public sealed class ProposalAdmissionTests
{
    [Fact]
    public void PrepareIsPureAndCommitPointsAreExplicit()
    {
        var admission = new ProposalAdmission(
            maxUncommittedSize: 10);
        admission.SetUncommittedSize(4);
        admission.SetPendingConfigurationIndex(7);
        Entry[] entries =
        [
            new Entry
            {
                Data =
                    ByteString.CopyFrom([1, 2, 3]),
            },
        ];

        ProposalAdmissionPlan plan =
            admission.Prepare(
                entries,
                pendingConfigurationIndex: 9);

        Assert.True(plan.Accepted);
        Assert.Equal(7UL, plan.UncommittedSize);
        Assert.Equal(
            9UL,
            plan.PendingConfigurationIndex);
        Assert.Equal(4UL, admission.UncommittedSize);
        Assert.Equal(
            7UL,
            admission.PendingConfigurationIndex);

        admission.CommitQuota(plan);
        Assert.Equal(7UL, admission.UncommittedSize);
        Assert.Equal(
            7UL,
            admission.PendingConfigurationIndex);

        admission.PublishPending(plan);
        Assert.Equal(
            9UL,
            admission.PendingConfigurationIndex);
    }

    [Fact]
    public void QuotaPreparationPreservesExistingAdmissionRules()
    {
        var bounded = new ProposalAdmission(
            maxUncommittedSize: 5);
        bounded.SetUncommittedSize(4);
        ProposalAdmissionPlan rejected =
            bounded.Prepare(
                [EntryWithPayload(2)],
                pendingConfigurationIndex: 0);
        Assert.False(rejected.Accepted);
        Assert.Equal(4UL, bounded.UncommittedSize);

        var first = new ProposalAdmission(
            maxUncommittedSize: 5);
        ProposalAdmissionPlan oversizedFirst =
            first.Prepare(
                [EntryWithPayload(6)],
                pendingConfigurationIndex: 0);
        Assert.True(oversizedFirst.Accepted);
        Assert.Equal(
            6UL,
            oversizedFirst.UncommittedSize);

        var overflow = new ProposalAdmission(
            maxUncommittedSize: ulong.MaxValue);
        overflow.SetUncommittedSize(
            ulong.MaxValue);
        ProposalAdmissionPlan overflowed =
            overflow.Prepare(
                [EntryWithPayload(1)],
                pendingConfigurationIndex: 0);
        Assert.False(overflowed.Accepted);

        var exact = new ProposalAdmission(
            maxUncommittedSize: ulong.MaxValue);
        exact.SetUncommittedSize(
            ulong.MaxValue - 1);
        ProposalAdmissionPlan exactMaximum =
            exact.Prepare(
                [EntryWithPayload(1)],
                pendingConfigurationIndex: 0);
        Assert.True(exactMaximum.Accepted);
        Assert.Equal(
            ulong.MaxValue,
            exactMaximum.UncommittedSize);
    }

    [Fact]
    public void ConfigurationPreparationMatchesPendingAndJointRules()
    {
        var admission = new ProposalAdmission(
            maxUncommittedSize: 10);

        Assert.True(
            ProposalAdmission.CanAcceptConfiguration(
                candidatePendingIndex: 6,
                appliedIndex: 6,
                alreadyJoint: false,
                wantsLeaveJoint: false));
        Assert.False(
            ProposalAdmission.CanAcceptConfiguration(
                candidatePendingIndex: 7,
                appliedIndex: 6,
                alreadyJoint: false,
                wantsLeaveJoint: false));
        Assert.False(
            ProposalAdmission.CanAcceptConfiguration(
                candidatePendingIndex: 0,
                appliedIndex: 0,
                alreadyJoint: true,
                wantsLeaveJoint: false));
        Assert.False(
            ProposalAdmission.CanAcceptConfiguration(
                candidatePendingIndex: 0,
                appliedIndex: 0,
                alreadyJoint: false,
                wantsLeaveJoint: true));

        Assert.Equal(
            12UL,
            ProposalAdmission
                .PrepareConfigurationIndex(
                lastIndex: 10,
                offset: 1));
        RaftInvariantException exception =
            Assert.Throws<RaftInvariantException>(
                () => ProposalAdmission
                    .PrepareConfigurationIndex(
                        lastIndex:
                            ulong.MaxValue - 1,
                        offset: 1));
        Assert.StartsWith(
            "Configuration entry at offset 1 has no representable log index:",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ReleaseAndResetOwnAllAdmissionState()
    {
        var admission = new ProposalAdmission(
            maxUncommittedSize: 10);
        admission.SetUncommittedSize(7);
        admission.SetPendingConfigurationIndex(9);

        admission.Release(3);
        Assert.Equal(4UL, admission.UncommittedSize);
        admission.Release(4);
        Assert.Equal(0UL, admission.UncommittedSize);
        admission.Release(10);
        Assert.Equal(0UL, admission.UncommittedSize);

        Assert.True(
            admission.HasPendingConfiguration(
                appliedIndex: 8));
        Assert.False(
            admission.HasPendingConfiguration(
                appliedIndex: 9));
        Assert.True(
            admission.IsPendingApplied(
                appliedIndex: 9));

        admission.Reset();

        Assert.Equal(0UL, admission.UncommittedSize);
        Assert.Equal(
            0UL,
            admission.PendingConfigurationIndex);
    }

    private static Entry EntryWithPayload(
        int length)
    {
        return new Entry
        {
            Data = ByteString.CopyFrom(
                new byte[length]),
        };
    }
}
