using DotnetRaft.Protocol;

using ProtocolConfChange = DotnetRaft.Protocol.ConfChange;

namespace DotnetRaft;

public interface IRaftNode : IAsyncDisposable
{
    Task Completion { get; }

    Status? TerminalStatus { get; }

    void Tick();

    ValueTask CampaignAsync(
        CancellationToken cancellationToken = default);

    /// <summary>Waits for submission, not commitment or physical application.</summary>
    ValueTask ProposeAsync(
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default);

    ValueTask ProposeConfChangeAsync(
        ProtocolConfChange change,
        CancellationToken cancellationToken = default);

    ValueTask ProposeConfChangeAsync(
        ConfChangeV2 change,
        CancellationToken cancellationToken = default);

    ValueTask StepAsync(
        Message message,
        CancellationToken cancellationToken = default);

    ValueTask ForgetLeaderAsync(
        CancellationToken cancellationToken = default);

    ValueTask ReadIndexAsync(
        ReadOnlyMemory<byte> context,
        CancellationToken cancellationToken = default);

    ValueTask TransferLeadershipAsync(
        ulong transferee,
        CancellationToken cancellationToken = default);

    ValueTask ReportUnreachableAsync(
        ulong id,
        CancellationToken cancellationToken = default);

    ValueTask ReportSnapshotAsync(
        ulong id,
        SnapshotStatus status,
        CancellationToken cancellationToken = default);

    /// <summary>Accepts the next batch for the host's single Ready consumer.</summary>
    ValueTask<Ready> WaitForReadyAsync(
        CancellationToken cancellationToken = default);

    /// <summary>Acknowledges the outstanding batch in Ready/Advance mode.</summary>
    ValueTask AdvanceAsync(
        CancellationToken cancellationToken = default);

    ValueTask<ConfState> ApplyConfChangeAsync(
        ProtocolConfChange change,
        CancellationToken cancellationToken = default);

    ValueTask<ConfState> ApplyConfChangeAsync(
        ConfChangeV2 change,
        CancellationToken cancellationToken = default);

    ValueTask<Status> GetStatusAsync(
        CancellationToken cancellationToken = default);

    ValueTask StopAsync();
}
