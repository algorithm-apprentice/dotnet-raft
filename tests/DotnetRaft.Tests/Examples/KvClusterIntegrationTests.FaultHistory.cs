using System.Collections.Concurrent;

using DotnetRaft.Examples.KvCluster;
using DotnetRaft.Protocol;

namespace DotnetRaft.Tests.Examples;

public sealed partial class KvClusterIntegrationTests
{
    [Fact]
    public async Task RegisterHistorySurvivesPartitionSnapshotAndRetry()
    {
        const ulong laggerId = 3;
        const int timedOutRequest = 2;
        var network = new InMemoryNetwork();
        var oldLeader = CreateNode(1, network, snapshotThresholdEntries: 2, requestTimeout: TimeSpan.FromSeconds(2));
        var survivor = CreateNode(2, network, automaticTicks: true, snapshotThresholdEntries: 2);
        var lagger = CreateLagger();
        var active = new List<NodeHarness> { oldLeader, survivor, lagger };
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        var history = new RegisterClientHistory(guard.Token);
        string phase = "bootstrap";
        try
        {
            await StartAndElectAsync(network, [.. active]);
            ProposalResponse initial = Required(await history.MutateAsync(oldLeader, 1, "initial"));
            await WaitForApplicationAsync([.. active], initial.AppliedIndex);
            await WaitForStatusAsync([lagger], "lagger to learn the original leader", (_, status) => status.LeaderId == 1);

            phase = "asymmetric partition and client timeouts";
            network.Drop = message => message.To == laggerId;
            Task<ProposalResponse?> uncertainWrite = history.MutateAsync(lagger, timedOutRequest, "uncertain");
            Task<ReadResponse?> unreachableRead = history.ReadAsync(lagger);
            await Task.WhenAll(uncertainWrite, unreachableRead).WaitAsync(guard.Token);
            Assert.Null(await uncertainWrite);
            Assert.Null(await unreachableRead);
            ulong offlineLast = lagger.Storage.GetLastIndex();

            phase = "majority observes the timed-out write and compacts";
            var command = new KvCommand(RegisterClientHistory.RequestId(timedOutRequest), KvCommandType.Set,
                RegisterClientHistory.Key, "uncertain");
            await WaitForStatusAsync([oldLeader, survivor], "the timed-out write to apply on the majority",
                (replica, _) => replica.State.ResolveRequest(command) is { Conflict: false });
            Assert.Equal("uncertain", Required(await history.ReadAsync(survivor)).Value);
            ulong originalIndex = Assert.IsType<KvRequestResolution>(oldLeader.State.ResolveRequest(command)).ResultIndex;
            Assert.True(offlineLast < originalIndex, "The lagging node must lack the timed-out write.");

            _ = Required(await history.MutateAsync(oldLeader, 3, "newer"));
            _ = Required(await history.MutateAsync(survivor, 4, "newest"));
            await WaitForStatusAsync([oldLeader, survivor], "both survivors to compact through the uncertain write",
                (replica, _) => replica.Storage.GetFirstIndex() - 1 >= originalIndex);

            phase = "reopen the lagger and elect with the other survivor";
            NodeHarness stopped = lagger;
            network.Unregister(stopped.Host);
            await StopAndDisposeNodesAsync([stopped], disposeDirectories: false);
            active.Remove(stopped);
            try
            {
                lagger = CreateLagger(stopped.Directory);
                active.Add(lagger);
            }
            catch
            {
                stopped.Directory.Dispose();
                throw;
            }

            await StartNodesAsync(network, [lagger]);
            Assert.True(lagger.Storage.GetLastIndex() < originalIndex);
            network.Drop = message => message.From == 1 || message.To == 1;
            ulong formerTerm = (await oldLeader.Host.GetStatusAsync(guard.Token)).Term;
            await ElectAsync(survivor);
            ProposalResponse afterElection = Required(await history.MutateAsync(survivor, 5, "new-leader"));
            await WaitForApplicationAsync([lagger], afterElection.AppliedIndex);
            Assert.True(lagger.Storage.GetFirstIndex() - 1 > offlineLast, "Recovery must install a snapshot, not replay the missing log.");
            ClusterStatusResponse isolated = await oldLeader.Host.GetStatusAsync(guard.Token);
            Assert.Equal(RaftRole.Leader, isolated.Role);
            Assert.Equal(formerTerm, isolated.Term);
            Assert.True((await survivor.Host.GetStatusAsync(guard.Token)).Term > formerTerm);
            phase = "isolated leader and recovered follower client traffic";
            Task<ReadResponse?> isolatedRead = history.ReadAsync(oldLeader);
            Task<ProposalResponse?> retriedWrite = history.MutateAsync(lagger, timedOutRequest, "uncertain");
            await Task.WhenAll(isolatedRead, retriedWrite).WaitAsync(guard.Token);
            Assert.Null(await isolatedRead);

            ProposalResponse retry = Required(await retriedWrite);
            Assert.True(retry.Duplicate);
            Assert.Equal(originalIndex, retry.AppliedIndex);
            Assert.Equal("new-leader", Required(await history.ReadAsync(lagger)).Value);

            phase = "heal, delete, and verify the full client history";
            network.Drop = null;
            survivor.TickSource.Tick();
            await WaitForStatusAsync([oldLeader], "the healed node to follow the new leader", (_, status) => status.LeaderId == 2);
            _ = Required(await history.MutateAsync(oldLeader, 6, null));
            Assert.Null(Required(await history.ReadAsync(oldLeader)).Value);
            Assert.True(RegisterAttemptHistory.IsLinearizable(history.Attempts), history.Describe());
        }
        catch (Exception exception)
        {
            throw new InvalidOperationException($"Faulted KV schedule failed in '{phase}'. Client attempts:\n{history.Describe()}", exception);
        }
        finally
        {
            await StopAndDisposeNodesAsync(active, disposeDirectories: true);
        }

        NodeHarness CreateLagger(TemporaryNodeDirectory? directory = null) =>
            CreateNode(laggerId, network, directory: directory, requestTimeout: TimeSpan.FromSeconds(1));
    }

    private static T Required<T>(T? result) where T : class => Assert.IsType<T>(result);

    private sealed class RegisterClientHistory(CancellationToken cancellationToken)
    {
        internal const string Key = "faulted-register";
        private readonly ConcurrentQueue<(ulong Node, RegisterAttempt Attempt)> attempts = new();
        private int order;

        internal RegisterAttempt[] Attempts => [.. attempts.Select(item => item.Attempt)];

        internal static Guid RequestId(int number) => new(number, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        internal async Task<ProposalResponse?> MutateAsync(NodeHarness node, int requestNumber, string? value)
        {
            Guid id = RequestId(requestNumber);
            int start = Interlocked.Increment(ref order);
            try
            {
                ProposalResponse result = value is null
                    ? await node.Host.DeleteAsync(Key, id, cancellationToken)
                    : await node.Host.PutAsync(Key, value, id, cancellationToken);
                Record(node, start, id, value, succeeded: true, result.AppliedIndex);
                Assert.Equal(id, result.RequestId);
                Assert.True(result.AppliedIndex <= result.PhysicalApplied);
                return result;
            }
            catch (TimeoutException)
            {
                Record(node, start, id, value, succeeded: false);
                return null;
            }
        }

        internal async Task<ReadResponse?> ReadAsync(NodeHarness node)
        {
            int start = Interlocked.Increment(ref order);
            try
            {
                ReadResponse result = await node.Host.ReadAsync(Key, cancellationToken);
                Record(node, start, null, result.Value, succeeded: true);
                Assert.True(result.Linearizable);
                Assert.Equal(result.Value is not null, result.Found);
                Assert.True(result.PhysicalApplied >= result.RequiredIndex);
                return result;
            }
            catch (TimeoutException)
            {
                Record(node, start, null, null, succeeded: false);
                return null;
            }
        }

        internal string Describe() => string.Join(Environment.NewLine,
            attempts.OrderBy(item => item.Attempt.Started).Select(item => $"node {item.Node}: {item.Attempt}"));

        private void Record(NodeHarness node, int start, Guid? id, string? value, bool succeeded, ulong? index = null) =>
            attempts.Enqueue((node.Host.NodeId, new RegisterAttempt(start, Interlocked.Increment(ref order), id, value, succeeded, index)));
    }
}
