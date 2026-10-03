using System.Diagnostics;
using System.Runtime;
using System.Text.Json;

using DotnetRaft;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

const ulong FnvOffset = 14695981039346656037UL;
const ulong FnvPrime = 1099511628211UL;

if (!GCSettings.IsServerGC)
{
    throw new InvalidOperationException(
        "Refactor benchmarks require server GC.");
}

bool smoke = args.Contains(
    "--smoke",
    StringComparer.Ordinal);
int iterations = smoke ? 1 : 5;
int proposalOperations = smoke ? 100 : 10_000;
int dispatchOperations = smoke ? 1_000 : 100_000;

Run(
    "sync-proposal-cycle",
    proposalOperations,
    iterations,
    smoke
        ? 16479762179264021545UL
        : null,
    BenchmarkProposalCycle);
Run(
    "follower-heartbeat-dispatch",
    dispatchOperations,
    iterations,
    smoke
        ? 10682668835992251717UL
        : null,
    BenchmarkFollowerHeartbeat);

static void Run(
    string name,
    int operations,
    int iterations,
    ulong? expectedChecksum,
    Func<int, (
        long Ticks,
        long AllocatedBytes,
        ulong Checksum)> benchmark)
{
    _ = benchmark(operations);
    var time = new double[iterations];
    var allocations = new double[iterations];
    var checksums = new ulong[iterations];
    for (var iteration = 0;
         iteration < iterations;
         iteration++)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        (
            long ticks,
            long allocated,
            ulong checksum) = benchmark(operations);
        time[iteration] =
            ticks
            * 1_000_000_000d
            / Stopwatch.Frequency
            / operations;
        allocations[iteration] =
            allocated / (double)operations;
        checksums[iteration] = checksum;
        if (!double.IsFinite(time[iteration])
            || time[iteration] <= 0)
        {
            throw new InvalidOperationException(
                $"{name} produced invalid timing data.");
        }

        if (!double.IsFinite(allocations[iteration])
            || allocations[iteration] <= 0)
        {
            throw new InvalidOperationException(
                $"{name} produced invalid allocation data.");
        }
    }

    if (checksums.Any(
            checksum => checksum != checksums[0]))
    {
        throw new InvalidOperationException(
            $"{name} checksum changed between iterations.");
    }

    if (expectedChecksum is ulong expected
        && checksums[0] != expected)
    {
        throw new InvalidOperationException(
            $"{name} checksum {checksums[0]} did not match expected {expected}.");
    }

    Array.Sort(time);
    Array.Sort(allocations);
    double nanoseconds = time[time.Length / 2];
    Console.WriteLine(
        JsonSerializer.Serialize(
            new
            {
                name,
                operations,
                iterations,
                medianNanosecondsPerOperation =
                    nanoseconds,
                operationsPerSecond =
                    1_000_000_000d
                    / nanoseconds,
                allocatedBytesPerOperation =
                    allocations[
                        allocations.Length / 2],
                checksum = checksums[0],
            }));
}

static (
    long Ticks,
    long AllocatedBytes,
    ulong Checksum) BenchmarkProposalCycle(
        int operations)
{
    (RawNode node, MemoryStorage storage) =
        CreateLeader();
    byte[] payload =
        Enumerable.Range(0, 32)
            .Select(index => (byte)index)
            .ToArray();
    ulong checksum = FnvOffset;
    long allocatedBefore =
        GC.GetAllocatedBytesForCurrentThread();
    long started = Stopwatch.GetTimestamp();
    for (var operation = 0;
         operation < operations;
         operation++)
    {
        node.Propose(payload);
        Ready append = node.Ready();
        Persist(storage, append);
        checksum = Add(
            checksum,
            append.Entries[^1].Index);
        node.Advance(append);

        Ready committed = node.Ready();
        Persist(storage, committed);
        checksum = Add(
            checksum,
            committed.CommittedEntries[^1]
                .Index);
        node.Advance(committed);
        checksum = Add(
            checksum,
            storage.GetLastIndex());
    }

    return (
        Stopwatch.GetTimestamp() - started,
        GC.GetAllocatedBytesForCurrentThread()
            - allocatedBefore,
        checksum);
}

static (
    long Ticks,
    long AllocatedBytes,
    ulong Checksum) BenchmarkFollowerHeartbeat(
        int operations)
{
    RawNode node = CreateFollower();
    var heartbeat = new Message
    {
        From = 2,
        To = 1,
        Type = MessageType.MsgHeartbeat,
        Term = 1,
        Commit = 2,
        Context = ByteString.CopyFromUtf8("ctx"),
    };
    ulong checksum = FnvOffset;
    long allocatedBefore =
        GC.GetAllocatedBytesForCurrentThread();
    long started = Stopwatch.GetTimestamp();
    for (var operation = 0;
         operation < operations;
         operation++)
    {
        node.Step(heartbeat);
        Ready ready = node.Ready();
        checksum = Add(
            checksum,
            (ulong)ready.Messages.Count);
        checksum = Add(
            checksum,
            ready.HardState?.Commit ?? 0);
        node.Advance(ready);
    }

    return (
        Stopwatch.GetTimestamp() - started,
        GC.GetAllocatedBytesForCurrentThread()
            - allocatedBefore,
        checksum);
}

static (RawNode Node, MemoryStorage Storage)
    CreateLeader()
{
    MemoryStorage storage = CreateStorage([1]);
    RawNode node = RawNode.Restart(
        new RaftConfig
        {
            Id = 1,
            ElectionTick = 10,
            HeartbeatTick = 1,
            Storage = storage,
            Applied = 2,
            MaxSizePerMessage = ulong.MaxValue,
            MaxUncommittedEntriesSize =
                ulong.MaxValue,
            MaxInflightMessages = 256,
        });
    node.Campaign();
    Ready election = node.Ready();
    Persist(storage, election);
    node.Advance(election);
    Ready leadership = node.Ready();
    Persist(storage, leadership);
    node.Advance(leadership);
    Ready commit = node.Ready();
    Persist(storage, commit);
    node.Advance(commit);
    return (node, storage);
}

static RawNode CreateFollower()
{
    return RawNode.Restart(
        new RaftConfig
        {
            Id = 1,
            ElectionTick = 10,
            HeartbeatTick = 1,
            Storage = CreateStorage([1, 2]),
            Applied = 2,
        });
}

static MemoryStorage CreateStorage(
    IEnumerable<ulong> voters)
{
    var state = new ConfState();
    state.Voters.Add(voters);
    var storage = new MemoryStorage();
    storage.ApplySnapshot(
        new Snapshot
        {
            Metadata = new SnapshotMetadata
            {
                Index = 2,
                Term = 1,
                ConfState = state,
            },
        });
    storage.SetHardState(
        new HardState
        {
            Term = 1,
            Commit = 2,
        });
    return storage;
}

static void Persist(
    MemoryStorage storage,
    Ready ready)
{
    if (ready.Snapshot is not null)
    {
        storage.ApplySnapshot(ready.Snapshot);
    }

    storage.Append(ready.Entries);
    if (ready.HardState is not null)
    {
        storage.SetHardState(ready.HardState);
    }
}

static ulong Add(ulong checksum, ulong value)
{
    return unchecked(
        (checksum ^ value) * FnvPrime);
}
