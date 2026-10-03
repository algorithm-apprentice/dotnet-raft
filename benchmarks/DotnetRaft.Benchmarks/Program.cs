using System.Diagnostics;
using System.Runtime;
using System.Text.Json;

using DotnetRaft;
using DotnetRaft.Diagnostics;
using DotnetRaft.Protocol;
using DotnetRaft.Storage;

using Google.Protobuf;

const ulong FnvOffset = 14695981039346656037UL;
const ulong FnvPrime = 1099511628211UL;

if (!GCSettings.IsServerGC)
{
    throw new InvalidOperationException(
        "DotnetRaft benchmarks require server GC.");
}

bool smoke = args.Contains(
    "--smoke",
    StringComparer.Ordinal);
int iterations = smoke
    ? 1
    : ReadIntOption(
        args,
        "--iterations",
        5);
int proposalOperations = smoke
    ? 100
    : ReadIntOption(
        args,
        "--operations",
        10_000);
int statusOperations = smoke
    ? 1_000
    : checked(proposalOperations * 10);
int descriptionOperations = statusOperations;

Run(
    "sync-proposal-cycle",
    proposalOperations,
    iterations,
    BenchmarkProposalCycle);
Run(
    "status-snapshot",
    statusOperations,
    iterations,
    BenchmarkStatus);
Run(
    "describe-message",
    descriptionOperations,
    iterations,
    BenchmarkDescription);

static void Run(
    string name,
    int operations,
    int iterations,
    Func<int, (long ElapsedTicks, ulong Checksum)>
        benchmark)
{
    _ = benchmark(operations);
    var times = new double[iterations];
    var checksums = new ulong[iterations];
    for (var iteration = 0;
         iteration < iterations;
         iteration++)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        (long ticks, ulong checksum) =
            benchmark(operations);
        times[iteration] =
            ticks
            * 1_000_000_000d
            / Stopwatch.Frequency
            / operations;
        checksums[iteration] = checksum;
    }

    if (checksums.Any(
            checksum => checksum != checksums[0]))
    {
        throw new InvalidOperationException(
            $"{name} produced inconsistent checksums.");
    }

    Array.Sort(times);
    double median = times[times.Length / 2];
    double operationsPerSecond =
        1_000_000_000d / median;
    Console.WriteLine(
        JsonSerializer.Serialize(
            new
            {
                name,
                operations,
                iterations,
                medianNanosecondsPerOperation =
                    median,
                operationsPerSecond,
                checksum = checksums[0],
            }));
}

static (
    long ElapsedTicks,
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
    long start = Stopwatch.GetTimestamp();
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

    long elapsed =
        Stopwatch.GetTimestamp() - start;
    return (elapsed, checksum);
}

static (
    long ElapsedTicks,
    ulong Checksum) BenchmarkStatus(
        int operations)
{
    (RawNode node, _) = CreateLeader();
    ulong checksum = FnvOffset;
    long start = Stopwatch.GetTimestamp();
    for (var operation = 0;
         operation < operations;
         operation++)
    {
        Status status = node.GetStatus();
        checksum = Add(
            checksum,
            status.Basic.Term);
        checksum = Add(
            checksum,
            status.Basic.Commit);
        checksum = Add(
            checksum,
            status.Basic.Applied);
        checksum = Add(
            checksum,
            (ulong)status.Progress.Count);
    }

    long elapsed =
        Stopwatch.GetTimestamp() - start;
    return (elapsed, checksum);
}

static (
    long ElapsedTicks,
    ulong Checksum) BenchmarkDescription(
        int operations)
{
    var message = new Message
    {
        From = 1,
        To = 2,
        Type = MessageType.MsgApp,
        Term = 7,
        LogTerm = 6,
        Index = 41,
        Commit = 40,
    };
    message.Entries.Add(
        new Entry
        {
            Term = 7,
            Index = 42,
            Data = ByteString.CopyFrom(
                Enumerable.Range(0, 32)
                    .Select(index => (byte)index)
                    .ToArray()),
        });
    message.Entries.Add(
        new Entry
        {
            Term = 7,
            Index = 43,
            Data =
                ByteString.CopyFromUtf8(
                    "payload-two"),
        });

    ulong checksum = FnvOffset;
    long start = Stopwatch.GetTimestamp();
    for (var operation = 0;
         operation < operations;
         operation++)
    {
        string description =
            RaftDescriptions.DescribeMessage(
                message);
        checksum = Add(
            checksum,
            (ulong)description.Length);
    }

    long elapsed =
        Stopwatch.GetTimestamp() - start;
    return (elapsed, checksum);
}

static (RawNode Node, MemoryStorage Storage)
    CreateLeader()
{
    var state = new ConfState();
    state.Voters.Add(1);
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
    var node = RawNode.Restart(
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

static int ReadIntOption(
    string[] arguments,
    string name,
    int defaultValue)
{
    int index = Array.IndexOf(arguments, name);
    if (index < 0)
    {
        return defaultValue;
    }

    if (index == arguments.Length - 1
        || !int.TryParse(
            arguments[index + 1],
            System.Globalization.NumberStyles.None,
            System.Globalization.CultureInfo.InvariantCulture,
            out int value)
        || value <= 0)
    {
        throw new ArgumentException(
            $"{name} requires a positive integer.");
    }

    return value;
}
