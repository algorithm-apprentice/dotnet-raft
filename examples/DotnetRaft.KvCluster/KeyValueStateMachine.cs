using System.Text.Json;

using Google.Protobuf;

namespace DotnetRaft.Examples.KvCluster;

public sealed record KeyValueReadResult(
    bool Found,
    string? Value,
    ulong PhysicalApplied);

public sealed class KeyValueStateMachine
{
    private readonly object gate = new();
    private readonly Dictionary<string, string> values =
        new(StringComparer.Ordinal);
    private ulong physicalApplied;

    public ulong PhysicalApplied
    {
        get
        {
            lock (gate)
            {
                return physicalApplied;
            }
        }
    }

    public KvSetCommand ApplySet(
        ulong index,
        ByteString data)
    {
        ArgumentNullException.ThrowIfNull(data);
        KvSetCommand command =
            KvCommandCodec.Decode(data.Span);
        lock (gate)
        {
            EnsureNext(index);
            values[command.Key] = command.Value;
            physicalApplied = index;
        }

        return command;
    }

    public void AdvanceNoOp(ulong index)
    {
        lock (gate)
        {
            EnsureNext(index);
            physicalApplied = index;
        }
    }

    public void Restore(
        ulong index,
        ByteString data)
    {
        ArgumentNullException.ThrowIfNull(data);
        Dictionary<string, string> restored =
            data.IsEmpty
                ? new Dictionary<string, string>(
                    StringComparer.Ordinal)
                : JsonSerializer.Deserialize<
                      Dictionary<string, string>>(
                      data.Span)
                  ?? throw new InvalidDataException(
                      "Snapshot state is null.");

        lock (gate)
        {
            values.Clear();
            foreach ((string key, string value) in
                     restored.OrderBy(
                         pair => pair.Key,
                         StringComparer.Ordinal))
            {
                values.Add(key, value);
            }

            physicalApplied = index;
        }
    }

    public ByteString CreateSnapshotData()
    {
        lock (gate)
        {
            var sorted =
                new SortedDictionary<string, string>(
                    values,
                    StringComparer.Ordinal);
            return ByteString.CopyFrom(
                JsonSerializer.SerializeToUtf8Bytes(
                    sorted));
        }
    }

    public KeyValueReadResult ReadAtLeast(
        string key,
        ulong requiredIndex)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        lock (gate)
        {
            if (physicalApplied < requiredIndex)
            {
                throw new InvalidOperationException(
                    $"Physical application {physicalApplied} has not reached read index {requiredIndex}.");
            }

            bool found = values.TryGetValue(
                key,
                out string? value);
            return new KeyValueReadResult(
                found,
                value,
                physicalApplied);
        }
    }

    public void EnsureApplied(ulong requiredIndex)
    {
        lock (gate)
        {
            if (physicalApplied < requiredIndex)
            {
                throw new InvalidOperationException(
                    $"Physical application {physicalApplied} has not reached read index {requiredIndex}.");
            }
        }
    }

    public KeyValueReadResult ReadLocal(
        string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        lock (gate)
        {
            bool found = values.TryGetValue(
                key,
                out string? value);
            return new KeyValueReadResult(
                found,
                value,
                physicalApplied);
        }
    }

    private void EnsureNext(ulong index)
    {
        if (physicalApplied == ulong.MaxValue
            || index != physicalApplied + 1)
        {
            throw new InvalidOperationException(
                $"Committed entry index {index} does not follow physical application index {physicalApplied}.");
        }
    }
}
