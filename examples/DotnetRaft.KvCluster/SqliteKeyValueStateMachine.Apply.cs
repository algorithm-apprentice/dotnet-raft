using DotnetRaft.Protocol;

using Google.Protobuf;

using Microsoft.Data.Sqlite;

namespace DotnetRaft.Examples.KvCluster;

public sealed partial class SqliteKeyValueStateMachine
{
    public KvApplyResult ApplyCommand(ulong index, ByteString data)
    {
        ArgumentNullException.ThrowIfNull(data);
        (KvCommand command, ByteString fingerprint) = DurableKvCommandCodec.ParseForApplication(data.Span);
        lock (gate)
        {
            ThrowIfDisposed();
            EnsureNext(physicalApplied, index);
            using SqliteTransaction transaction = connection.BeginTransaction(deferred: false);
            KvApplyResult result = ApplyCommandCore(index, index, command, fingerprint, transaction);
            CommitAppliedPrefix(index, transaction);
            return result;
        }
    }

    // The Ready consumer passes a nonempty run of ordinary entries. The
    // entire run becomes visible at commit, including its request results.
    internal IReadOnlyList<KvApplyResult> ApplyNormalEntries(IReadOnlyList<Entry> entries, int start, int end)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(start);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(end, start);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(end, entries.Count);
        var results = new List<KvApplyResult>();
        lock (gate)
        {
            ThrowIfDisposed();
            using SqliteTransaction transaction = connection.BeginTransaction(deferred: false);
            ulong appliedThrough = entries[end - 1].Index;
            ApplyBatchInTransaction(entries, start, end, appliedThrough, transaction, results);
            CommitAppliedPrefix(appliedThrough, transaction);
        }

        return results;
    }

    private void ApplyBatchInTransaction(
        IReadOnlyList<Entry> entries,
        int start,
        int end,
        ulong appliedThrough,
        SqliteTransaction transaction,
        List<KvApplyResult> results)
    {
        ulong previous = physicalApplied;
        for (int offset = start; offset < end; offset++)
        {
            Entry entry = entries[offset];
            EnsureNext(previous, entry.Index);
            if (entry.Type != EntryType.EntryNormal)
            {
                throw new InvalidDataException($"Entry {entry.Index} is not an ordinary application entry.");
            }

            if (!entry.Data.IsEmpty)
            {
                (KvCommand command, ByteString fingerprint) = DurableKvCommandCodec.ParseForApplication(entry.Data.Span);
                results.Add(ApplyCommandCore(entry.Index, appliedThrough, command, fingerprint, transaction));
            }

            previous = entry.Index;
        }
    }

    private KvApplyResult ApplyCommandCore(
        ulong index,
        ulong appliedThrough,
        KvCommand command,
        ByteString fingerprint,
        SqliteTransaction transaction)
    {
        RequestRecord? existing = ReadRequest(command.RequestId, transaction);
        if (existing is null)
        {
            ApplyMutation(command, transaction);
            InsertRequest(command.RequestId, fingerprint, index, transaction);
        }

        return new KvApplyResult(command.RequestId, existing?.Fingerprint ?? fingerprint,
            existing?.ResultIndex ?? index, appliedThrough, Duplicate: existing is not null,
            Conflict: false).ForRequest(fingerprint);
    }

    private void CommitAppliedPrefix(ulong index, SqliteTransaction transaction)
    {
        UpdateMetadata(index, confState, transaction);
        transaction.Commit();
        physicalApplied = index;
    }

    public void AdvanceNoOp(ulong index)
    {
        lock (gate)
        {
            ThrowIfDisposed();
            EnsureNext(physicalApplied, index);
            using SqliteTransaction transaction = connection.BeginTransaction(deferred: false);
            CommitAppliedPrefix(index, transaction);
        }
    }

    public void ApplyConfiguration(ulong index, ConfState configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ConfState owned = NormalizeConfState(configuration);
        lock (gate)
        {
            ThrowIfDisposed();
            EnsureNext(physicalApplied, index);
            using SqliteTransaction transaction = connection.BeginTransaction(deferred: false);
            UpdateMetadata(index, owned, transaction);
            transaction.Commit();
            confState = owned;
            physicalApplied = index;
        }
    }

    private void ApplyMutation(KvCommand command, SqliteTransaction transaction)
    {
        switch (command.Type)
        {
            case KvCommandType.Set:
                SetValue(command.Key, command.Value!, transaction);
                break;
            case KvCommandType.Delete:
                DeleteValue(command.Key, transaction);
                break;
            default:
                throw new InvalidDataException($"Unknown KV command type {command.Type}.");
        }
    }

    private void SetValue(string key, string value, SqliteTransaction transaction)
    {
        using SqliteCommand upsert = CreateCommand(
            """
            INSERT INTO kv (key, value) VALUES ($key, $value)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value;
            """, transaction);
        upsert.Parameters.AddWithValue("$key", key);
        upsert.Parameters.AddWithValue("$value", value);
        upsert.ExecuteNonQuery();
    }

    private void DeleteValue(string key, SqliteTransaction transaction)
    {
        using SqliteCommand delete = CreateCommand("DELETE FROM kv WHERE key = $key;", transaction);
        delete.Parameters.AddWithValue("$key", key);
        delete.ExecuteNonQuery();
    }
}
