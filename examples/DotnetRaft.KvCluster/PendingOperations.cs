using System.Collections.Concurrent;

using Google.Protobuf;

namespace DotnetRaft.Examples.KvCluster;

public abstract class PendingOperationRegistry
{
    private readonly ConcurrentDictionary<
        Guid,
        TaskCompletionSource<ulong>> pending = [];

    public Task<ulong> Register(Guid requestId)
    {
        if (requestId == Guid.Empty)
        {
            throw new ArgumentException(
                "Request ID must be nonempty.",
                nameof(requestId));
        }

        var completion =
            new TaskCompletionSource<ulong>(
                TaskCreationOptions
                    .RunContinuationsAsynchronously);
        if (!pending.TryAdd(
                requestId,
                completion))
        {
            throw new InvalidOperationException(
                $"Request {requestId} is already pending.");
        }

        return completion.Task;
    }

    public bool Complete(
        Guid requestId,
        ulong index)
    {
        return pending.TryRemove(
                requestId,
                out TaskCompletionSource<ulong>?
                    completion)
            && completion.TrySetResult(index);
    }

    public virtual bool Remove(Guid requestId)
    {
        return pending.TryRemove(
            requestId,
            out _);
    }

    protected bool IsPending(Guid requestId)
    {
        return pending.ContainsKey(requestId);
    }
}

public sealed class PendingProposalRegistry
{
    private readonly object gate = new();
    private readonly Dictionary<Guid, Entry>
        pending = [];

    public PendingProposalRegistration Register(
        KvCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        ByteString fingerprint =
            DurableKvCommandCodec.Fingerprint(
                command);
        lock (gate)
        {
            if (pending.TryGetValue(
                    command.RequestId,
                    out Entry? existing))
            {
                if (!existing.Fingerprint.Equals(
                        fingerprint))
                {
                    throw new KvRequestConflictException(
                        command.RequestId);
                }

                existing.Waiters++;
                return new PendingProposalRegistration(
                    this,
                    existing,
                    isOwner: false);
            }

            var entry = new Entry(
                command.RequestId,
                fingerprint);
            pending.Add(
                command.RequestId,
                entry);
            return new PendingProposalRegistration(
                this,
                entry,
                isOwner: true);
        }
    }

    public bool Complete(KvApplyResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        Entry? entry;
        lock (gate)
        {
            if (!pending.Remove(
                    result.RequestId,
                    out entry))
            {
                return false;
            }

            entry.Completed = true;
        }

        entry.SubmissionCancellation.Cancel();
        KvApplyResult completion =
            entry.Fingerprint.Equals(
                result.Fingerprint)
                ? result
                : result with
                {
                    Duplicate = false,
                    Conflict = true,
                };
        return entry.Completion.TrySetResult(
            completion);
    }

    public int CompleteResolved(
        Func<Guid, ByteString, KvApplyResult?>
            resolver)
    {
        ArgumentNullException.ThrowIfNull(resolver);
        Entry[] snapshot;
        lock (gate)
        {
            snapshot = [.. pending.Values];
        }

        var completed = 0;
        foreach (Entry entry in snapshot)
        {
            KvApplyResult? result =
                resolver(
                    entry.RequestId,
                    entry.Fingerprint);
            if (result is not null
                && Complete(result))
            {
                completed++;
            }
        }

        return completed;
    }

    private PendingProposalSubmission?
        BeginSubmission(
        Entry entry,
        bool isOwner)
    {
        lock (gate)
        {
            if (!isOwner)
            {
                throw new InvalidOperationException(
                    "Only the owning registration can submit the proposal.");
            }

            if (entry.Completed
                || !pending.TryGetValue(
                    entry.RequestId,
                    out Entry? current)
                || !ReferenceEquals(
                    current,
                    entry))
            {
                return null;
            }

            if (entry.SubmissionStarted)
            {
                throw new InvalidOperationException(
                    "Proposal submission has already started.");
            }

            entry.SubmissionStarted = true;
            return new PendingProposalSubmission(
                this,
                entry);
        }
    }

    private void FailSubmission(
        Entry entry,
        Exception exception)
    {
        ArgumentNullException.ThrowIfNull(
            exception);
        lock (gate)
        {
            if (!pending.TryGetValue(
                    entry.RequestId,
                    out Entry? current)
                || !ReferenceEquals(
                    current,
                    entry))
            {
                return;
            }

            pending.Remove(entry.RequestId);
            entry.Completed = true;
        }

        entry.SubmissionCancellation.Cancel();
        entry.Completion.TrySetException(
            exception);
    }

    private void Release(Entry entry)
    {
        var cancelSubmission = false;
        lock (gate)
        {
            if (entry.Completed)
            {
                return;
            }

            entry.Waiters--;
            if (entry.Waiters == 0
                && pending.TryGetValue(
                    entry.RequestId,
                    out Entry? current)
                && ReferenceEquals(
                    current,
                    entry))
            {
                pending.Remove(
                    entry.RequestId);
                cancelSubmission = true;
            }
        }

        if (cancelSubmission)
        {
            entry.SubmissionCancellation.Cancel();
        }
    }

    internal sealed class Entry
    {
        internal Entry(
            Guid requestId,
            ByteString fingerprint)
        {
            RequestId = requestId;
            Fingerprint = fingerprint;
        }

        internal Guid RequestId { get; }

        internal ByteString Fingerprint { get; }

        internal TaskCompletionSource<
            KvApplyResult> Completion
        {
            get;
        } = new(
            TaskCreationOptions
                .RunContinuationsAsynchronously);

        internal int Waiters { get; set; } = 1;

        internal bool Completed { get; set; }

        internal bool SubmissionStarted
        {
            get;
            set;
        }

        internal CancellationTokenSource
            SubmissionCancellation
        {
            get;
        } = new();
    }

    public sealed class PendingProposalSubmission
    {
        private readonly Entry entry;
        private readonly PendingProposalRegistry
            owner;

        internal PendingProposalSubmission(
            PendingProposalRegistry owner,
            Entry entry)
        {
            this.owner = owner;
            this.entry = entry;
        }

        public CancellationToken CancellationToken =>
            entry.SubmissionCancellation.Token;

        public void Fail(Exception exception)
        {
            owner.FailSubmission(
                entry,
                exception);
        }
    }

    public sealed class PendingProposalRegistration
        : IDisposable
    {
        private readonly PendingProposalRegistry
            owner;
        private Entry? entry;

        internal PendingProposalRegistration(
            PendingProposalRegistry owner,
            Entry entry,
            bool isOwner)
        {
            this.owner = owner;
            this.entry = entry;
            IsOwner = isOwner;
        }

        public bool IsOwner { get; }

        public Task<KvApplyResult> Task =>
            entry?.Completion.Task
            ?? throw new ObjectDisposedException(
                nameof(
                    PendingProposalRegistration));

        public PendingProposalSubmission?
            BeginSubmission()
        {
            Entry current =
                entry
                ?? throw new ObjectDisposedException(
                    nameof(
                        PendingProposalRegistration));
            return owner.BeginSubmission(
                current,
                IsOwner);
        }

        public void Dispose()
        {
            Entry? current =
                Interlocked.Exchange(
                    ref entry,
                    null);
            if (current is not null)
            {
                owner.Release(current);
            }
        }
    }
}

public sealed class PendingReadRegistry
    : PendingOperationRegistry
{
    private readonly Dictionary<Guid, ulong>
        deferred = [];
    private readonly object gate = new();

    public void CompleteOrDefer(
        Guid requestId,
        ulong index,
        ulong physicalApplied)
    {
        if (index <= physicalApplied)
        {
            Complete(requestId, index);
            return;
        }

        lock (gate)
        {
            deferred[requestId] = index;
        }

        if (!IsPending(requestId))
        {
            lock (gate)
            {
                deferred.Remove(requestId);
            }
        }
    }

    public void CompleteThrough(
        ulong physicalApplied)
    {
        KeyValuePair<Guid, ulong>[] ready;
        lock (gate)
        {
            ready =
            [
                .. deferred.Where(
                    pair =>
                        pair.Value
                        <= physicalApplied),
            ];
            foreach (KeyValuePair<Guid, ulong>
                     pair in ready)
            {
                deferred.Remove(pair.Key);
            }
        }

        foreach (KeyValuePair<Guid, ulong> pair
                 in ready)
        {
            Complete(
                pair.Key,
                pair.Value);
        }
    }

    public override bool Remove(Guid requestId)
    {
        lock (gate)
        {
            deferred.Remove(requestId);
        }

        return base.Remove(requestId);
    }
}
