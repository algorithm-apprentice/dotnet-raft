using Google.Protobuf;

namespace DotnetRaft.Examples.KvCluster;

public sealed class PendingProposalRegistry
{
    private readonly object gate = new();
    private readonly Dictionary<Guid, Entry>
        pending = [];

    public PendingProposalRegistration Register(
        KvCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        return Register(command.RequestId, DurableKvCommandCodec.Fingerprint(command));
    }

    internal PendingProposalRegistration Register(Guid requestId, ByteString fingerprint)
    {
        lock (gate)
        {
            if (pending.TryGetValue(
                    requestId,
                    out Entry? existing))
            {
                return JoinPendingRequest(existing, fingerprint);
            }

            var entry = new Entry(
                requestId,
                fingerprint);
            pending.Add(
                requestId,
                entry);
            return new PendingProposalRegistration(
                this,
                entry,
                isOwner: true);
        }
    }

    // Called while gate is held, so joining and creating registrations remain atomic.
    private PendingProposalRegistration JoinPendingRequest(Entry entry, ByteString fingerprint)
    {
        if (!entry.Fingerprint.Equals(fingerprint))
        {
            throw new KvRequestConflictException(entry.RequestId);
        }

        entry.Waiters++;
        return new PendingProposalRegistration(this, entry, isOwner: false);
    }

    private bool IsCurrentRegistration(Entry entry) =>
        pending.TryGetValue(entry.RequestId, out Entry? current) && ReferenceEquals(current, entry);

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
        return entry.Completion.TrySetResult(
            result.ForRequest(entry.Fingerprint));
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
                || !IsCurrentRegistration(entry))
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
            if (!IsCurrentRegistration(entry))
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
                && IsCurrentRegistration(entry))
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
