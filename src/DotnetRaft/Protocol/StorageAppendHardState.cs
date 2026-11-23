namespace DotnetRaft.Protocol;

/// <summary>Maps the optional state update on a local protobuf append request.</summary>
public static class StorageAppendHardState
{
    /// <summary>Reads the complete update, or null when no hard-state fields are present.</summary>
    /// <exception cref="ArgumentException">The request is not a local append or has partial state.</exception>
    public static RaftHardState? Read(Message request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Type != MessageType.MsgStorageAppend || request.To != RaftLocalMessageTargets.AppendThread)
        {
            throw new ArgumentException("Message is not a local storage-append request.", nameof(request));
        }

        if (!request.HasTerm && !request.HasVote && !request.HasCommit)
        {
            return null;
        }

        if (!request.HasTerm || !request.HasVote || !request.HasCommit)
        {
            throw new ArgumentException("Storage-append hard-state fields must be all present or all absent.", nameof(request));
        }

        return new RaftHardState(request.Term, request.Vote, request.Commit);
    }

    internal static void Write(Message request, RaftHardState state)
    {
        // Setting zero-valued fields is intentional: presence describes work.
        request.Term = state.Term;
        request.Vote = state.Vote;
        request.Commit = state.Commit;
    }
}
