namespace DotnetRaft.Quorum;

internal enum VoteResult : byte
{
    Pending = 1,
    Lost,
    Won,
}
