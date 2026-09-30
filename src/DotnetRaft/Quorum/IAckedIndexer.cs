namespace DotnetRaft.Quorum;

internal interface IAckedIndexer
{
    bool TryGetAckedIndex(ulong voterId, out ulong index);
}
