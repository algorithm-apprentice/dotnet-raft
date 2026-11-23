namespace DotnetRaft.Protocol;

public enum EntryType
{
    EntryNormal,
    EntryConfChange,
    EntryConfChangeV2,
}
