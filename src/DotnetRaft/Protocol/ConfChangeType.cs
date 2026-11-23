namespace DotnetRaft.Protocol;

public enum ConfChangeType
{
    ConfChangeAddNode,
    ConfChangeRemoveNode,
    ConfChangeUpdateNode,
    ConfChangeAddLearnerNode,
}
