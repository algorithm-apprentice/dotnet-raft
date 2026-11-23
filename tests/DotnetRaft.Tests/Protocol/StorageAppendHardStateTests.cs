using DotnetRaft.Protocol;

namespace DotnetRaft.Tests.Protocol;

public sealed class StorageAppendHardStateTests
{
    [Fact]
    public void ReadDistinguishesAbsentPartialAndExplicitZeroUpdates()
    {
        var append = new Message
        {
            Type = MessageType.MsgStorageAppend,
            To = RaftLocalMessageTargets.AppendThread,
        };

        Assert.Null(StorageAppendHardState.Read(append));

        append.Term = 5;
        Assert.Throws<ArgumentException>(() => StorageAppendHardState.Read(append));

        append.Vote = 0;
        append.Commit = 0;
        Assert.Equal(new RaftHardState(5, 0, 0), StorageAppendHardState.Read(append));
    }
}
