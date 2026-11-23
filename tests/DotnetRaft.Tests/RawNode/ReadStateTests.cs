namespace DotnetRaft.Tests.RawNode;

public sealed class ReadStateTests
{
    [Fact]
    public void ReadContextIsDetachedAndComparedByContent()
    {
        byte[] context = [1, 0, 255];
        var state = new ReadState(7, context);
        context[2] = 9;

        var equivalent = new ReadState(7, [1, 0, 255]);
        Assert.Equal(equivalent, state);
        Assert.Equal(equivalent.GetHashCode(), state.GetHashCode());
        Assert.NotEqual(new ReadState(7, context), state);
        Assert.NotEqual(new ReadState(8, [1, 0, 255]), state);
    }
}
