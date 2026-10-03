namespace DotnetRaft.Tests.Interaction;

public sealed class InteractionDirectiveTests
{
    [Fact]
    public void QuotesAreOrdinaryTokenBytes()
    {
        InteractionDirective directive =
            InteractionDirective.Parse(
                "propose 1 \"foo bar\"",
                "quoted.txt",
                7);

        Assert.Equal("propose", directive.Command);
        Assert.Equal(
            ["1", "\"foo", "bar\""],
            directive.Arguments.Select(
                argument => argument.Key));
        Assert.All(
            directive.Arguments,
            argument => Assert.Empty(argument.Values));
    }

    [Fact]
    public void TabsDoNotSplitDirectives()
    {
        InteractionDirective directive =
            InteractionDirective.Parse(
                "propose\t1 value",
                "tabs.txt",
                2);

        Assert.Equal("propose\t1", directive.Command);
        Assert.Equal(
            ["value"],
            directive.Arguments.Select(
                argument => argument.Key));
    }

    [Fact]
    public void ParenthesizedValuesSplitOnlyAtTopLevelCommas()
    {
        InteractionDirective directive =
            InteractionDirective.Parse(
                "add-nodes 3 voters=(1, 2,(3,4),5 6)",
                "lists.txt",
                3);

        InteractionArgument voters =
            Assert.Single(
                directive.Arguments,
                argument =>
                    argument.Key == "voters");
        Assert.Equal(
            ["1", "2", "(3,4)", "5 6"],
            voters.Values);
    }

    [Theory]
    [InlineData("1", true)]
    [InlineData("t", true)]
    [InlineData("T", true)]
    [InlineData("TRUE", true)]
    [InlineData("true", true)]
    [InlineData("True", true)]
    [InlineData("0", false)]
    [InlineData("f", false)]
    [InlineData("F", false)]
    [InlineData("FALSE", false)]
    [InlineData("false", false)]
    [InlineData("False", false)]
    public void BooleanParsingMatchesGo(
        string value,
        bool expected)
    {
        Assert.Equal(
            expected,
            InteractionParsing.ParseBoolean(
                value,
                "flag"));
    }

    [Theory]
    [InlineData("yes")]
    [InlineData("2")]
    [InlineData("")]
    public void BooleanParsingRejectsOtherSpellings(
        string value)
    {
        Assert.Throws<FormatException>(
            () => InteractionParsing.ParseBoolean(
                value,
                "flag"));
    }

    [Fact]
    public void ParserReportsFileAndLine()
    {
        FormatException exception =
            Assert.Throws<FormatException>(
                () => InteractionDirective.Parse(
                    "add-nodes voters=(1,2",
                    "broken.txt",
                    19));

        Assert.Contains(
            "broken.txt:19",
            exception.Message,
            StringComparison.Ordinal);
    }
}
