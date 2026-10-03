namespace DotnetRaft.Tests.Interaction;

public sealed class InteractionScriptRunnerTests
{
    [Fact]
    public void ParserHandlesCommentsInputAndExpectedOutput()
    {
        const string script = """
            # comment

            propose-conf-change 1 transition=explicit
            v2 r3
            ----
            ok

            raft-state
            ----
            1: Leader (Voter) Term:1 Lead:1
            """;

        IReadOnlyList<InteractionScriptCase> cases =
            InteractionScriptRunner.Parse(
                script,
                "inline.txt");

        Assert.Equal(2, cases.Count);
        Assert.Equal(
            "v2 r3",
            cases[0].Input);
        Assert.Equal(
            "ok",
            cases[0].Expected);
        Assert.Equal(8, cases[1].LineNumber);
    }

    [Fact]
    public void MissingSeparatorReportsFileAndLine()
    {
        const string script = """
            campaign 1
            ok
            """;

        FormatException exception =
            Assert.Throws<FormatException>(
                () => InteractionScriptRunner.Parse(
                    script,
                    "missing.txt"));

        Assert.Contains(
            "missing.txt:1",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void MismatchReportsCaseExpectedAndActual()
    {
        const string script = """
            log-level none
            ----
            not-ok
            """;

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(
                () => InteractionScriptRunner.Run(
                    script,
                    "mismatch.txt"));

        Assert.Contains(
            "mismatch.txt case 1",
            exception.Message,
            StringComparison.Ordinal);
        Assert.Contains(
            "Expected:\nnot-ok",
            exception.Message,
            StringComparison.Ordinal);
        Assert.Contains(
            "Actual:\nok",
            exception.Message,
            StringComparison.Ordinal);
    }
}
