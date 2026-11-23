using DotnetRaft.Tests.Interaction;

if (args is not ["preview-golden", string file])
{
    Console.Error.WriteLine("Usage: dotnet run --project tests/DotnetRaft.Tests -- preview-golden <interaction-fixture>");
    return 2;
}

Console.Write(InteractionScriptRunner.Preview(File.ReadAllText(file), file));
return 0;
