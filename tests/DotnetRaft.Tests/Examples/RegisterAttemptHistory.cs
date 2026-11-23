namespace DotnetRaft.Tests.Examples;

// A null request ID denotes a read. An unsuccessful attempt has no result;
// returning a timeout does not bound when the mutation may take effect.
internal readonly record struct RegisterAttempt(
    int Started, int Returned, Guid? RequestId, string? Value, bool Succeeded, ulong? ResultIndex = null);

internal static class RegisterAttemptHistory
{
    internal static bool IsLinearizable(IReadOnlyList<RegisterAttempt> attempts)
    {
        var operations = new List<RegisterOperation>();
        foreach (RegisterAttempt attempt in attempts)
        {
            if (attempt.Returned <= attempt.Started
                || (attempt.ResultIndex.HasValue != (attempt.RequestId.HasValue && attempt.Succeeded))
                || attempt.ResultIndex == 0)
            {
                throw new ArgumentException("Invalid client attempt interval or result.", nameof(attempts));
            }

            if (attempt.RequestId is null && attempt.Succeeded)
            {
                operations.Add(new RegisterOperation(attempt.Started, attempt.Returned, IsRead: true, attempt.Value));
            }
        }

        foreach (IGrouping<Guid?, RegisterAttempt> retries in attempts.Where(attempt => attempt.RequestId.HasValue)
                     .GroupBy(attempt => attempt.RequestId))
        {
            string? value = retries.First().Value;
            if (retries.Any(attempt => !string.Equals(attempt.Value, value, StringComparison.Ordinal)))
            {
                throw new ArgumentException("This checker supports only identical-command retries.", nameof(attempts));
            }

            RegisterAttempt[] successes = retries.Where(attempt => attempt.Succeeded).ToArray();
            if (successes.Select(attempt => attempt.ResultIndex).Distinct().Skip(1).Any())
            {
                return false;
            }

            int? completed = successes.Length == 0 ? null : successes.Min(attempt => attempt.Returned);
            ulong? writeIndex = successes.Length == 0 ? null : successes[0].ResultIndex;
            operations.Add(new RegisterOperation(retries.Min(attempt => attempt.Started), completed, IsRead: false, value, writeIndex));
        }

        return RegisterHistory.IsLinearizable(operations);
    }
}
