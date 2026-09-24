using System.Text.RegularExpressions;

namespace APIService.Core;

public static class SqlIdent
{
    private static readonly Regex IdentRegex =
        new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

    public static string Validate(string name)
    {
        if (string.IsNullOrWhiteSpace(name) || !IdentRegex.IsMatch(name))
            throw new InvalidOperationException($"Invalid SQL identifier: '{name}'");
        return name;
    }
}