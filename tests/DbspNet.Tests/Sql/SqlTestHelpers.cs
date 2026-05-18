using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Shared row-lookup helper for the SQL test suites. The compiler stores
/// VARCHAR columns as <see cref="Utf8String"/>, but tests are written
/// against .NET <see cref="string"/> literals — this hides the conversion
/// at the assertion boundary so tests stay readable.
/// </summary>
internal static class SqlTestHelpers
{
    /// <summary>
    /// Wrap any <see cref="string"/> in <paramref name="row"/> as a
    /// <see cref="Utf8String"/>. Other values pass through unchanged.
    /// Used by per-file <c>WeightOf</c> helpers — the schema is not
    /// available at the call site, so we encode any string positionally
    /// (relies on the convention that strings only appear for VARCHAR
    /// columns in tests).
    /// </summary>
    public static object?[] EncodeStrings(object?[] row)
    {
        object?[]? encoded = null;
        for (var i = 0; i < row.Length; i++)
        {
            if (row[i] is string s)
            {
                encoded ??= (object?[])row.Clone();
                encoded[i] = Utf8String.Of(s);
            }
        }

        return encoded ?? row;
    }
}
