namespace DbspNet.Sql.TypeSystem;

/// <summary>
/// Runtime values at SQL execution time. <c>null</c> represents SQL NULL;
/// any other <c>object</c> is a boxed value whose runtime type matches the
/// declared column's <see cref="SqlType.ClrType"/>. The compiler emits
/// delegates of type <c>Func&lt;object?[], object?&gt;</c> — rows are an
/// object array indexed positionally; scalar expressions evaluate to an
/// <c>object?</c> with NULL encoded as <c>null</c>.
/// </summary>
/// <remarks>
/// For v1 we lean on boxing for simplicity. Future optimisations: per-schema
/// generated struct rows, or a <see cref="ReadOnlySpan{T}"/>-backed row
/// representation that threads typed accessors through Expression trees.
/// See <c>docs/skipped.md</c>.
/// </remarks>
public static class SqlValue
{
    public static object? Null => null;

    /// <summary>
    /// Compare two SQL values for exact equality (non-three-valued-logic
    /// equality — used only in keys and for <c>IS [NOT] DISTINCT FROM</c>
    /// semantics later). Equal NULLs compare as equal here.
    /// </summary>
    public static bool StructurallyEqual(object? a, object? b)
    {
        if (a is null)
        {
            return b is null;
        }

        if (b is null)
        {
            return false;
        }

        return a.Equals(b);
    }

    /// <summary>Compute a hash code consistent with <see cref="StructurallyEqual"/>.</summary>
    public static int HashOf(object? value) => value is null ? 0 : value.GetHashCode();
}
