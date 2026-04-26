using System.Collections;
using System.Collections.Generic;

namespace DbspNet.Core.Collections;

/// <summary>
/// A fixed-arity, heterogeneous row of values with structural (by-value)
/// equality and a cached hash. Used as a key inside
/// <see cref="ZSet{TKey,TWeight}"/> and <c>IndexedZSet</c>, where correct
/// value-equality is load-bearing.
/// </summary>
/// <remarks>
/// We deliberately do NOT use <c>ValueTuple</c> (arity &lt;= 7, no schema
/// support beyond that) or <c>object[]</c> directly (reference-equality
/// default). NULL column values are represented as <c>null</c> in the array;
/// SQL three-valued-logic semantics live one layer up in the expression
/// layer.
/// <para>
/// The class is non-sealed so per-schema codecs can emit subclasses that
/// store typed fields directly and override equality to bypass the boxed
/// element walk. Subclasses MUST call <see cref="StructuralRow(int)"/> with a
/// hash that agrees with what <see cref="StructuralRow(object?[])"/> would
/// have produced for the same logical values, otherwise cross-type lookups
/// (typed key probing an untyped row, or vice versa) silently miss.
/// </para>
/// </remarks>
public class StructuralRow : IEquatable<StructuralRow>, IReadOnlyList<object?>
{
    // Default storage path; <c>null</c> in typed subclasses that hold their
    // own fields. Base methods that need the values go through the virtual
    // <see cref="Count"/> / indexer so subclass overrides flow through.
    private readonly object?[]? _values;
    private readonly int _hash;

    public StructuralRow(params object?[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        _values = values;
        _hash = ComputeHash(values);
    }

    /// <summary>
    /// Subclass constructor: registers a precomputed hash without backing
    /// <c>object?[]</c> storage. Subclasses must override <see cref="Count"/>
    /// and <see cref="this[int]"/> to surface their typed fields.
    /// </summary>
    protected StructuralRow(int hash)
    {
        _values = null;
        _hash = hash;
    }

    public static StructuralRow Of(ReadOnlySpan<object?> values)
    {
        var copy = new object?[values.Length];
        values.CopyTo(copy);
        return new StructuralRow(copy);
    }

    public virtual int Count => _values!.Length;

    public virtual object? this[int index] => _values![index];

    public virtual bool Equals(StructuralRow? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        if (_hash != other._hash)
        {
            return false;
        }

        var n = Count;
        if (n != other.Count)
        {
            return false;
        }

        for (var i = 0; i < n; i++)
        {
            if (!Equals(this[i], other[i]))
            {
                return false;
            }
        }

        return true;
    }

    public override bool Equals(object? obj) => Equals(obj as StructuralRow);

    public override int GetHashCode() => _hash;

    public IEnumerator<object?> GetEnumerator()
    {
        var n = Count;
        for (var i = 0; i < n; i++)
        {
            yield return this[i];
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public override string ToString()
    {
        var n = Count;
        var parts = new string[n];
        for (var i = 0; i < n; i++)
        {
            parts[i] = this[i] switch
            {
                null => "NULL",
                string s => "'" + s.Replace("'", "''", StringComparison.Ordinal) + "'",
                var v => v.ToString() ?? "NULL",
            };
        }

        return "(" + string.Join(", ", parts) + ")";
    }

    public static bool operator ==(StructuralRow? left, StructuralRow? right) =>
        left is null ? right is null : left.Equals(right);

    public static bool operator !=(StructuralRow? left, StructuralRow? right) => !(left == right);

    /// <summary>
    /// The canonical hash used by both the default backing-array constructor
    /// and the typed-subclass path. Includes the arity so the empty row
    /// hashes distinctly from a row of one NULL.
    /// </summary>
    public static int ComputeHash(IReadOnlyList<object?> values)
    {
        ArgumentNullException.ThrowIfNull(values);
        var hc = default(HashCode);
        hc.Add(values.Count);
        for (var i = 0; i < values.Count; i++)
        {
            hc.Add(values[i]);
        }

        return hc.ToHashCode();
    }
}
