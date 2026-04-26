using System.Collections;
using DbspNet.Core.Algebra;

namespace DbspNet.Core.Collections;

/// <summary>
/// A weighted multiset (Z-set): a finite map from keys to non-zero weights
/// in an <see cref="IZRing{TSelf}"/>. Zero-weight entries are invariant —
/// the single mutation path (<see cref="ZSetBuilder{TKey,TWeight}"/>) drops
/// them automatically and public operators preserve that invariant.
/// </summary>
public sealed class ZSet<TKey, TWeight> : IEquatable<ZSet<TKey, TWeight>>, IEnumerable<KeyValuePair<TKey, TWeight>>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Dictionary<TKey, TWeight> _entries;

    // Ctor takes ownership; callers must not retain a reference to the dict.
    internal ZSet(Dictionary<TKey, TWeight> entries)
    {
        _entries = entries;
    }

    public static ZSet<TKey, TWeight> Empty { get; } = new(new Dictionary<TKey, TWeight>());

    public int Count => _entries.Count;

    public bool IsEmpty => _entries.Count == 0;

    /// <summary>
    /// Returns the weight of <paramref name="key"/>, or <c>Zero</c> if absent.
    /// </summary>
    public TWeight WeightOf(TKey key) =>
        _entries.TryGetValue(key, out var w) ? w : TWeight.Zero;

    public bool Contains(TKey key) => _entries.ContainsKey(key);

    public IEnumerable<TKey> Keys => _entries.Keys;

    public ZSet<TKey, TWeight> Plus(ZSet<TKey, TWeight> other)
    {
        ArgumentNullException.ThrowIfNull(other);
        var b = ZSetBuilder.From(_entries);
        foreach (var (k, w) in other._entries)
        {
            b.Add(k, w);
        }

        return b.Build();
    }

    public ZSet<TKey, TWeight> Minus(ZSet<TKey, TWeight> other)
    {
        ArgumentNullException.ThrowIfNull(other);
        var b = ZSetBuilder.From(_entries);
        foreach (var (k, w) in other._entries)
        {
            b.Add(k, TWeight.Negate(w));
        }

        return b.Build();
    }

    public ZSet<TKey, TWeight> Negate()
    {
        var d = new Dictionary<TKey, TWeight>(capacity: _entries.Count);
        foreach (var (k, w) in _entries)
        {
            d[k] = TWeight.Negate(w);
        }

        return new ZSet<TKey, TWeight>(d);
    }

    public ZSet<TKey, TWeight> ScalarMultiply(TWeight scalar)
    {
        if (TWeight.IsZero(scalar))
        {
            return Empty;
        }

        var d = new Dictionary<TKey, TWeight>(capacity: _entries.Count);
        foreach (var (k, w) in _entries)
        {
            var product = TWeight.Multiply(w, scalar);
            if (!TWeight.IsZero(product))
            {
                d[k] = product;
            }
        }

        return new ZSet<TKey, TWeight>(d);
    }

    public ZSet<TKey, TWeight> Filter(Func<TKey, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        var d = new Dictionary<TKey, TWeight>();
        foreach (var (k, w) in _entries)
        {
            if (predicate(k))
            {
                d[k] = w;
            }
        }

        return new ZSet<TKey, TWeight>(d);
    }

    public ZSet<TKey2, TWeight> MapKeys<TKey2>(Func<TKey, TKey2> f)
        where TKey2 : notnull
    {
        ArgumentNullException.ThrowIfNull(f);
        var b = new ZSetBuilder<TKey2, TWeight>();
        foreach (var (k, w) in _entries)
        {
            b.Add(f(k), w);
        }

        return b.Build();
    }

    /// <summary>
    /// Folds <paramref name="delta"/> into this Z-set's backing dictionary
    /// in place, preserving the zero-is-absent invariant. Used by
    /// <c>ZSetTrace</c>; callers must not retain any reference to this
    /// instance across a merge. Runs in <c>O(|delta|)</c>.
    /// </summary>
    internal void MergeInPlace(ZSet<TKey, TWeight> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        if (delta._entries.Count == 0)
        {
            return;
        }

        foreach (var (k, dw) in delta._entries)
        {
            if (_entries.TryGetValue(k, out var current))
            {
                var sum = TWeight.Add(current, dw);
                if (TWeight.IsZero(sum))
                {
                    _entries.Remove(k);
                }
                else
                {
                    _entries[k] = sum;
                }
            }
            else
            {
                _entries[k] = dw;
            }
        }
    }

    /// <summary>
    /// Returns a shallow copy with its own backing dictionary. Used by
    /// in-place merges on <c>IndexedZSet</c> to avoid aliasing the caller's
    /// inner Z-sets.
    /// </summary>
    internal ZSet<TKey, TWeight> Clone()
    {
        return new ZSet<TKey, TWeight>(new Dictionary<TKey, TWeight>(_entries));
    }

    public IEnumerator<KeyValuePair<TKey, TWeight>> GetEnumerator() => _entries.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => _entries.GetEnumerator();

    public static ZSet<TKey, TWeight> operator +(ZSet<TKey, TWeight> a, ZSet<TKey, TWeight> b) => a.Plus(b);

    public static ZSet<TKey, TWeight> operator -(ZSet<TKey, TWeight> a, ZSet<TKey, TWeight> b) => a.Minus(b);

    public static ZSet<TKey, TWeight> operator -(ZSet<TKey, TWeight> a) => a.Negate();

    public bool Equals(ZSet<TKey, TWeight>? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        if (_entries.Count != other._entries.Count)
        {
            return false;
        }

        foreach (var (k, w) in _entries)
        {
            if (!other._entries.TryGetValue(k, out var w2) || !w.Equals(w2))
            {
                return false;
            }
        }

        return true;
    }

    public override bool Equals(object? obj) => Equals(obj as ZSet<TKey, TWeight>);

    public override int GetHashCode()
    {
        // Order-independent hash: XOR per-entry hashes. Acceptable since
        // this type is rarely used as a dictionary key.
        var hash = 0;
        foreach (var (k, w) in _entries)
        {
            hash ^= HashCode.Combine(k, w);
        }

        return hash;
    }

    public override string ToString()
    {
        if (_entries.Count == 0)
        {
            return "{}";
        }

        var parts = new List<string>(_entries.Count);
        foreach (var (k, w) in _entries)
        {
            parts.Add($"{k} => {w}");
        }

        parts.Sort(StringComparer.Ordinal);
        return "{" + string.Join(", ", parts) + "}";
    }
}

/// <summary>
/// Static helpers for constructing <see cref="ZSet{TKey,TWeight}"/>.
/// </summary>
public static class ZSet
{
    public static ZSet<TKey, TWeight> Empty<TKey, TWeight>()
        where TKey : notnull
        where TWeight : struct, IZRing<TWeight>
        => ZSet<TKey, TWeight>.Empty;

    public static ZSet<TKey, TWeight> Singleton<TKey, TWeight>(TKey key, TWeight weight)
        where TKey : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        if (TWeight.IsZero(weight))
        {
            return ZSet<TKey, TWeight>.Empty;
        }

        return new ZSet<TKey, TWeight>(new Dictionary<TKey, TWeight> { [key] = weight });
    }

    public static ZSet<TKey, TWeight> FromEntries<TKey, TWeight>(IEnumerable<KeyValuePair<TKey, TWeight>> entries)
        where TKey : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(entries);
        var b = new ZSetBuilder<TKey, TWeight>();
        foreach (var (k, w) in entries)
        {
            b.Add(k, w);
        }

        return b.Build();
    }

    public static ZSet<TKey, TWeight> FromEntries<TKey, TWeight>(IEnumerable<(TKey Key, TWeight Weight)> entries)
        where TKey : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(entries);
        var b = new ZSetBuilder<TKey, TWeight>();
        foreach (var (k, w) in entries)
        {
            b.Add(k, w);
        }

        return b.Build();
    }

    /// <summary>
    /// Creates a Z-set from a set of keys, each with weight <c>One</c>.
    /// Duplicates accumulate.
    /// </summary>
    public static ZSet<TKey, TWeight> FromKeys<TKey, TWeight>(IEnumerable<TKey> keys)
        where TKey : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(keys);
        var b = new ZSetBuilder<TKey, TWeight>();
        foreach (var k in keys)
        {
            b.Add(k, TWeight.One);
        }

        return b.Build();
    }
}
