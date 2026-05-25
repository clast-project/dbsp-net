// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using DbspNet.Core.Algebra;

namespace DbspNet.Core.Collections;

/// <summary>
/// A keyed collection of Z-sets: conceptually <c>Dictionary&lt;TKey, ZSet&lt;TValue, TWeight&gt;&gt;</c>.
/// The inner Z-set for any key is never empty; the outer dictionary skips
/// keys whose group weight vector is entirely zero.
/// </summary>
/// <remarks>
/// Used by bilinear operators (join) to index the probe side, and by
/// aggregate operators to hold per-group value multisets (so MIN/MAX and
/// AVG can be recomputed on retraction).
/// </remarks>
public sealed class IndexedZSet<TKey, TValue, TWeight>
    : IEquatable<IndexedZSet<TKey, TValue, TWeight>>, IEnumerable<KeyValuePair<TKey, ZSet<TValue, TWeight>>>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Dictionary<TKey, ZSet<TValue, TWeight>> _groups;

    internal IndexedZSet(Dictionary<TKey, ZSet<TValue, TWeight>> groups)
    {
        _groups = groups;
    }

    public static IndexedZSet<TKey, TValue, TWeight> Empty { get; } =
        new(new Dictionary<TKey, ZSet<TValue, TWeight>>());

    public int GroupCount => _groups.Count;

    public bool IsEmpty => _groups.Count == 0;

    public IEnumerable<TKey> Keys => _groups.Keys;

    public ZSet<TValue, TWeight> GroupFor(TKey key) =>
        _groups.TryGetValue(key, out var g) ? g : ZSet<TValue, TWeight>.Empty;

    public bool ContainsKey(TKey key) => _groups.ContainsKey(key);

    public IEnumerator<KeyValuePair<TKey, ZSet<TValue, TWeight>>> GetEnumerator() => _groups.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => _groups.GetEnumerator();

    public bool Equals(IndexedZSet<TKey, TValue, TWeight>? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        if (_groups.Count != other._groups.Count)
        {
            return false;
        }

        foreach (var (k, g) in _groups)
        {
            if (!other._groups.TryGetValue(k, out var g2) || !g.Equals(g2))
            {
                return false;
            }
        }

        return true;
    }

    public override bool Equals(object? obj) => Equals(obj as IndexedZSet<TKey, TValue, TWeight>);

    public override int GetHashCode()
    {
        var hash = 0;
        foreach (var (k, g) in _groups)
        {
            hash ^= HashCode.Combine(k, g);
        }

        return hash;
    }

    /// <summary>
    /// Flattens this indexed Z-set to a flat Z-set of <c>(key, value)</c> pairs.
    /// </summary>
    public ZSet<(TKey Key, TValue Value), TWeight> Flatten()
    {
        var b = new ZSetBuilder<(TKey, TValue), TWeight>();
        foreach (var (k, g) in _groups)
        {
            foreach (var (v, w) in g)
            {
                b.Add((k, v), w);
            }
        }

        return b.Build();
    }

    public IndexedZSet<TKey, TValue, TWeight> Plus(IndexedZSet<TKey, TValue, TWeight> other)
    {
        ArgumentNullException.ThrowIfNull(other);
        var b = new IndexedZSetBuilder<TKey, TValue, TWeight>();
        foreach (var (k, g) in _groups)
        {
            foreach (var (v, w) in g)
            {
                b.Add(k, v, w);
            }
        }

        foreach (var (k, g) in other._groups)
        {
            foreach (var (v, w) in g)
            {
                b.Add(k, v, w);
            }
        }

        return b.Build();
    }

    public IndexedZSet<TKey, TValue, TWeight> Minus(IndexedZSet<TKey, TValue, TWeight> other)
    {
        ArgumentNullException.ThrowIfNull(other);
        var b = new IndexedZSetBuilder<TKey, TValue, TWeight>();
        foreach (var (k, g) in _groups)
        {
            foreach (var (v, w) in g)
            {
                b.Add(k, v, w);
            }
        }

        foreach (var (k, g) in other._groups)
        {
            foreach (var (v, w) in g)
            {
                b.Add(k, v, TWeight.Negate(w));
            }
        }

        return b.Build();
    }

    /// <summary>
    /// Folds <paramref name="delta"/> into this indexed Z-set's backing
    /// dictionaries in place. For each (key, group) pair in the delta:
    /// merges into the existing group if present (removing the key if the
    /// merged group becomes empty), otherwise installs a clone of the
    /// delta's group (so the caller's inner Z-set is not aliased). Runs in
    /// <c>O(|delta|)</c> total — one inner merge per affected key, each
    /// proportional to that key's delta group size. Used by
    /// <c>IndexedZSetTrace</c>; callers must not retain any reference to
    /// this instance across a merge.
    /// </summary>
    internal void MergeInPlace(IndexedZSet<TKey, TValue, TWeight> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        if (delta._groups.Count == 0)
        {
            return;
        }

        foreach (var (key, deltaGroup) in delta._groups)
        {
            if (_groups.TryGetValue(key, out var currentGroup))
            {
                currentGroup.MergeInPlace(deltaGroup);
                if (currentGroup.IsEmpty)
                {
                    _groups.Remove(key);
                }
            }
            else
            {
                _groups[key] = deltaGroup.Clone();
            }
        }
    }

    /// <summary>
    /// Removes every key whose <paramref name="monotoneKey"/> projection is
    /// strictly below <paramref name="threshold"/>, mutating the backing
    /// dictionary in place, and returns the removed keys (so the caller can
    /// drop any parallel per-key state). Used by frontier-driven trace GC; a
    /// key exactly at the threshold is retained, since a future input may still
    /// carry that value.
    /// </summary>
    internal IReadOnlyList<TKey> RemoveKeysBelow(long threshold, Func<TKey, long> monotoneKey)
    {
        ArgumentNullException.ThrowIfNull(monotoneKey);
        List<TKey>? removed = null;
        foreach (var key in _groups.Keys)
        {
            if (monotoneKey(key) < threshold)
            {
                (removed ??= new List<TKey>()).Add(key);
            }
        }

        if (removed is null)
        {
            return Array.Empty<TKey>();
        }

        foreach (var key in removed)
        {
            _groups.Remove(key);
        }

        return removed;
    }

    public static IndexedZSet<TKey, TValue, TWeight> operator +(
        IndexedZSet<TKey, TValue, TWeight> a,
        IndexedZSet<TKey, TValue, TWeight> b) => a.Plus(b);

    public static IndexedZSet<TKey, TValue, TWeight> operator -(
        IndexedZSet<TKey, TValue, TWeight> a,
        IndexedZSet<TKey, TValue, TWeight> b) => a.Minus(b);
}

/// <summary>
/// Efficient incremental builder for <see cref="IndexedZSet{TKey,TValue,TWeight}"/>.
/// </summary>
public sealed class IndexedZSetBuilder<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private Dictionary<TKey, ZSetBuilder<TValue, TWeight>>? _groups;

    public IndexedZSetBuilder()
    {
        _groups = new Dictionary<TKey, ZSetBuilder<TValue, TWeight>>();
    }

    private Dictionary<TKey, ZSetBuilder<TValue, TWeight>> Groups =>
        _groups ?? throw new InvalidOperationException("IndexedZSetBuilder used after Build().");

    public void Add(TKey key, TValue value, TWeight weight)
    {
        if (TWeight.IsZero(weight))
        {
            return;
        }

        var g = Groups;
        if (!g.TryGetValue(key, out var inner))
        {
            inner = new ZSetBuilder<TValue, TWeight>();
            g[key] = inner;
        }

        inner.Add(value, weight);
    }

    public IndexedZSet<TKey, TValue, TWeight> Build()
    {
        var raw = Groups;
        _groups = null;
        if (raw.Count == 0)
        {
            return IndexedZSet<TKey, TValue, TWeight>.Empty;
        }

        var final = new Dictionary<TKey, ZSet<TValue, TWeight>>(capacity: raw.Count);
        foreach (var (k, builder) in raw)
        {
            var zset = builder.Build();
            if (!zset.IsEmpty)
            {
                final[k] = zset;
            }
        }

        return new IndexedZSet<TKey, TValue, TWeight>(final);
    }
}

/// <summary>
/// Static helpers for constructing <see cref="IndexedZSet{TKey,TValue,TWeight}"/>.
/// </summary>
public static class IndexedZSet
{
    public static IndexedZSet<TKey, TValue, TWeight> Empty<TKey, TValue, TWeight>()
        where TKey : notnull
        where TValue : notnull
        where TWeight : struct, IZRing<TWeight>
        => IndexedZSet<TKey, TValue, TWeight>.Empty;

    /// <summary>
    /// Rekeys a Z-set by extracting a key from each entry. The original
    /// row becomes the inner value.
    /// </summary>
    public static IndexedZSet<TKey, TRow, TWeight> IndexBy<TKey, TRow, TWeight>(
        ZSet<TRow, TWeight> source,
        Func<TRow, TKey> keyOf)
        where TKey : notnull
        where TRow : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(keyOf);

        var b = new IndexedZSetBuilder<TKey, TRow, TWeight>();
        foreach (var (row, w) in source)
        {
            b.Add(keyOf(row), row, w);
        }

        return b.Build();
    }
}
