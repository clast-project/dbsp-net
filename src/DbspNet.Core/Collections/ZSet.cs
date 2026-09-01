// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using DbspNet.Core.Algebra;

namespace DbspNet.Core.Collections;

/// <summary>
/// A weighted multiset (Z-set): a finite map from keys to non-zero weights
/// in an <see cref="IZRing{TSelf}"/>. Zero-weight entries are invariant —
/// the single mutation path (<see cref="ZSetBuilder{TKey,TWeight}"/>) drops
/// them automatically and public operators preserve that invariant.
/// </summary>
public sealed class ZSet<TKey, TWeight> : IEquatable<ZSet<TKey, TWeight>>, IMultiset<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Dictionary<TKey, TWeight> _entries;

    // Ctor takes ownership; callers must not retain a reference to the dict.
    internal ZSet(Dictionary<TKey, TWeight> entries)
    {
        _entries = entries;
    }

    // Registered in a side table only when DBSPNET_TRACE_ACCESS_PROFILE is set and a trace marks
    // this instance as its state (docs/design-incremental-persistence.md §11). No field here: one
    // would cost 8 bytes on every Z-set, which the w1profile B/ev instrument picks up.
    internal void MarkTraceState() =>
        TraceAccessProfile.Mark(this, "flat", () => _entries.Count);

    public static ZSet<TKey, TWeight> Empty { get; } = new(new Dictionary<TKey, TWeight>());

    public int Count => _entries.Count;

    public bool IsEmpty => _entries.Count == 0;

    /// <summary>
    /// Sum of every entry's weight. Z-set-linear alternative to
    /// <see cref="IsEmpty"/> when an operator needs a "group is
    /// present" check that doesn't depend on the dictionary's
    /// representation. <see cref="IsEmpty"/> (dict-shape) can
    /// disagree with <c>SumWeights == Zero</c> (linear) when the
    /// underlying multiset has cancelling entries — e.g.
    /// <c>{r1:+1, r2:-1}</c> has 2 dict entries but sum 0. Linear
    /// aggregators (per the DBSP paper §7.2-7.4 and Feldera's
    /// <c>Aggregator</c> trait contract) suppress emission iff the
    /// per-group sum of weights is zero.
    /// </summary>
    public TWeight SumWeights()
    {
        var sum = TWeight.Zero;
        foreach (var (_, w) in _entries)
        {
            sum = TWeight.Add(sum, w);
        }

        return sum;
    }

    /// <summary>
    /// Returns the weight of <paramref name="key"/>, or <c>Zero</c> if absent.
    /// </summary>
    public TWeight WeightOf(TKey key)
    {
        var found = _entries.TryGetValue(key, out var w);
        if (TraceAccessProfile.Enabled && TraceAccessProfile.Counting)
        {
            TraceAccessProfile.For(this)?.Probe(key, found);
        }

        return found ? w : TWeight.Zero;
    }

    public bool Contains(TKey key)
    {
        var found = _entries.ContainsKey(key);
        if (TraceAccessProfile.Enabled && TraceAccessProfile.Counting)
        {
            TraceAccessProfile.For(this)?.Probe(key, found);
        }

        return found;
    }

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
        // Output has at most one entry per input row (fewer only if rows collide
        // under f), so the input count is a tight upper bound — pre-size to it to
        // avoid resize churn on the common 1:1 projection (§16.7).
        var b = new ZSetBuilder<TKey2, TWeight>(_entries.Count);
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
    /// Removes every key whose <paramref name="monotoneKey"/> projection is
    /// strictly below <paramref name="threshold"/>, mutating the backing
    /// dictionary in place, and returns the number removed. The non-indexed
    /// counterpart of <see cref="IndexedZSet{TKey,TValue,TWeight}"/>'s
    /// <c>RemoveKeysBelow</c>, used by DISTINCT's frontier-driven trace GC
    /// (where the key is the whole row). A key exactly at the threshold is
    /// retained — a future input may still carry that value.
    /// </summary>
    internal int RemoveKeysBelow(long threshold, Func<TKey, long> monotoneKey)
    {
        if (TraceAccessProfile.Enabled && TraceAccessProfile.Counting)
        {
            TraceAccessProfile.For(this)?.Scan();
        }

        ArgumentNullException.ThrowIfNull(monotoneKey);
        List<TKey>? removed = null;
        foreach (var key in _entries.Keys)
        {
            if (monotoneKey(key) < threshold)
            {
                (removed ??= new List<TKey>()).Add(key);
            }
        }

        if (removed is null)
        {
            return 0;
        }

        foreach (var key in removed)
        {
            _entries.Remove(key);
        }

        return removed.Count;
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

    /// <summary>
    /// The dictionary's own struct enumerator, returned by value so
    /// <c>foreach</c> binds to it by pattern rather than through
    /// <see cref="IEnumerable{T}"/>.
    /// </summary>
    /// <remarks>
    /// Returning <c>IEnumerator&lt;&gt;</c> here would box this struct once per
    /// enumeration and turn every <c>MoveNext</c>/<c>Current</c> into an
    /// interface call — a per-row dispatch tax paid by every operator on every
    /// tick. Measured end-to-end through a fused map/filter circuit (4
    /// alternating A/B reps, M4 Pro): -13.5% wall on the many-ticks/few-rows
    /// shape (100 rows x 20k ticks), -2.3% on the wide shape (10k rows x 300
    /// ticks), inconclusive on 8-row ticks. Allocation drops by exactly 28 B
    /// per enumeration, which is 3.0% of total on the tiny shape but only
    /// 0.003% on the wide one — the dominant allocation is the fresh output
    /// dictionary, not this. So this buys back dispatch, not the Layer-A
    /// allocation floor. Exposing the concrete enumerator is deliberate: this
    /// type is the dictionary-backed multiset, and the abstraction that lets a
    /// sorted run stand in its place is <see cref="IMultiset{TKey,TWeight}"/>,
    /// not this method.
    /// </remarks>
    public Dictionary<TKey, TWeight>.Enumerator GetEnumerator()
    {
        if (TraceAccessProfile.Enabled && TraceAccessProfile.Counting)
        {
            TraceAccessProfile.For(this)?.Scan();
        }

        return _entries.GetEnumerator();
    }

    IEnumerator<KeyValuePair<TKey, TWeight>> IEnumerable<KeyValuePair<TKey, TWeight>>.GetEnumerator() =>
        _entries.GetEnumerator();

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
        // Size exactly when the source can report a count without enumerating
        // (array/list/collection — the common case at ingest); otherwise fall
        // back to growing from empty. Capacity is a pure allocation hint.
        var b = entries.TryGetNonEnumeratedCount(out var n)
            ? new ZSetBuilder<TKey, TWeight>(n)
            : new ZSetBuilder<TKey, TWeight>();
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
        var b = entries.TryGetNonEnumeratedCount(out var n)
            ? new ZSetBuilder<TKey, TWeight>(n)
            : new ZSetBuilder<TKey, TWeight>();
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
        var b = keys.TryGetNonEnumeratedCount(out var n)
            ? new ZSetBuilder<TKey, TWeight>(n)
            : new ZSetBuilder<TKey, TWeight>();
        foreach (var k in keys)
        {
            b.Add(k, TWeight.One);
        }

        return b.Build();
    }
}
