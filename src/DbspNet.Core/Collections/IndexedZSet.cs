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
    // A Dictionary, or a ChunkedMap when DBSPNET_ZSET_MAP=chunked (docs/design-chunked-deltas.md).
    // One field either way, so an indexed Z-set is no bigger than before.
    private readonly object _groupStore;

    internal IndexedZSet(Dictionary<TKey, ZSet<TValue, TWeight>> groups)
    {
        _groupStore = groups;
        if (DeltaSizeProfile.Enabled)
        {
            DeltaSizeProfile.Record(groups);
        }
    }

    internal IndexedZSet(ChunkedMap<TKey, ZSet<TValue, TWeight>> groups)
    {
        _groupStore = groups;
        if (DeltaSizeProfile.Enabled)
        {
            DeltaSizeProfile.Record(groups);
        }
    }

    private MapView<TKey, ZSet<TValue, TWeight>> Groups => new(_groupStore);

    /// <summary>An empty indexed Z-set to mutate in place (a trace's state), in the configured map.</summary>
    internal static IndexedZSet<TKey, TValue, TWeight> NewMutable() =>
        ZSetMapMode.Chunked ? new(new ChunkedMap<TKey, ZSet<TValue, TWeight>>()) : new(new Dictionary<TKey, ZSet<TValue, TWeight>>());

    // Registered in a side table only when DBSPNET_TRACE_ACCESS_PROFILE is set and a trace marks
    // this instance as its state (docs/design-incremental-persistence.md §11). No field here: one
    // would cost 8 bytes on every Z-set, which the w1profile B/ev instrument picks up.
    internal void MarkTraceState() =>
        TraceAccessProfile.Mark(this, "indexed", () => Groups.Count, () =>
        {
            long values = 0, singletons = 0, bytes = 0;
            var groupsBySize = new long[6];
            var valuesBySize = new long[6];
            foreach (var group in Groups.Values)
            {
                var n = group.Count;
                values += n;
                if (n == 1)
                {
                    singletons++;
                }

                bytes += group.ContainerBytes();
                var b = TraceShape.Bucket(n);
                groupsBySize[b]++;
                valuesBySize[b] += n;
            }

            return new TraceShape(Groups.Count, values, singletons, bytes, groupsBySize, valuesBySize);
        }, CensusEntries);

    private IEnumerable<object> CensusEntries()
    {
        foreach (var (key, group) in Groups)
        {
            yield return key;
            foreach (var entry in group.Entries())
            {
                yield return entry.Key;
            }
        }
    }

    public static IndexedZSet<TKey, TValue, TWeight> Empty { get; } =
        new(new Dictionary<TKey, ZSet<TValue, TWeight>>());

    public int GroupCount => Groups.Count;

    public bool IsEmpty => Groups.Count == 0;

    public IEnumerable<TKey> Keys => Groups.Keys;

    public ZSet<TValue, TWeight> GroupFor(TKey key)
    {
        var found = Groups.TryGetValue(key, out var g);
        if (TraceAccessProfile.Enabled && TraceAccessProfile.Counting)
        {
            TraceAccessProfile.For(this)?.Probe(key, found);
        }

        return found ? g! : ZSet<TValue, TWeight>.Empty;
    }

    public bool ContainsKey(TKey key)
    {
        var found = Groups.ContainsKey(key);
        if (TraceAccessProfile.Enabled && TraceAccessProfile.Counting)
        {
            TraceAccessProfile.For(this)?.Probe(key, found);
        }

        return found;
    }

    /// <summary>
    /// A struct enumerator over the backing map, returned by value so
    /// <c>foreach</c> binds to it by pattern rather than through
    /// <see cref="IEnumerable{T}"/> — see the note on
    /// <see cref="ZSet{TKey,TWeight}.GetEnumerator"/> for the cost this avoids.
    /// </summary>
    public Enumerator GetEnumerator()
    {
        if (TraceAccessProfile.Enabled && TraceAccessProfile.Counting)
        {
            TraceAccessProfile.For(this)?.Scan();
        }

        return new Enumerator(Groups.GetEnumerator());
    }

    IEnumerator<KeyValuePair<TKey, ZSet<TValue, TWeight>>>
        IEnumerable<KeyValuePair<TKey, ZSet<TValue, TWeight>>>.GetEnumerator() => new Enumerator(Groups.GetEnumerator());

    IEnumerator IEnumerable.GetEnumerator() => new Enumerator(Groups.GetEnumerator());

    /// <summary>Enumerates the groups in either backing map, in the map's order.</summary>
    public struct Enumerator : IEnumerator<KeyValuePair<TKey, ZSet<TValue, TWeight>>>
    {
        private MapView<TKey, ZSet<TValue, TWeight>>.Enumerator _inner;

        internal Enumerator(MapView<TKey, ZSet<TValue, TWeight>>.Enumerator inner)
        {
            _inner = inner;
        }

        public readonly KeyValuePair<TKey, ZSet<TValue, TWeight>> Current => _inner.Current;

        readonly object IEnumerator.Current => Current;

        public bool MoveNext() => _inner.MoveNext();

        public void Dispose() => _inner.Dispose();

        void IEnumerator.Reset() => throw new NotSupportedException();
    }

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

        if (Groups.Count != other.Groups.Count)
        {
            return false;
        }

        foreach (var (k, g) in Groups)
        {
            if (!other.Groups.TryGetValue(k, out var g2) || !g.Equals(g2))
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
        foreach (var (k, g) in Groups)
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
        var total = 0L;
        foreach (var g in Groups.Values)
        {
            total += g.Count;
        }

        var b = new ZSetBuilder<(TKey, TValue), TWeight>((int)Math.Min(total, int.MaxValue));
        foreach (var (k, g) in Groups)
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
        foreach (var (k, g) in Groups)
        {
            foreach (var (v, w) in g)
            {
                b.Add(k, v, w);
            }
        }

        foreach (var (k, g) in other.Groups)
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
        foreach (var (k, g) in Groups)
        {
            foreach (var (v, w) in g)
            {
                b.Add(k, v, w);
            }
        }

        foreach (var (k, g) in other.Groups)
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
        if (delta.Groups.Count == 0)
        {
            return;
        }

        foreach (var (key, deltaGroup) in delta.Groups)
        {
            if (Groups.TryGetValue(key, out var currentGroup))
            {
                currentGroup.MergeInPlace(deltaGroup);
                if (currentGroup.IsEmpty)
                {
                    Groups.Remove(key);
                }
            }
            else
            {
                Groups.Set(key, deltaGroup.Clone());
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
        if (TraceAccessProfile.Enabled && TraceAccessProfile.Counting)
        {
            TraceAccessProfile.For(this)?.Scan();
        }

        ArgumentNullException.ThrowIfNull(monotoneKey);
        List<TKey>? removed = null;
        foreach (var key in Groups.Keys)
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
            Groups.Remove(key);
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
    // A Dictionary, or a ChunkedMap when DBSPNET_ZSET_MAP=chunked; null after Build(). One field.
    private object? _store;

    public IndexedZSetBuilder()
    {
        _store = MapView<TKey, ZSetBuilder<TValue, TWeight>>.NewStore();
    }

    private MapView<TKey, ZSetBuilder<TValue, TWeight>> Groups =>
        new(_store ?? throw new InvalidOperationException("IndexedZSetBuilder used after Build()."));

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
            g.Set(key, inner);
        }

        inner.Add(value, weight);
    }

    public IndexedZSet<TKey, TValue, TWeight> Build()
    {
        var raw = Groups;
        _store = null;
        if (raw.Count == 0)
        {
            return IndexedZSet<TKey, TValue, TWeight>.Empty;
        }

        var finalStore = MapView<TKey, ZSet<TValue, TWeight>>.NewStore(raw.Count);
        var final = new MapView<TKey, ZSet<TValue, TWeight>>(finalStore);
        foreach (var (k, builder) in raw)
        {
            var zset = builder.Build();
            if (!zset.IsEmpty)
            {
                final.Set(k, zset);
            }
        }

        return finalStore is Dictionary<TKey, ZSet<TValue, TWeight>> d
            ? new IndexedZSet<TKey, TValue, TWeight>(d)
            : new IndexedZSet<TKey, TValue, TWeight>((ChunkedMap<TKey, ZSet<TValue, TWeight>>)finalStore);
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
