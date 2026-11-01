// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using System.Runtime.CompilerServices;
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
    // A Dictionary, or a ChunkedMap (the default; DBSPNET_ZSET_MAP=dictionary opts out) (docs/design-chunked-deltas.md).
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

    // ---- Cold backing (docs/design-durable-state.md §1f, increment 4: lazy restore) ----
    //
    // A restored trace starts with an empty in-memory map and a cold source (the runs it was
    // checkpointed to). A key is read from cold the first time it is asked for and then lives in
    // the map like any other; Resolved remembers which keys have been read, so a key absent from
    // cold, or emptied since, is never read twice. Point reads (GroupFor, ContainsKey) and
    // MergeInPlace resolve one key; anything that needs every group (enumeration, Keys, equality,
    // GC) promotes the rest first, so it stays correct, just eager. GroupCount stays exact without
    // reading: the source knows its count, and every resolved key's truth is in the map.
    //
    // A side table, not a field: an indexed Z-set is no bigger than before, and until some
    // restore has gone lazy in this process the only cost is one static read on a map miss.
    private sealed class Cold(IColdGroups<TKey, TValue, TWeight> source)
    {
        public IColdGroups<TKey, TValue, TWeight> Source { get; } = source;

        public HashSet<TKey> Resolved { get; } = new();

        /// <summary>Resolved keys that cold held (so are no longer counted as cold-only).</summary>
        public int ResolvedPresent { get; set; }
    }

    private static readonly ConditionalWeakTable<IndexedZSet<TKey, TValue, TWeight>, Cold> ColdTable = new();
    private static volatile bool _anyCold;

    private Cold? ColdState => _anyCold && ColdTable.TryGetValue(this, out var c) ? c : null;

    /// <summary>An empty indexed Z-set whose groups are read from <paramref name="source"/> on demand.</summary>
    internal static IndexedZSet<TKey, TValue, TWeight> ColdBacked(IColdGroups<TKey, TValue, TWeight> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        var z = NewMutable();
        _anyCold = true;
        var cold = new Cold(source);
        ColdTable.Add(z, cold);
        foreach (var key in source.PreloadKeys)
        {
            z.Resolve(cold, key);
        }

        source.ReleasePreload();
        return z;
    }

    /// <summary>True while some groups have not been read from the cold source.</summary>
    internal bool IsCold => ColdState is not null;

    /// <summary>Keys read from cold so far (diagnostics and tests).</summary>
    internal int ColdResolvedCount => ColdState?.Resolved.Count ?? 0;

    /// <summary>
    /// If this is empty and not cold, and <paramref name="other"/> is cold-backed, take its source
    /// and any groups it has already read (a restored trace adopting what the codec loaded) instead
    /// of reading it all.
    /// </summary>
    internal bool TryAdoptColdFrom(IndexedZSet<TKey, TValue, TWeight> other)
    {
        if (!_anyCold || ColdState is not null || Groups.Count != 0 || other.ColdState is not { } cold)
        {
            return false;
        }

        // Groups other already read (a preload) come along; the cold source covers the rest.
        foreach (var (key, group) in other.Groups)
        {
            Groups.Set(key, group);
        }

        ColdTable.Remove(other);
        ColdTable.Add(this, cold);
        return true;
    }

    // Read key from cold if it has not been; true if it is now in the map.
    private bool Resolve(Cold cold, TKey key)
    {
        if (!cold.Resolved.Add(key))
        {
            return false;
        }

        var t0 = System.Diagnostics.Stopwatch.GetTimestamp();
        var g = cold.Source.Read(key);
        ColdStats.ReadTime(System.Diagnostics.Stopwatch.GetTimestamp() - t0);
        ColdStats.Resolve(!g.IsEmpty);
        if (g.IsEmpty)
        {
            return false;
        }

        Groups.Set(key, g);
        cold.ResolvedPresent++;
        return true;
    }

    // Every group, read into the map: what a whole-collection read needs first.
    private MapView<TKey, ZSet<TValue, TWeight>> All
    {
        get
        {
            if (ColdState is { } cold)
            {
                ColdStats.Promote(cold.Source.GroupCount);
                var t0 = System.Diagnostics.Stopwatch.GetTimestamp();
                foreach (var (key, group) in cold.Source.ReadAll())
                {
                    if (!cold.Resolved.Contains(key) && !group.IsEmpty)
                    {
                        Groups.Set(key, group);
                    }
                }

                ColdStats.PromoteTime(System.Diagnostics.Stopwatch.GetTimestamp() - t0);

                ColdTable.Remove(this);
            }

            return Groups;
        }
    }

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
            foreach (var group in All.Values)
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
        foreach (var (key, group) in All)
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

    public int GroupCount => ColdState is { } cold ? cold.Source.GroupCount - cold.ResolvedPresent + Groups.Count : Groups.Count;

    public bool IsEmpty => GroupCount == 0;

    public IEnumerable<TKey> Keys => All.Keys;

    public ZSet<TValue, TWeight> GroupFor(TKey key)
    {
        var found = Groups.TryGetValue(key, out var g)
            || (ColdState is { } cold && Resolve(cold, key) && Groups.TryGetValue(key, out g));
        if (TraceAccessProfile.Enabled && TraceAccessProfile.Counting)
        {
            TraceAccessProfile.For(this)?.Probe(key, found);
        }

        return found ? g! : ZSet<TValue, TWeight>.Empty;
    }

    public bool ContainsKey(TKey key)
    {
        var found = Groups.ContainsKey(key) || (ColdState is { } cold && Resolve(cold, key));
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

        return new Enumerator(All.GetEnumerator());
    }

    IEnumerator<KeyValuePair<TKey, ZSet<TValue, TWeight>>>
        IEnumerable<KeyValuePair<TKey, ZSet<TValue, TWeight>>>.GetEnumerator() => new Enumerator(All.GetEnumerator());

    IEnumerator IEnumerable.GetEnumerator() => new Enumerator(All.GetEnumerator());

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

        if (All.Count != other.All.Count)
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
        foreach (var (k, g) in All)
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
        foreach (var g in All.Values)
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
        foreach (var (k, g) in All)
        {
            foreach (var (v, w) in g)
            {
                b.Add(k, v, w);
            }
        }

        foreach (var (k, g) in other.All)
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
        foreach (var (k, g) in All)
        {
            foreach (var (v, w) in g)
            {
                b.Add(k, v, w);
            }
        }

        foreach (var (k, g) in other.All)
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
        if (delta.All.Count == 0)
        {
            return;
        }

        var cold = ColdState;
        foreach (var (key, deltaGroup) in delta.Groups)
        {
            // A key this has not read from cold yet: read it, so the delta adds to what was there.
            if (Groups.TryGetValue(key, out var currentGroup)
                || (cold is not null && Resolve(cold, key) && Groups.TryGetValue(key, out currentGroup)))
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
        foreach (var key in All.Keys)
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
    // A Dictionary, or a ChunkedMap (the default; DBSPNET_ZSET_MAP=dictionary opts out); null after Build(). One field.
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
