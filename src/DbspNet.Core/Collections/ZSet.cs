// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using System.Runtime.CompilerServices;
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
    // A Dictionary, or an InlineEntry holding at most one entry (a zero Weight means empty,
    // per the zero-is-absent invariant). Inline exists for the per-key groups of indexed
    // traces, most of which hold one row: a one-entry dictionary costs the group four
    // objects (Z-set, dictionary, buckets, entries) where inline costs two, and a bulk load
    // retains millions of them. One field either way, so a dictionary-backed Z-set (every
    // delta) is no bigger than before; telling them apart is an exact type check against a
    // sealed class.
    private object _store;

    private sealed class InlineEntry(TKey key, TWeight weight)
    {
        public TKey Key = key;
        public TWeight Weight = weight;
    }

    // Ctor takes ownership; callers must not retain a reference to the dict.
    internal ZSet(Dictionary<TKey, TWeight> entries)
    {
        _store = entries;
        if (DeltaSizeProfile.Enabled)
        {
            DeltaSizeProfile.Record(entries);
        }
    }

    // Ctor takes ownership; the chunked twin of the Dictionary ctor (docs/design-chunked-deltas.md).
    internal ZSet(ChunkedMap<TKey, TWeight> entries)
    {
        _store = entries;
        if (DeltaSizeProfile.Enabled)
        {
            DeltaSizeProfile.Record(entries);
        }
    }

    private ZSet(TKey key, TWeight weight)
    {
        _store = new InlineEntry(key, weight);
    }

    /// <summary>An empty Z-set to mutate in place (a trace's state), in the configured map.</summary>
    internal static ZSet<TKey, TWeight> NewMutable() =>
        ZSetMapMode.Chunked ? new(new ChunkedMap<TKey, TWeight>()) : new(new Dictionary<TKey, TWeight>());

    /// <summary>
    /// Approximate heap bytes of this Z-set's own objects (itself and its storage, not the keys):
    /// for the trace-shape census. 64-bit layout: object header 16 B; Dictionary ~80 B plus a
    /// bucket int per slot and an entry (hash, next, key, value) per slot.
    /// </summary>
    internal long ContainerBytes()
    {
        const long Self = 24;
        if (Chunked is { } c)
        {
            return Self + 64 + c.ArrayBytes;
        }

        if (Dict is not { } d)
        {
            return Self + 16 + (2 * IntPtr.Size);
        }

        long capacity = d.EnsureCapacity(0);
        var entry = 8 + Unsafe.SizeOf<TKey>() + Unsafe.SizeOf<TWeight>();
        return Self + 80 + (24 + (4 * capacity)) + (24 + (entry * capacity));
    }

    /// <summary>The backing dictionary, or null when inline or chunked.</summary>
    private Dictionary<TKey, TWeight>? Dict =>
        _store.GetType() == typeof(Dictionary<TKey, TWeight>) ? Unsafe.As<Dictionary<TKey, TWeight>>(_store) : null;

    /// <summary>The backing chunked map, or null (docs/design-chunked-deltas.md).</summary>
    private ChunkedMap<TKey, TWeight>? Chunked =>
        _store.GetType() == typeof(ChunkedMap<TKey, TWeight>) ? Unsafe.As<ChunkedMap<TKey, TWeight>>(_store) : null;

    private bool IsInline => _store.GetType() == typeof(InlineEntry);

    // The inline entry's fields; valid only while Dict is null.
    private ref TKey OneKey => ref Unsafe.As<InlineEntry>(_store).Key;

    private ref TWeight OneWeight => ref Unsafe.As<InlineEntry>(_store).Weight;

    // Registered in a side table only when DBSPNET_TRACE_ACCESS_PROFILE is set and a trace marks
    // this instance as its state (docs/design-incremental-persistence.md §11). No field here: one
    // would cost 8 bytes on every Z-set, which the w1profile B/ev instrument picks up.
    internal void MarkTraceState() =>
        TraceAccessProfile.Mark(this, "flat", () => Count, entries: CensusEntries);

    private IEnumerable<object> CensusEntries()
    {
        foreach (var entry in Entries())
        {
            yield return entry.Key;
        }
    }

    public static ZSet<TKey, TWeight> Empty { get; } = new(new Dictionary<TKey, TWeight>());

    /// <summary>A one-entry Z-set held inline (no dictionary); empty for a zero weight.</summary>
    internal static ZSet<TKey, TWeight> Inline(TKey key, TWeight weight) =>
        TWeight.IsZero(weight) ? new(default!, TWeight.Zero) : new(key, weight);

    public int Count => Dict?.Count ?? Chunked?.Count ?? (TWeight.IsZero(OneWeight) ? 0 : 1);

    public bool IsEmpty => Dict is { } d ? d.Count == 0 : Chunked is { } c ? c.Count == 0 : TWeight.IsZero(OneWeight);

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
        foreach (var (_, w) in Entries())
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
        var found = TryGetWeight(key, out var w);
        if (TraceAccessProfile.Enabled && TraceAccessProfile.Counting)
        {
            TraceAccessProfile.For(this)?.Probe(key, found);
        }

        return found ? w : TWeight.Zero;
    }

    public bool Contains(TKey key)
    {
        var found = TryGetWeight(key, out _);
        if (TraceAccessProfile.Enabled && TraceAccessProfile.Counting)
        {
            TraceAccessProfile.For(this)?.Probe(key, found);
        }

        return found;
    }

    public IEnumerable<TKey> Keys =>
        Dict is { } d ? d.Keys : Chunked is { } c ? c.Keys : TWeight.IsZero(OneWeight) ? Array.Empty<TKey>() : [OneKey];

    private bool TryGetWeight(TKey key, out TWeight weight)
    {
        if (Dict is { } d)
        {
            return d.TryGetValue(key, out weight);
        }

        if (Chunked is { } c)
        {
            return c.TryGetValue(key, out weight);
        }

        if (!TWeight.IsZero(OneWeight) && EqualityComparer<TKey>.Default.Equals(OneKey, key))
        {
            weight = OneWeight;
            return true;
        }

        weight = TWeight.Zero;
        return false;
    }

    private ZSetBuilder<TKey, TWeight> ToBuilder(int extra)
    {
        if (Dict is { } d)
        {
            return ZSetBuilder.From(d);
        }

        if (Chunked is { } c)
        {
            return new ZSetBuilder<TKey, TWeight>(ChunkedMap<TKey, TWeight>.CopyOf(c));
        }

        var b = new ZSetBuilder<TKey, TWeight>(1 + extra);
        if (!TWeight.IsZero(OneWeight))
        {
            b.Add(OneKey, OneWeight);
        }

        return b;
    }

    public ZSet<TKey, TWeight> Plus(ZSet<TKey, TWeight> other)
    {
        ArgumentNullException.ThrowIfNull(other);
        var b = ToBuilder(other.Count);
        foreach (var (k, w) in other.Entries())
        {
            b.Add(k, w);
        }

        return b.Build();
    }

    public ZSet<TKey, TWeight> Minus(ZSet<TKey, TWeight> other)
    {
        ArgumentNullException.ThrowIfNull(other);
        var b = ToBuilder(other.Count);
        foreach (var (k, w) in other.Entries())
        {
            b.Add(k, TWeight.Negate(w));
        }

        return b.Build();
    }

    public ZSet<TKey, TWeight> Negate()
    {
        if (ZSetMapMode.Chunked)
        {
            var m = new ChunkedMap<TKey, TWeight>(Count);
            foreach (var (k, w) in Entries())
            {
                m[k] = TWeight.Negate(w);
            }

            return new ZSet<TKey, TWeight>(m);
        }

        var d = new Dictionary<TKey, TWeight>(capacity: Count);
        foreach (var (k, w) in Entries())
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

        if (ZSetMapMode.Chunked)
        {
            var m = new ChunkedMap<TKey, TWeight>(Count);
            foreach (var (k, w) in Entries())
            {
                var product = TWeight.Multiply(w, scalar);
                if (!TWeight.IsZero(product))
                {
                    m[k] = product;
                }
            }

            return new ZSet<TKey, TWeight>(m);
        }

        var d = new Dictionary<TKey, TWeight>(capacity: Count);
        foreach (var (k, w) in Entries())
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
        if (ZSetMapMode.Chunked)
        {
            var m = new ChunkedMap<TKey, TWeight>();
            foreach (var (k, w) in Entries())
            {
                if (predicate(k))
                {
                    m[k] = w;
                }
            }

            return new ZSet<TKey, TWeight>(m);
        }

        var d = new Dictionary<TKey, TWeight>();
        foreach (var (k, w) in Entries())
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
        var b = new ZSetBuilder<TKey2, TWeight>(Count);
        foreach (var (k, w) in Entries())
        {
            b.Add(f(k), w);
        }

        return b.Build();
    }

    /// <summary>
    /// Folds <paramref name="delta"/> into this Z-set in place, preserving the
    /// zero-is-absent invariant. Used by <c>ZSetTrace</c> and indexed-trace groups;
    /// callers must not retain any reference to this instance across a merge. Runs in
    /// <c>O(|delta|)</c>. An inline Z-set stays inline while the result has at most one
    /// key, and moves to a dictionary when a second key arrives.
    /// </summary>
    internal void MergeInPlace(ZSet<TKey, TWeight> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        if (delta.IsEmpty)
        {
            return;
        }

        if (IsInline)
        {
            if (delta.Count == 1)
            {
                var (k, dw) = delta.Single();
                if (TWeight.IsZero(OneWeight))
                {
                    OneKey = k;
                    OneWeight = dw;
                    return;
                }

                if (EqualityComparer<TKey>.Default.Equals(OneKey, k))
                {
                    OneWeight = TWeight.Add(OneWeight, dw);
                    if (TWeight.IsZero(OneWeight))
                    {
                        OneKey = default!;
                    }

                    return;
                }
            }

            if (ZSetMapMode.Chunked)
            {
                var chunkedPromoted = new ChunkedMap<TKey, TWeight>(1 + delta.Count);
                if (!TWeight.IsZero(OneWeight))
                {
                    chunkedPromoted[OneKey] = OneWeight;
                }

                _store = chunkedPromoted;
            }
            else
            {
                var promoted = new Dictionary<TKey, TWeight>(1 + delta.Count);
                if (!TWeight.IsZero(OneWeight))
                {
                    promoted[OneKey] = OneWeight;
                }

                _store = promoted;
            }
        }

        if (Chunked is { } map)
        {
            foreach (var (k, dw) in delta.Entries())
            {
                if (map.TryGetValue(k, out var current))
                {
                    var sum = TWeight.Add(current, dw);
                    if (TWeight.IsZero(sum))
                    {
                        map.Remove(k);
                    }
                    else
                    {
                        map[k] = sum;
                    }
                }
                else
                {
                    map[k] = dw;
                }
            }

            return;
        }

        var entries = Dict!;
        foreach (var (k, dw) in delta.Entries())
        {
            if (entries.TryGetValue(k, out var current))
            {
                var sum = TWeight.Add(current, dw);
                if (TWeight.IsZero(sum))
                {
                    entries.Remove(k);
                }
                else
                {
                    entries[k] = sum;
                }
            }
            else
            {
                entries[k] = dw;
            }
        }
    }

    // The only entry of a one-entry Z-set.
    private KeyValuePair<TKey, TWeight> Single()
    {
        if (Chunked is { } c)
        {
            foreach (var kv in c)
            {
                return kv;
            }

            throw new InvalidOperationException("empty Z-set");
        }

        if (Dict is not { } d)
        {
            return new(OneKey, OneWeight);
        }

        foreach (var kv in d)
        {
            return kv;
        }

        throw new InvalidOperationException("empty Z-set");
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
        if (Chunked is { } map)
        {
            List<TKey>? dropped = null;
            foreach (var key in map.Keys)
            {
                if (monotoneKey(key) < threshold)
                {
                    (dropped ??= new List<TKey>()).Add(key);
                }
            }

            foreach (var key in dropped ?? [])
            {
                map.Remove(key);
            }

            return dropped?.Count ?? 0;
        }

        var entries = Dict;
        if (entries is null)
        {
            if (TWeight.IsZero(OneWeight) || monotoneKey(OneKey) >= threshold)
            {
                return 0;
            }

            OneKey = default!;
            OneWeight = TWeight.Zero;
            return 1;
        }

        List<TKey>? removed = null;
        foreach (var key in entries.Keys)
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
            entries.Remove(key);
        }

        return removed.Count;
    }

    /// <summary>
    /// Returns a shallow copy with its own storage. Used by in-place merges on
    /// <c>IndexedZSet</c> to avoid aliasing the caller's inner Z-sets — which makes it
    /// where a trace's new group is born, so a one-entry copy is made inline.
    /// </summary>
    internal ZSet<TKey, TWeight> Clone()
    {
        if (IsInline || Count == 1)
        {
            var (k, w) = IsEmpty ? new KeyValuePair<TKey, TWeight>(default!, TWeight.Zero) : Single();
            return Inline(k, w);
        }

        return Chunked is { } c
            ? new ZSet<TKey, TWeight>(ChunkedMap<TKey, TWeight>.CopyOf(c))
            : new ZSet<TKey, TWeight>(new Dictionary<TKey, TWeight>(Dict!));
    }

    /// <summary>
    /// A struct enumerator over either representation, returned by value so
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
    /// allocation floor. The abstraction that lets a sorted run stand in this
    /// type's place is <see cref="IMultiset{TKey,TWeight}"/>, not this method.
    /// </remarks>
    public Enumerator GetEnumerator()
    {
        if (TraceAccessProfile.Enabled && TraceAccessProfile.Counting)
        {
            TraceAccessProfile.For(this)?.Scan();
        }

        return new Enumerator(this);
    }

    // Enumeration for this type's own methods: not counted as a trace scan.
    internal Enumerator Entries() => new(this);

    IEnumerator<KeyValuePair<TKey, TWeight>> IEnumerable<KeyValuePair<TKey, TWeight>>.GetEnumerator() =>
        new Enumerator(this);

    IEnumerator IEnumerable.GetEnumerator() => new Enumerator(this);

    /// <summary>Enumerates a <see cref="ZSet{TKey,TWeight}"/> in either representation.</summary>
    public struct Enumerator : IEnumerator<KeyValuePair<TKey, TWeight>>
    {
        private readonly ZSet<TKey, TWeight>? _inline;
        private readonly bool _isChunked;
        private Dictionary<TKey, TWeight>.Enumerator _dictionary;
        private ChunkedMap<TKey, TWeight>.Enumerator _chunked;
        private bool _done;

        internal Enumerator(ZSet<TKey, TWeight> set)
        {
            if (set.Dict is { } d)
            {
                _inline = null;
                _isChunked = false;
                _dictionary = d.GetEnumerator();
                _chunked = default;
                _done = false;
            }
            else if (set.Chunked is { } c)
            {
                _inline = null;
                _isChunked = true;
                _dictionary = default;
                _chunked = c.GetEnumerator();
                _done = false;
            }
            else
            {
                _inline = set;
                _isChunked = false;
                _dictionary = default;
                _chunked = default;
                _done = TWeight.IsZero(set.OneWeight);
            }
        }

        public readonly KeyValuePair<TKey, TWeight> Current =>
            _inline is not null ? new(_inline.OneKey, _inline.OneWeight)
            : _isChunked ? _chunked.Current
            : _dictionary.Current;

        readonly object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (_inline is null)
            {
                return _isChunked ? _chunked.MoveNext() : _dictionary.MoveNext();
            }

            if (_done)
            {
                return false;
            }

            _done = true;
            return true;
        }

        // Lets a method of ZSet foreach over Entries() directly.
        public readonly Enumerator GetEnumerator() => this;

        public void Dispose() => _dictionary.Dispose();

        void IEnumerator.Reset() => throw new NotSupportedException();
    }

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

        if (Count != other.Count)
        {
            return false;
        }

        foreach (var (k, w) in Entries())
        {
            if (!other.TryGetWeight(k, out var w2) || !w.Equals(w2))
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
        foreach (var (k, w) in Entries())
        {
            hash ^= HashCode.Combine(k, w);
        }

        return hash;
    }

    public override string ToString()
    {
        if (IsEmpty)
        {
            return "{}";
        }

        var parts = new List<string>(Count);
        foreach (var (k, w) in Entries())
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

        return ZSet<TKey, TWeight>.Inline(key, weight);
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
