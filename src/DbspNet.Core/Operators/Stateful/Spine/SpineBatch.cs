using Clast.BloomFilter;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// An immutable spine batch — sorted columnar key/weight arrays plus a
/// per-batch <see cref="BloomFilter{TKey}"/>. Probes are
/// definite-miss-skipped by the bloom, then resolved by binary search.
/// </summary>
/// <remarks>
/// <para>Sorted columnar layout mirrors Differential Dataflow's
/// <c>OrdKeyBatch</c>: a single sorted <typeparamref name="TKey"/>[]
/// alongside a parallel weight column. Compaction is a linear-time
/// k-way merge of sorted runs rather than a dict rebuild.</para>
/// <para>The bloom is sized for the batch's actual entry count at 1%
/// target FPP, capped at 64 KiB per batch. It still earns its keep on
/// top of the binary search: a cache-line bloom probe is ~10 ns vs a
/// log-N binary search that may cache-miss on larger batches.</para>
/// </remarks>
internal sealed class SpineBatch<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private const double TargetFpp = 0.01;
    private const int MaxBloomBytes = 1 << 16;

    private readonly TKey[] _keys;
    private readonly TWeight[] _weights;
    private readonly IComparer<TKey> _comparer;

    public BloomFilter<TKey>? Bloom { get; }

    private SpineBatch(TKey[] keys, TWeight[] weights, IComparer<TKey> comparer)
    {
        _keys = keys;
        _weights = weights;
        _comparer = comparer;
        Bloom = BuildBloom(keys);
    }

    public bool IsEmpty => _keys.Length == 0;

    public int Count => _keys.Length;

    public TWeight WeightOf(TKey key)
    {
        if (Bloom is not null && !Bloom.MightContain(key))
        {
            return TWeight.Zero;
        }

        var idx = Array.BinarySearch(_keys, key, _comparer);
        return idx >= 0 ? _weights[idx] : TWeight.Zero;
    }

    /// <summary>Enumerates entries in sorted key order.</summary>
    public IEnumerable<KeyValuePair<TKey, TWeight>> Entries()
    {
        for (var i = 0; i < _keys.Length; i++)
        {
            yield return new KeyValuePair<TKey, TWeight>(_keys[i], _weights[i]);
        }
    }

    /// <summary>Builds a sorted batch from a Z-set delta.</summary>
    public static SpineBatch<TKey, TWeight> FromZSet(ZSet<TKey, TWeight> data, IComparer<TKey> comparer)
    {
        if (data.IsEmpty)
        {
            return new SpineBatch<TKey, TWeight>(Array.Empty<TKey>(), Array.Empty<TWeight>(), comparer);
        }

        var n = data.Count;
        var keys = new TKey[n];
        var weights = new TWeight[n];
        var i = 0;
        foreach (var (k, w) in data)
        {
            keys[i] = k;
            weights[i] = w;
            i++;
        }

        Array.Sort(keys, weights, comparer);
        return new SpineBatch<TKey, TWeight>(keys, weights, comparer);
    }

    /// <summary>
    /// Merges <paramref name="batches"/> in order: matching keys have
    /// their weights summed, zero-sum entries are dropped, output keys
    /// remain sorted. Pairwise reduction — adequate for the small
    /// fan-in (typically 4) the tiered compaction policy emits.
    /// </summary>
    public static SpineBatch<TKey, TWeight> Merge(
        IReadOnlyList<SpineBatch<TKey, TWeight>> batches, IComparer<TKey> comparer)
    {
        if (batches.Count == 0)
        {
            return new SpineBatch<TKey, TWeight>(Array.Empty<TKey>(), Array.Empty<TWeight>(), comparer);
        }

        var result = batches[0];
        for (var i = 1; i < batches.Count; i++)
        {
            result = MergePair(result, batches[i], comparer);
        }

        return result;
    }

    private static SpineBatch<TKey, TWeight> MergePair(
        SpineBatch<TKey, TWeight> a, SpineBatch<TKey, TWeight> b, IComparer<TKey> comparer)
    {
        var aKeys = a._keys;
        var aWeights = a._weights;
        var bKeys = b._keys;
        var bWeights = b._weights;

        // Upper bound: every entry survives. Trim at the end if cancellations shrank the output.
        var keys = new TKey[aKeys.Length + bKeys.Length];
        var weights = new TWeight[aKeys.Length + bKeys.Length];
        int ai = 0, bi = 0, oi = 0;

        while (ai < aKeys.Length && bi < bKeys.Length)
        {
            var cmp = comparer.Compare(aKeys[ai], bKeys[bi]);
            if (cmp < 0)
            {
                keys[oi] = aKeys[ai];
                weights[oi] = aWeights[ai];
                oi++;
                ai++;
            }
            else if (cmp > 0)
            {
                keys[oi] = bKeys[bi];
                weights[oi] = bWeights[bi];
                oi++;
                bi++;
            }
            else
            {
                var sum = TWeight.Add(aWeights[ai], bWeights[bi]);
                if (!TWeight.IsZero(sum))
                {
                    keys[oi] = aKeys[ai];
                    weights[oi] = sum;
                    oi++;
                }

                ai++;
                bi++;
            }
        }

        while (ai < aKeys.Length)
        {
            keys[oi] = aKeys[ai];
            weights[oi] = aWeights[ai];
            oi++;
            ai++;
        }

        while (bi < bKeys.Length)
        {
            keys[oi] = bKeys[bi];
            weights[oi] = bWeights[bi];
            oi++;
            bi++;
        }

        if (oi < keys.Length)
        {
            Array.Resize(ref keys, oi);
            Array.Resize(ref weights, oi);
        }

        return new SpineBatch<TKey, TWeight>(keys, weights, comparer);
    }

    private static BloomFilter<TKey>? BuildBloom(TKey[] keys)
    {
        if (keys.Length == 0)
        {
            return null;
        }

        var builder = BloomFilterBuilder<TKey>.WithCapacity(keys.Length, TargetFpp, MaxBloomBytes);
        foreach (var k in keys)
        {
            builder.Add(k);
        }

        return builder.Build();
    }
}

/// <summary>
/// Indexed-trace counterpart of <see cref="SpineBatch{TKey,TWeight}"/>.
/// Sorted-columnar layout: outer keys are sorted; for each key, an
/// offset range into parallel value / weight arrays describes the
/// (also sorted) group.
/// </summary>
internal sealed class SpineIndexedBatch<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private const double TargetFpp = 0.01;
    private const int MaxBloomBytes = 1 << 16;

    private readonly TKey[] _keys;
    private readonly int[] _offsets;        // length _keys.Length + 1
    private readonly TValue[] _values;
    private readonly TWeight[] _weights;
    private readonly IComparer<TKey> _keyComparer;
    private readonly IComparer<TValue> _valueComparer;

    public BloomFilter<TKey>? Bloom { get; }

    private SpineIndexedBatch(
        TKey[] keys, int[] offsets, TValue[] values, TWeight[] weights,
        IComparer<TKey> keyComparer, IComparer<TValue> valueComparer)
    {
        _keys = keys;
        _offsets = offsets;
        _values = values;
        _weights = weights;
        _keyComparer = keyComparer;
        _valueComparer = valueComparer;
        Bloom = BuildBloom(keys);
    }

    public bool IsEmpty => _keys.Length == 0;

    public int GroupCount => _keys.Length;

    /// <summary>
    /// Returns true if the bloom does not rule the key out — i.e. the
    /// caller should proceed to a real lookup.
    /// </summary>
    public bool MightContain(TKey key) =>
        Bloom is null || Bloom.MightContain(key);

    /// <summary>
    /// Returns the group for <paramref name="key"/> as a Z-set, or empty
    /// if the key is absent. Caller must have already cleared the bloom
    /// via <see cref="MightContain"/> for the bloom skip to apply.
    /// </summary>
    public ZSet<TValue, TWeight> GroupFor(TKey key)
    {
        if (Bloom is not null && !Bloom.MightContain(key))
        {
            return ZSet<TValue, TWeight>.Empty;
        }

        var idx = Array.BinarySearch(_keys, key, _keyComparer);
        if (idx < 0)
        {
            return ZSet<TValue, TWeight>.Empty;
        }

        var b = new ZSetBuilder<TValue, TWeight>();
        var start = _offsets[idx];
        var end = _offsets[idx + 1];
        for (var i = start; i < end; i++)
        {
            b.Add(_values[i], _weights[i]);
        }

        return b.Build();
    }

    /// <summary>Enumerates each (key, group) pair in sorted key order.</summary>
    public IEnumerable<KeyValuePair<TKey, ZSet<TValue, TWeight>>> Entries()
    {
        for (var ki = 0; ki < _keys.Length; ki++)
        {
            var b = new ZSetBuilder<TValue, TWeight>();
            var start = _offsets[ki];
            var end = _offsets[ki + 1];
            for (var i = start; i < end; i++)
            {
                b.Add(_values[i], _weights[i]);
            }

            yield return new KeyValuePair<TKey, ZSet<TValue, TWeight>>(_keys[ki], b.Build());
        }
    }

    public static SpineIndexedBatch<TKey, TValue, TWeight> FromIndexed(
        IndexedZSet<TKey, TValue, TWeight> data,
        IComparer<TKey> keyComparer, IComparer<TValue> valueComparer)
    {
        if (data.IsEmpty)
        {
            return new SpineIndexedBatch<TKey, TValue, TWeight>(
                Array.Empty<TKey>(), new int[] { 0 },
                Array.Empty<TValue>(), Array.Empty<TWeight>(),
                keyComparer, valueComparer);
        }

        // Collect groups, sort outer keys, then within each group sort values.
        var groups = new List<(TKey Key, (TValue Value, TWeight Weight)[] Items)>(data.GroupCount);
        var totalItems = 0;
        foreach (var (k, group) in data)
        {
            var items = new (TValue Value, TWeight Weight)[group.Count];
            var i = 0;
            foreach (var (v, w) in group)
            {
                items[i++] = (v, w);
            }

            Array.Sort(items, (x, y) => valueComparer.Compare(x.Value, y.Value));
            groups.Add((k, items));
            totalItems += items.Length;
        }

        groups.Sort((x, y) => keyComparer.Compare(x.Key, y.Key));

        var keys = new TKey[groups.Count];
        var offsets = new int[groups.Count + 1];
        var values = new TValue[totalItems];
        var weights = new TWeight[totalItems];

        var cursor = 0;
        for (var gi = 0; gi < groups.Count; gi++)
        {
            keys[gi] = groups[gi].Key;
            offsets[gi] = cursor;
            foreach (var (v, w) in groups[gi].Items)
            {
                values[cursor] = v;
                weights[cursor] = w;
                cursor++;
            }
        }

        offsets[groups.Count] = cursor;

        return new SpineIndexedBatch<TKey, TValue, TWeight>(
            keys, offsets, values, weights, keyComparer, valueComparer);
    }

    /// <summary>
    /// Pairwise sorted merge. Matching outer keys merge their groups
    /// (sum colliding (value) weights, drop zeros); the result remains
    /// sorted in both dimensions. Outer keys with empty groups after
    /// merge are dropped.
    /// </summary>
    public static SpineIndexedBatch<TKey, TValue, TWeight> Merge(
        IReadOnlyList<SpineIndexedBatch<TKey, TValue, TWeight>> batches,
        IComparer<TKey> keyComparer, IComparer<TValue> valueComparer)
    {
        if (batches.Count == 0)
        {
            return new SpineIndexedBatch<TKey, TValue, TWeight>(
                Array.Empty<TKey>(), new int[] { 0 },
                Array.Empty<TValue>(), Array.Empty<TWeight>(),
                keyComparer, valueComparer);
        }

        var result = batches[0];
        for (var i = 1; i < batches.Count; i++)
        {
            result = MergePair(result, batches[i], keyComparer, valueComparer);
        }

        return result;
    }

    private static SpineIndexedBatch<TKey, TValue, TWeight> MergePair(
        SpineIndexedBatch<TKey, TValue, TWeight> a,
        SpineIndexedBatch<TKey, TValue, TWeight> b,
        IComparer<TKey> keyComparer, IComparer<TValue> valueComparer)
    {
        var aKeys = a._keys;
        var bKeys = b._keys;
        var keysOut = new List<TKey>(aKeys.Length + bKeys.Length);
        var offsetsOut = new List<int>(aKeys.Length + bKeys.Length + 1) { 0 };
        var valuesOut = new List<TValue>();
        var weightsOut = new List<TWeight>();

        int ai = 0, bi = 0;
        while (ai < aKeys.Length && bi < bKeys.Length)
        {
            var cmp = keyComparer.Compare(aKeys[ai], bKeys[bi]);
            if (cmp < 0)
            {
                AppendGroup(a, ai, keysOut, offsetsOut, valuesOut, weightsOut);
                ai++;
            }
            else if (cmp > 0)
            {
                AppendGroup(b, bi, keysOut, offsetsOut, valuesOut, weightsOut);
                bi++;
            }
            else
            {
                MergeGroupAndAppend(
                    a, ai, b, bi, valueComparer,
                    keysOut, offsetsOut, valuesOut, weightsOut);
                ai++;
                bi++;
            }
        }

        while (ai < aKeys.Length)
        {
            AppendGroup(a, ai, keysOut, offsetsOut, valuesOut, weightsOut);
            ai++;
        }

        while (bi < bKeys.Length)
        {
            AppendGroup(b, bi, keysOut, offsetsOut, valuesOut, weightsOut);
            bi++;
        }

        return new SpineIndexedBatch<TKey, TValue, TWeight>(
            keysOut.ToArray(),
            offsetsOut.ToArray(),
            valuesOut.ToArray(),
            weightsOut.ToArray(),
            keyComparer, valueComparer);
    }

    private static void AppendGroup(
        SpineIndexedBatch<TKey, TValue, TWeight> src, int keyIndex,
        List<TKey> keys, List<int> offsets, List<TValue> values, List<TWeight> weights)
    {
        var start = src._offsets[keyIndex];
        var end = src._offsets[keyIndex + 1];
        if (start == end)
        {
            return;
        }

        keys.Add(src._keys[keyIndex]);
        for (var i = start; i < end; i++)
        {
            values.Add(src._values[i]);
            weights.Add(src._weights[i]);
        }

        offsets.Add(values.Count);
    }

    private static void MergeGroupAndAppend(
        SpineIndexedBatch<TKey, TValue, TWeight> a, int ai,
        SpineIndexedBatch<TKey, TValue, TWeight> b, int bi,
        IComparer<TValue> valueComparer,
        List<TKey> keys, List<int> offsets, List<TValue> values, List<TWeight> weights)
    {
        var aStart = a._offsets[ai];
        var aEnd = a._offsets[ai + 1];
        var bStart = b._offsets[bi];
        var bEnd = b._offsets[bi + 1];

        // Sorted-merge the two value spans. Emit only non-zero sums.
        var emitted = 0;
        int p = aStart, q = bStart;
        var emittedStart = values.Count;

        while (p < aEnd && q < bEnd)
        {
            var cmp = valueComparer.Compare(a._values[p], b._values[q]);
            if (cmp < 0)
            {
                values.Add(a._values[p]);
                weights.Add(a._weights[p]);
                emitted++;
                p++;
            }
            else if (cmp > 0)
            {
                values.Add(b._values[q]);
                weights.Add(b._weights[q]);
                emitted++;
                q++;
            }
            else
            {
                var sum = TWeight.Add(a._weights[p], b._weights[q]);
                if (!TWeight.IsZero(sum))
                {
                    values.Add(a._values[p]);
                    weights.Add(sum);
                    emitted++;
                }

                p++;
                q++;
            }
        }

        while (p < aEnd)
        {
            values.Add(a._values[p]);
            weights.Add(a._weights[p]);
            emitted++;
            p++;
        }

        while (q < bEnd)
        {
            values.Add(b._values[q]);
            weights.Add(b._weights[q]);
            emitted++;
            q++;
        }

        if (emitted == 0)
        {
            // All values cancelled; rewind so this key is dropped.
            return;
        }

        keys.Add(a._keys[ai]);
        offsets.Add(emittedStart + emitted);
    }

    private static BloomFilter<TKey>? BuildBloom(TKey[] keys)
    {
        if (keys.Length == 0)
        {
            return null;
        }

        var builder = BloomFilterBuilder<TKey>.WithCapacity(keys.Length, TargetFpp, MaxBloomBytes);
        foreach (var k in keys)
        {
            builder.Add(k);
        }

        return builder.Build();
    }
}
