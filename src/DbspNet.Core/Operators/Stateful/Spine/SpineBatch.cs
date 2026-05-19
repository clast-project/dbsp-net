using Clast.BloomFilter;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// A sealed spine batch. Conceptually a sorted columnar
/// <see cref="ZSet{TKey,TWeight}"/> snapshot plus a per-batch bloom
/// filter; concretely either fully resident in memory
/// (<see cref="ResidentSpineBatch{TKey,TWeight}"/>) or spilled to
/// disk with only the bloom retained
/// (<see cref="SpilledSpineBatch{TKey,TWeight}"/>).
/// </summary>
/// <remarks>
/// The bloom always stays in RAM: cache-line probe (~10 ns), 1 % FPP,
/// and it's the mechanism that lets spilled batches skip the disk
/// entirely on most probes.
/// </remarks>
internal abstract class SpineBatch<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    protected const double TargetFpp = 0.01;
    protected const int MaxBloomBytes = 1 << 16;

    public BloomFilter<TKey>? Bloom { get; protected init; }

    public bool IsEmpty => Count == 0;

    public abstract int Count { get; }

    public abstract TWeight WeightOf(TKey key);

    /// <summary>Enumerates entries in sorted key order.</summary>
    public abstract IEnumerable<KeyValuePair<TKey, TWeight>> Entries();

    /// <summary>
    /// Pairwise sorted merge of the supplied batches into a fresh
    /// resident batch. Inputs may be resident or spilled — spilled
    /// inputs are loaded transiently via their codec. Matching keys
    /// have their weights summed and zero-sum entries dropped.
    /// </summary>
    public static ResidentSpineBatch<TKey, TWeight> Merge(
        IReadOnlyList<SpineBatch<TKey, TWeight>> batches, IComparer<TKey> comparer)
    {
        if (batches.Count == 0)
        {
            return ResidentSpineBatch<TKey, TWeight>.Empty(comparer);
        }

        var result = batches[0].Materialise(comparer);
        for (var i = 1; i < batches.Count; i++)
        {
            var next = batches[i].Materialise(comparer);
            result = ResidentSpineBatch<TKey, TWeight>.MergePair(result, next, comparer);
        }

        return result;
    }

    /// <summary>
    /// Returns a resident form of this batch — the identity for
    /// resident batches, a transient load for spilled ones.
    /// </summary>
    protected internal abstract ResidentSpineBatch<TKey, TWeight> Materialise(IComparer<TKey> comparer);

    protected static BloomFilter<TKey>? BuildBloom(TKey[] keys)
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
/// In-memory spine batch — the sorted columnar pair (keys, weights)
/// plus the comparer used to maintain order. WeightOf is a
/// bloom-gated binary search.
/// </summary>
internal sealed class ResidentSpineBatch<TKey, TWeight> : SpineBatch<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly TKey[] _keys;
    private readonly TWeight[] _weights;
    private readonly IComparer<TKey> _comparer;

    internal TKey[] Keys => _keys;

    internal TWeight[] Weights => _weights;

    internal IComparer<TKey> Comparer => _comparer;

    private ResidentSpineBatch(TKey[] keys, TWeight[] weights, IComparer<TKey> comparer)
    {
        _keys = keys;
        _weights = weights;
        _comparer = comparer;
        Bloom = BuildBloom(keys);
    }

    public override int Count => _keys.Length;

    public override TWeight WeightOf(TKey key)
    {
        if (Bloom is not null && !Bloom.MightContain(key))
        {
            return TWeight.Zero;
        }

        var idx = Array.BinarySearch(_keys, key, _comparer);
        return idx >= 0 ? _weights[idx] : TWeight.Zero;
    }

    public override IEnumerable<KeyValuePair<TKey, TWeight>> Entries()
    {
        for (var i = 0; i < _keys.Length; i++)
        {
            yield return new KeyValuePair<TKey, TWeight>(_keys[i], _weights[i]);
        }
    }

    protected internal override ResidentSpineBatch<TKey, TWeight> Materialise(IComparer<TKey> comparer) => this;

    public static ResidentSpineBatch<TKey, TWeight> Empty(IComparer<TKey> comparer) =>
        new(Array.Empty<TKey>(), Array.Empty<TWeight>(), comparer);

    public static ResidentSpineBatch<TKey, TWeight> FromZSet(ZSet<TKey, TWeight> data, IComparer<TKey> comparer)
    {
        if (data.IsEmpty)
        {
            return Empty(comparer);
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
        return new ResidentSpineBatch<TKey, TWeight>(keys, weights, comparer);
    }

    /// <summary>Reconstructs a resident batch from its sorted columnar pair without re-sorting.</summary>
    internal static ResidentSpineBatch<TKey, TWeight> FromSortedArrays(
        TKey[] keys, TWeight[] weights, IComparer<TKey> comparer)
    {
        return new ResidentSpineBatch<TKey, TWeight>(keys, weights, comparer);
    }

    internal static ResidentSpineBatch<TKey, TWeight> MergePair(
        ResidentSpineBatch<TKey, TWeight> a, ResidentSpineBatch<TKey, TWeight> b, IComparer<TKey> comparer)
    {
        var aKeys = a._keys;
        var aWeights = a._weights;
        var bKeys = b._keys;
        var bWeights = b._weights;

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

        return new ResidentSpineBatch<TKey, TWeight>(keys, weights, comparer);
    }
}

/// <summary>
/// Spilled spine batch — bloom + count retained in RAM, key/weight
/// data lives on disk. Probes that pass the bloom (or methods that
/// need the data: <see cref="Entries"/>, compaction) load the batch
/// transiently via its codec and discard after use.
/// </summary>
internal sealed class SpilledSpineBatch<TKey, TWeight> : SpineBatch<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly ITableFileSystem _fileSystem;
    private readonly string _filePath;
    private readonly IZSetTraceCodec<TKey, TWeight> _codec;
    private readonly IComparer<TKey> _comparer;
    private readonly int _count;

    internal string FilePath => _filePath;

    internal SpilledSpineBatch(
        ITableFileSystem fileSystem,
        string filePath,
        IZSetTraceCodec<TKey, TWeight> codec,
        IComparer<TKey> comparer,
        BloomFilter<TKey>? bloom,
        int count)
    {
        _fileSystem = fileSystem;
        _filePath = filePath;
        _codec = codec;
        _comparer = comparer;
        _count = count;
        Bloom = bloom;
    }

    public override int Count => _count;

    public override TWeight WeightOf(TKey key)
    {
        if (Bloom is not null && !Bloom.MightContain(key))
        {
            return TWeight.Zero;
        }

        var resident = LoadResident();
        return resident.WeightOf(key);
    }

    public override IEnumerable<KeyValuePair<TKey, TWeight>> Entries() => LoadResident().Entries();

    protected internal override ResidentSpineBatch<TKey, TWeight> Materialise(IComparer<TKey> comparer) => LoadResident();

    /// <summary>Deletes the on-disk file backing this batch.</summary>
    public ValueTask DeleteAsync(CancellationToken cancellationToken = default) =>
        _fileSystem.DeleteAsync(_filePath, cancellationToken);

    private ResidentSpineBatch<TKey, TWeight> LoadResident()
    {
        // Sync block on the codec read. The InMemoryTableFileSystem
        // path is synchronous internally; LocalTableFileSystem performs
        // a single bounded disk read. If this turns into a hot path we
        // can revisit by threading async through the trace.
        var ctx = new SpillContext(_fileSystem);
        var loadTask = _codec.LoadAsync(ctx, _filePath, default);
        var loaded = loadTask.IsCompletedSuccessfully ? loadTask.Result : loadTask.AsTask().GetAwaiter().GetResult();
        return ResidentSpineBatch<TKey, TWeight>.FromZSet(loaded, _comparer);
    }
}

/// <summary>
/// Indexed-trace counterpart of <see cref="SpineBatch{TKey,TWeight}"/>.
/// </summary>
internal abstract class SpineIndexedBatch<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    protected const double TargetFpp = 0.01;
    protected const int MaxBloomBytes = 1 << 16;

    public BloomFilter<TKey>? Bloom { get; protected init; }

    public bool IsEmpty => GroupCount == 0;

    public abstract int GroupCount { get; }

    public abstract ZSet<TValue, TWeight> GroupFor(TKey key);

    public bool MightContain(TKey key) => Bloom is null || Bloom.MightContain(key);

    public abstract IEnumerable<KeyValuePair<TKey, ZSet<TValue, TWeight>>> Entries();

    public static ResidentSpineIndexedBatch<TKey, TValue, TWeight> Merge(
        IReadOnlyList<SpineIndexedBatch<TKey, TValue, TWeight>> batches,
        IComparer<TKey> keyComparer, IComparer<TValue> valueComparer)
    {
        if (batches.Count == 0)
        {
            return ResidentSpineIndexedBatch<TKey, TValue, TWeight>.Empty(keyComparer, valueComparer);
        }

        var result = batches[0].MaterialiseIndexed(keyComparer, valueComparer);
        for (var i = 1; i < batches.Count; i++)
        {
            var next = batches[i].MaterialiseIndexed(keyComparer, valueComparer);
            result = ResidentSpineIndexedBatch<TKey, TValue, TWeight>.MergePair(
                result, next, keyComparer, valueComparer);
        }

        return result;
    }

    protected internal abstract ResidentSpineIndexedBatch<TKey, TValue, TWeight> MaterialiseIndexed(
        IComparer<TKey> keyComparer, IComparer<TValue> valueComparer);

    protected static BloomFilter<TKey>? BuildBloom(TKey[] keys)
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

internal sealed class ResidentSpineIndexedBatch<TKey, TValue, TWeight> : SpineIndexedBatch<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly TKey[] _keys;
    private readonly int[] _offsets;
    private readonly TValue[] _values;
    private readonly TWeight[] _weights;
    private readonly IComparer<TKey> _keyComparer;
    private readonly IComparer<TValue> _valueComparer;

    private ResidentSpineIndexedBatch(
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

    public override int GroupCount => _keys.Length;

    public override ZSet<TValue, TWeight> GroupFor(TKey key)
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

    public override IEnumerable<KeyValuePair<TKey, ZSet<TValue, TWeight>>> Entries()
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

    protected internal override ResidentSpineIndexedBatch<TKey, TValue, TWeight> MaterialiseIndexed(
        IComparer<TKey> keyComparer, IComparer<TValue> valueComparer) => this;

    public static ResidentSpineIndexedBatch<TKey, TValue, TWeight> Empty(
        IComparer<TKey> keyComparer, IComparer<TValue> valueComparer) =>
        new(Array.Empty<TKey>(), new int[] { 0 }, Array.Empty<TValue>(), Array.Empty<TWeight>(),
            keyComparer, valueComparer);

    public static ResidentSpineIndexedBatch<TKey, TValue, TWeight> FromIndexed(
        IndexedZSet<TKey, TValue, TWeight> data,
        IComparer<TKey> keyComparer, IComparer<TValue> valueComparer)
    {
        if (data.IsEmpty)
        {
            return Empty(keyComparer, valueComparer);
        }

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

        return new ResidentSpineIndexedBatch<TKey, TValue, TWeight>(
            keys, offsets, values, weights, keyComparer, valueComparer);
    }

    internal static ResidentSpineIndexedBatch<TKey, TValue, TWeight> MergePair(
        ResidentSpineIndexedBatch<TKey, TValue, TWeight> a,
        ResidentSpineIndexedBatch<TKey, TValue, TWeight> b,
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

        return new ResidentSpineIndexedBatch<TKey, TValue, TWeight>(
            keysOut.ToArray(),
            offsetsOut.ToArray(),
            valuesOut.ToArray(),
            weightsOut.ToArray(),
            keyComparer, valueComparer);
    }

    private static void AppendGroup(
        ResidentSpineIndexedBatch<TKey, TValue, TWeight> src, int keyIndex,
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
        ResidentSpineIndexedBatch<TKey, TValue, TWeight> a, int ai,
        ResidentSpineIndexedBatch<TKey, TValue, TWeight> b, int bi,
        IComparer<TValue> valueComparer,
        List<TKey> keys, List<int> offsets, List<TValue> values, List<TWeight> weights)
    {
        var aStart = a._offsets[ai];
        var aEnd = a._offsets[ai + 1];
        var bStart = b._offsets[bi];
        var bEnd = b._offsets[bi + 1];

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
            return;
        }

        keys.Add(a._keys[ai]);
        offsets.Add(emittedStart + emitted);
    }
}

internal sealed class SpilledSpineIndexedBatch<TKey, TValue, TWeight> : SpineIndexedBatch<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly ITableFileSystem _fileSystem;
    private readonly string _filePath;
    private readonly IIndexedZSetTraceCodec<TKey, TValue, TWeight> _codec;
    private readonly IComparer<TKey> _keyComparer;
    private readonly IComparer<TValue> _valueComparer;
    private readonly int _groupCount;

    internal string FilePath => _filePath;

    internal SpilledSpineIndexedBatch(
        ITableFileSystem fileSystem,
        string filePath,
        IIndexedZSetTraceCodec<TKey, TValue, TWeight> codec,
        IComparer<TKey> keyComparer,
        IComparer<TValue> valueComparer,
        BloomFilter<TKey>? bloom,
        int groupCount)
    {
        _fileSystem = fileSystem;
        _filePath = filePath;
        _codec = codec;
        _keyComparer = keyComparer;
        _valueComparer = valueComparer;
        _groupCount = groupCount;
        Bloom = bloom;
    }

    public override int GroupCount => _groupCount;

    public override ZSet<TValue, TWeight> GroupFor(TKey key)
    {
        if (Bloom is not null && !Bloom.MightContain(key))
        {
            return ZSet<TValue, TWeight>.Empty;
        }

        return LoadResident().GroupFor(key);
    }

    public override IEnumerable<KeyValuePair<TKey, ZSet<TValue, TWeight>>> Entries() => LoadResident().Entries();

    protected internal override ResidentSpineIndexedBatch<TKey, TValue, TWeight> MaterialiseIndexed(
        IComparer<TKey> keyComparer, IComparer<TValue> valueComparer) => LoadResident();

    public ValueTask DeleteAsync(CancellationToken cancellationToken = default) =>
        _fileSystem.DeleteAsync(_filePath, cancellationToken);

    private ResidentSpineIndexedBatch<TKey, TValue, TWeight> LoadResident()
    {
        var ctx = new SpillContext(_fileSystem);
        var loadTask = _codec.LoadAsync(ctx, _filePath, default);
        var loaded = loadTask.IsCompletedSuccessfully ? loadTask.Result : loadTask.AsTask().GetAwaiter().GetResult();
        return ResidentSpineIndexedBatch<TKey, TValue, TWeight>.FromIndexed(loaded, _keyComparer, _valueComparer);
    }
}

/// <summary>
/// Thin <see cref="Circuit.ISnapshotWriter"/> +
/// <see cref="Circuit.ISnapshotReader"/> adapter over an
/// <see cref="ITableFileSystem"/>. Used both by spilled batches
/// (reads) and by the trace's spill machinery (writes); the
/// "filename" passed to a codec call is the full file-system path so
/// no prefixing is applied.
/// </summary>
internal sealed class SpillContext : Circuit.ISnapshotWriter, Circuit.ISnapshotReader
{
    private readonly ITableFileSystem _fs;

    public SpillContext(ITableFileSystem fs) { _fs = fs; }

    public ValueTask<ISequentialFile> CreateAsync(string filename, CancellationToken cancellationToken = default)
        => _fs.CreateAsync(filename, overwrite: true, cancellationToken);

    public ValueTask<IRandomAccessFile> OpenReadAsync(string filename, CancellationToken cancellationToken = default)
        => _fs.OpenReadAsync(filename, cancellationToken);

    public ValueTask<bool> ExistsAsync(string filename, CancellationToken cancellationToken = default)
        => _fs.ExistsAsync(filename, cancellationToken);
}
