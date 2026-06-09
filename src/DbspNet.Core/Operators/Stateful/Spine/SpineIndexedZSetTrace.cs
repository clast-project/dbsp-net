// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// LSM-style trace over an <see cref="IndexedZSet{TKey,TValue,TWeight}"/>.
/// Counterpart to <see cref="SpineZSetTrace{TKey,TWeight}"/> for the
/// grouped traces that <c>IncrementalAggregateOp</c> and the join
/// operators hold.
/// </summary>
/// <remarks>
/// Per-batch storage is sorted-columnar (outer keys sorted, inner
/// values sorted within each group). <see cref="GroupFor"/> on each
/// batch is a bloom-gated binary search; <see cref="Entries"/> still
/// goes through a single materialising merge so the existing snapshot
/// codec sees the same shape.
/// </remarks>
public sealed class SpineIndexedZSetTrace<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly List<List<SpineIndexedBatch<TKey, TValue, TWeight>>> _levels = new();
    private readonly ICompactionStrategy _strategy;
    private readonly IComparer<TKey> _keyComparer;
    private readonly IComparer<TValue> _valueComparer;
    private readonly SpineIndexedSpillConfig<TKey, TValue, TWeight>? _spillConfig;
    private readonly Func<TKey, long>? _monotoneKey;
    private long _spillCounter;

    // Cross-tick amortisation (docs §9.7). When _stagingCapacity > 0, per-tick
    // deltas accumulate in this mutable memtable (in-place dict merge — flat
    // cost) and flush into ONE sorted batch only when it holds ≥ that many keys,
    // instead of building a fresh batch every Integrate. When 0 the memtable is
    // never populated and every read's `!_memtable.IsEmpty` guard short-circuits,
    // so behaviour is byte-identical to the pre-memtable trace.
    private readonly int _stagingCapacity;
    private IndexedZSet<TKey, TValue, TWeight> _memtable =
        new(new Dictionary<TKey, ZSet<TValue, TWeight>>());

    public SpineIndexedZSetTrace() : this(TieredCompactionStrategy.Default, keyComparer: null, valueComparer: null)
    {
    }

    public SpineIndexedZSetTrace(
        ICompactionStrategy strategy,
        IComparer<TKey>? keyComparer = null,
        IComparer<TValue>? valueComparer = null,
        SpineIndexedSpillConfig<TKey, TValue, TWeight>? spillConfig = null,
        Func<TKey, long>? monotoneKey = null,
        int? stagingCapacity = null)
    {
        ArgumentNullException.ThrowIfNull(strategy);
        _strategy = strategy;
        _keyComparer = keyComparer ?? Comparer<TKey>.Default;
        _valueComparer = valueComparer ?? Comparer<TValue>.Default;
        _spillConfig = spillConfig;
        _monotoneKey = monotoneKey;
        _stagingCapacity = stagingCapacity ?? SpineStagingConfig.Capacity;
    }

    /// <summary>
    /// Folds <paramref name="delta"/> in by appending it as a new sorted
    /// batch at level 0, then running compaction to settlement. Empty
    /// deltas are a no-op.
    /// </summary>
    public void Integrate(IndexedZSet<TKey, TValue, TWeight> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        if (delta.IsEmpty)
        {
            return;
        }

        if (_stagingCapacity <= 0)
        {
            // Memtable disabled: one sorted batch per delta (original behaviour).
            EnsureLevel(0);
            _levels[0].Add(ResidentSpineIndexedBatch<TKey, TValue, TWeight>.FromIndexed(
                delta, _keyComparer, _valueComparer, _monotoneKey));
            RunCompaction();
            return;
        }

        // Memtable enabled: fold the delta in place (flat-dictionary cost) and
        // only materialise a sorted batch once enough keys have accumulated.
        _memtable.MergeInPlace(delta);
        if (_memtable.GroupCount >= _stagingCapacity)
        {
            FlushMemtable();
        }
    }

    /// <summary>
    /// Materialises the memtable into a single sorted batch at level 0 and
    /// compacts, then resets the memtable. No-op when the memtable is empty (so
    /// it is also safe to call before snapshot / on a disabled trace).
    /// </summary>
    private void FlushMemtable()
    {
        if (_memtable.IsEmpty)
        {
            return;
        }

        EnsureLevel(0);
        _levels[0].Add(ResidentSpineIndexedBatch<TKey, TValue, TWeight>.FromIndexed(
            _memtable, _keyComparer, _valueComparer, _monotoneKey));
        _memtable = new IndexedZSet<TKey, TValue, TWeight>(new Dictionary<TKey, ZSet<TValue, TWeight>>());
        RunCompaction();
    }

    /// <summary>
    /// Frontier-driven GC: drops every outer key whose <paramref name="monotoneKey"/>
    /// projection is strictly below <paramref name="threshold"/>, returning the
    /// removed keys so the owning operator can drop parallel per-key caches.
    /// </summary>
    /// <remarks>
    /// Per-batch dispatch on each batch's
    /// <see cref="SpineIndexedBatch{TKey,TValue,TWeight}.MinMonotoneKey"/> /
    /// <see cref="SpineIndexedBatch{TKey,TValue,TWeight}.MaxMonotoneKey"/>:
    /// whole-batch drop, keep-as-is, or load-and-mask-filter. Filtering removes
    /// whole groups (no partial-group filtering — the analyzer only flags outer
    /// keys today). Batch ordering and level layout are preserved so the
    /// compaction strategy's insertion-order assumptions stay intact. Counterpart
    /// to <see cref="SpineZSetTrace{TKey,TWeight}.DropKeysBelow"/>.
    /// </remarks>
    public IReadOnlyList<TKey> DropKeysBelow(long threshold, Func<TKey, long> monotoneKey)
    {
        ArgumentNullException.ThrowIfNull(monotoneKey);

        List<TKey>? removed = null;

        // Drop sub-frontier keys from the memtable too (it holds the most recent,
        // un-flushed deltas).
        if (!_memtable.IsEmpty)
        {
            var memRemoved = _memtable.RemoveKeysBelow(threshold, monotoneKey);
            if (memRemoved.Count > 0)
            {
                (removed ??= new List<TKey>()).AddRange(memRemoved);
            }
        }

        for (var li = 0; li < _levels.Count; li++)
        {
            var level = _levels[li];
            for (var bi = level.Count - 1; bi >= 0; bi--)
            {
                var batch = level[bi];
                var (batchMin, batchMax) = GetOrComputeMonotoneRange(batch, monotoneKey);

                if (batch.IsEmpty || batchMax < threshold)
                {
                    if (batch is SpilledSpineIndexedBatch<TKey, TValue, TWeight> spilled)
                    {
                        SyncDelete(spilled);
                    }

                    if (!batch.IsEmpty)
                    {
                        foreach (var (k, _) in batch.Entries())
                        {
                            (removed ??= new List<TKey>()).Add(k);
                        }
                    }

                    level.RemoveAt(bi);
                    continue;
                }

                if (batchMin >= threshold)
                {
                    continue;
                }

                var resident = batch.MaterialiseIndexed(_keyComparer, _valueComparer);
                var (filtered, droppedKeys) = FilterAbove(resident, threshold, monotoneKey);

                if (droppedKeys is not null)
                {
                    (removed ??= new List<TKey>()).AddRange(droppedKeys);
                }

                if (batch is SpilledSpineIndexedBatch<TKey, TValue, TWeight> spilledMixed)
                {
                    SyncDelete(spilledMixed);
                }

                if (filtered.IsEmpty)
                {
                    level.RemoveAt(bi);
                }
                else
                {
                    level[bi] = MaybeSpill(filtered, li);
                }
            }
        }

        return (IReadOnlyList<TKey>?)removed ?? Array.Empty<TKey>();
    }

    private (long Min, long Max) GetOrComputeMonotoneRange(
        SpineIndexedBatch<TKey, TValue, TWeight> batch, Func<TKey, long> monotoneKey)
    {
        if (batch.MinMonotoneKey is long min && batch.MaxMonotoneKey is long max)
        {
            return (min, max);
        }

        if (batch.IsEmpty)
        {
            return (long.MaxValue, long.MinValue);
        }

        var resident = batch.MaterialiseIndexed(_keyComparer, _valueComparer);
        var keys = resident.Keys;
        var rangeMin = monotoneKey(keys[0]);
        var rangeMax = rangeMin;
        for (var i = 1; i < keys.Length; i++)
        {
            var p = monotoneKey(keys[i]);
            if (p < rangeMin) { rangeMin = p; }
            if (p > rangeMax) { rangeMax = p; }
        }

        return (rangeMin, rangeMax);
    }

    private (ResidentSpineIndexedBatch<TKey, TValue, TWeight> Filtered, List<TKey>? Dropped) FilterAbove(
        ResidentSpineIndexedBatch<TKey, TValue, TWeight> resident, long threshold, Func<TKey, long> monotoneKey)
    {
        var keys = resident.Keys;
        var offsets = resident.Offsets;
        var values = resident.Values;
        var weights = resident.Weights;
        var keep = new bool[keys.Length];
        var keptGroups = 0;
        var keptItems = 0;
        List<TKey>? dropped = null;

        for (var ki = 0; ki < keys.Length; ki++)
        {
            if (monotoneKey(keys[ki]) >= threshold)
            {
                keep[ki] = true;
                keptGroups++;
                keptItems += offsets[ki + 1] - offsets[ki];
            }
            else
            {
                (dropped ??= new List<TKey>()).Add(keys[ki]);
            }
        }

        if (dropped is null)
        {
            return (resident, null);
        }

        if (keptGroups == 0)
        {
            return (ResidentSpineIndexedBatch<TKey, TValue, TWeight>.Empty(_keyComparer, _valueComparer), dropped);
        }

        var newKeys = new TKey[keptGroups];
        var newOffsets = new int[keptGroups + 1];
        var newValues = new TValue[keptItems];
        var newWeights = new TWeight[keptItems];

        var go = 0;
        var vo = 0;
        for (var ki = 0; ki < keys.Length; ki++)
        {
            if (!keep[ki])
            {
                continue;
            }

            newKeys[go] = keys[ki];
            newOffsets[go] = vo;
            var start = offsets[ki];
            var end = offsets[ki + 1];
            for (var i = start; i < end; i++)
            {
                newValues[vo] = values[i];
                newWeights[vo] = weights[i];
                vo++;
            }

            go++;
        }

        newOffsets[keptGroups] = vo;

        var filtered = ResidentSpineIndexedBatch<TKey, TValue, TWeight>.FromSortedArrays(
            newKeys, newOffsets, newValues, newWeights,
            _keyComparer, _valueComparer, _monotoneKey ?? monotoneKey);

        return (filtered, dropped);
    }

    /// <summary>
    /// Returns the integrated group for <paramref name="key"/> — the
    /// union of every batch's group for that key, with weights summed
    /// per value. Zero-weight values are filtered. Bloom-gated per
    /// batch; matching batches binary-search for the key.
    /// </summary>
    public ZSet<TValue, TWeight> GroupFor(TKey key)
    {
        var b = new ZSetBuilder<TValue, TWeight>();
        var any = false;
        foreach (var level in _levels)
        {
            foreach (var batch in level)
            {
                var g = batch.GroupFor(key);
                if (g.IsEmpty)
                {
                    continue;
                }

                any = true;
                foreach (var (v, w) in g)
                {
                    b.Add(v, w);
                }
            }
        }

        // The memtable holds un-flushed deltas; fold its group in too (weights
        // sum across all sources, zeros drop in Build).
        if (!_memtable.IsEmpty)
        {
            var mg = _memtable.GroupFor(key);
            if (!mg.IsEmpty)
            {
                any = true;
                foreach (var (v, w) in mg)
                {
                    b.Add(v, w);
                }
            }
        }

        return any ? b.Build() : ZSet<TValue, TWeight>.Empty;
    }

    /// <summary>
    /// Batched group probe: given <paramref name="sortedKeys"/> in ascending
    /// key order, returns the integrated group for each key that has one,
    /// merged across batches. Unlike a loop of <see cref="GroupFor"/> calls,
    /// this walks each batch's sorted outer-key column exactly once with a
    /// galloping cursor (O(D·log(N/D)) instead of D independent bloom + binary
    /// searches) and returns each group as a sorted <c>(value, weight)</c>
    /// array sliced straight from the batch columns — no per-probe
    /// <c>ZSetBuilder</c>, so value rows are never hashed. This is the
    /// merge-execution prototype from docs/design-row-representation.md §6.1.
    /// </summary>
    /// <remarks>
    /// Semantics match calling <see cref="GroupFor"/> per key and discarding
    /// the empty results: weights are summed per value across all batches and
    /// zero-weight values are dropped. Only keys with a non-empty integrated
    /// group appear in the result, in <paramref name="sortedKeys"/> order.
    /// <paramref name="sortedKeys"/> must be sorted by this trace's key comparer
    /// and hold no duplicates.
    /// </remarks>
    public List<(TKey Key, (TValue Value, TWeight Weight)[] Group)> GroupForManySorted(TKey[] sortedKeys)
    {
        ArgumentNullException.ThrowIfNull(sortedKeys);
        var result = new List<(TKey, (TValue, TWeight)[])>();
        if (sortedKeys.Length == 0)
        {
            return result;
        }

        // Per delta key, the sorted (value, weight) runs gathered from each
        // batch. Most keys live in a single batch, so the inner list stays
        // tiny and is only allocated on first match.
        var runs = new List<(TValue[] Values, TWeight[] Weights, int Start, int End)>?[sortedKeys.Length];
        var first = sortedKeys[0];
        var last = sortedKeys[sortedKeys.Length - 1];

        foreach (var level in _levels)
        {
            foreach (var batch in level)
            {
                if (batch.IsEmpty)
                {
                    continue;
                }

                var rb = batch as ResidentSpineIndexedBatch<TKey, TValue, TWeight>
                    ?? batch.MaterialiseIndexed(_keyComparer, _valueComparer);
                var keys = rb.Keys;
                if (keys.Length == 0)
                {
                    continue;
                }

                // Whole-batch range gate: skip batches whose key span can't
                // overlap the probe span (cheaper than galloping every key
                // against a disjoint batch).
                if (_keyComparer.Compare(keys[keys.Length - 1], first) < 0 ||
                    _keyComparer.Compare(keys[0], last) > 0)
                {
                    continue;
                }

                var offsets = rb.Offsets;
                var values = rb.Values;
                var weights = rb.Weights;
                var cursor = 0;
                for (var di = 0; di < sortedKeys.Length && cursor < keys.Length; di++)
                {
                    var idx = SortedKeySearch.GallopIndexOf(keys, cursor, sortedKeys[di], _keyComparer);
                    if (idx >= 0)
                    {
                        (runs[di] ??= new()).Add((values, weights, offsets[idx], offsets[idx + 1]));
                        cursor = idx + 1;
                    }
                    else
                    {
                        cursor = ~idx;
                    }
                }
            }
        }

        var memtableActive = !_memtable.IsEmpty;
        for (var di = 0; di < sortedKeys.Length; di++)
        {
            var keyRuns = runs[di];

            // Fold the memtable's group for this key in as one more sorted run,
            // so un-flushed deltas participate in the merge. A key present ONLY
            // in the memtable (no batch run) is emitted too.
            if (memtableActive)
            {
                var mg = _memtable.GroupFor(sortedKeys[di]);
                if (!mg.IsEmpty)
                {
                    (keyRuns ??= new()).Add(SortedRunFromGroup(mg));
                }
            }

            if (keyRuns is null)
            {
                continue;
            }

            var group = MergeRuns(keyRuns, _valueComparer);
            if (group.Length > 0)
            {
                result.Add((sortedKeys[di], group));
            }
        }

        return result;
    }

    /// <summary>
    /// Copies a memtable group's <c>(value, weight)</c> pairs into arrays sorted
    /// by <see cref="_valueComparer"/>, shaped as a run for <see cref="MergeRuns"/>
    /// (which requires each run sorted, distinct, and zero-free — a ZSet group is
    /// distinct and zero-free; this adds the sort).
    /// </summary>
    private (TValue[] Values, TWeight[] Weights, int Start, int End) SortedRunFromGroup(
        ZSet<TValue, TWeight> group)
    {
        var n = group.Count;
        var values = new TValue[n];
        var weights = new TWeight[n];
        var i = 0;
        foreach (var (v, w) in group)
        {
            values[i] = v;
            weights[i] = w;
            i++;
        }

        Array.Sort(values, weights, _valueComparer);
        return (values, weights, 0, n);
    }

    private static (TValue Value, TWeight Weight)[] MergeRuns(
        List<(TValue[] Values, TWeight[] Weights, int Start, int End)> runs,
        IComparer<TValue> valueComparer)
    {
        // Fast path: a key present in a single batch — its run is already
        // sorted, distinct, and zero-free, so slice it straight out.
        if (runs.Count == 1)
        {
            var (v, w, s, e) = runs[0];
            var single = new (TValue, TWeight)[e - s];
            for (var i = 0; i < single.Length; i++)
            {
                single[i] = (v[s + i], w[s + i]);
            }

            return single;
        }

        var acc = SliceToList(runs[0]);
        for (var r = 1; r < runs.Count; r++)
        {
            acc = MergeTwo(acc, runs[r], valueComparer);
        }

        return acc.ToArray();
    }

    private static List<(TValue Value, TWeight Weight)> SliceToList(
        (TValue[] Values, TWeight[] Weights, int Start, int End) run)
    {
        var list = new List<(TValue, TWeight)>(run.End - run.Start);
        for (var i = run.Start; i < run.End; i++)
        {
            list.Add((run.Values[i], run.Weights[i]));
        }

        return list;
    }

    private static List<(TValue Value, TWeight Weight)> MergeTwo(
        List<(TValue Value, TWeight Weight)> a,
        (TValue[] Values, TWeight[] Weights, int Start, int End) b,
        IComparer<TValue> valueComparer)
    {
        var merged = new List<(TValue, TWeight)>(a.Count + (b.End - b.Start));
        int i = 0, j = b.Start;
        while (i < a.Count && j < b.End)
        {
            var c = valueComparer.Compare(a[i].Value, b.Values[j]);
            if (c < 0)
            {
                merged.Add(a[i]);
                i++;
            }
            else if (c > 0)
            {
                merged.Add((b.Values[j], b.Weights[j]));
                j++;
            }
            else
            {
                var sum = TWeight.Add(a[i].Weight, b.Weights[j]);
                if (!TWeight.IsZero(sum))
                {
                    merged.Add((a[i].Value, sum));
                }

                i++;
                j++;
            }
        }

        while (i < a.Count)
        {
            merged.Add(a[i]);
            i++;
        }

        while (j < b.End)
        {
            merged.Add((b.Values[j], b.Weights[j]));
            j++;
        }

        return merged;
    }

    /// <summary>True iff every batch is empty (or the spine holds no batches).</summary>
    public bool IsEmpty
    {
        get
        {
            if (!_memtable.IsEmpty)
            {
                return false;
            }

            foreach (var level in _levels)
            {
                foreach (var batch in level)
                {
                    if (!batch.IsEmpty)
                    {
                        return false;
                    }
                }
            }

            return true;
        }
    }

    /// <summary>
    /// Enumerates <c>(key, integrated group)</c> for every key whose
    /// merged group is non-empty. Materialises a nested hash table —
    /// cost is O(total batch entries).
    /// </summary>
    public IEnumerable<KeyValuePair<TKey, ZSet<TValue, TWeight>>> Entries() =>
        MaterialiseGroups();

    /// <summary>
    /// Returns a single consolidated
    /// <see cref="IndexedZSet{TKey,TValue,TWeight}"/> equivalent to the
    /// spine's integrated state. For snapshot serialisation paths that
    /// haven't been migrated to per-batch writes, and as a test oracle.
    /// </summary>
    public IndexedZSet<TKey, TValue, TWeight> Materialize()
    {
        var b = new IndexedZSetBuilder<TKey, TValue, TWeight>();
        foreach (var (k, group) in MaterialiseGroups())
        {
            foreach (var (v, w) in group)
            {
                b.Add(k, v, w);
            }
        }

        return b.Build();
    }

    /// <summary>Total immutable batch count across all levels.</summary>
    public int BatchCount
    {
        get
        {
            var count = 0;
            foreach (var level in _levels)
            {
                count += level.Count;
            }

            return count;
        }
    }

    /// <summary>Number of levels currently allocated.</summary>
    public int LevelCount => _levels.Count;

    /// <summary>
    /// Distinct keys with a non-empty integrated group. O(total batch
    /// entries) — for tests asserting frontier-driven GC bounds the state.
    /// </summary>
    public int GroupCount => MaterialiseGroups().Count;

    /// <summary>
    /// Snapshots each non-empty batch's entries as a fresh
    /// <see cref="IndexedZSet{TKey,TValue,TWeight}"/>, level structure
    /// flattened. Used by the per-batch snapshot path.
    /// </summary>
    public IReadOnlyList<IndexedZSet<TKey, TValue, TWeight>> GetBatches()
    {
        // Snapshot must capture un-flushed deltas, so settle the memtable into a
        // batch first (no-op when the memtable is empty / disabled).
        FlushMemtable();
        var result = new List<IndexedZSet<TKey, TValue, TWeight>>(BatchCount);
        foreach (var level in _levels)
        {
            foreach (var batch in level)
            {
                if (batch.IsEmpty)
                {
                    continue;
                }

                var b = new IndexedZSetBuilder<TKey, TValue, TWeight>();
                foreach (var (k, group) in batch.Entries())
                {
                    foreach (var (v, w) in group)
                    {
                        b.Add(k, v, w);
                    }
                }

                result.Add(b.Build());
            }
        }

        return result;
    }

    internal SpineState State => SnapshotState();

    private SpineState SnapshotState()
    {
        var outer = new List<IReadOnlyList<int>>(_levels.Count);
        foreach (var level in _levels)
        {
            var inner = new int[level.Count];
            for (var i = 0; i < level.Count; i++)
            {
                inner[i] = level[i].GroupCount;
            }

            outer.Add(inner);
        }

        return new SpineState(outer);
    }

    private void EnsureLevel(int level)
    {
        while (_levels.Count <= level)
        {
            _levels.Add(new List<SpineIndexedBatch<TKey, TValue, TWeight>>());
        }
    }

    private void RunCompaction()
    {
        while (true)
        {
            var action = _strategy.NextAction(SnapshotState());
            if (action is null)
            {
                return;
            }

            Apply(action.Value);
        }
    }

    private void Apply(CompactionAction action)
    {
        if (action.SourceLevel < 0 || action.SourceLevel >= _levels.Count)
        {
            throw new InvalidOperationException(
                $"compaction strategy returned action for nonexistent level {action.SourceLevel}");
        }

        var src = _levels[action.SourceLevel];
        if (action.BatchCount <= 0 || action.BatchCount > src.Count)
        {
            throw new InvalidOperationException(
                $"compaction strategy requested merge of {action.BatchCount} batches " +
                $"at level {action.SourceLevel}, which holds {src.Count}");
        }

        var toMerge = src.GetRange(0, action.BatchCount);
        var merged = SpineIndexedBatch<TKey, TValue, TWeight>.Merge(
            toMerge, _keyComparer, _valueComparer, _monotoneKey);
        src.RemoveRange(0, action.BatchCount);

        foreach (var input in toMerge)
        {
            if (input is SpilledSpineIndexedBatch<TKey, TValue, TWeight> spilled)
            {
                SyncDelete(spilled);
            }
        }

        var destLevel = action.SourceLevel + 1;
        EnsureLevel(destLevel);
        if (!merged.IsEmpty)
        {
            _levels[destLevel].Add(MaybeSpill(merged, destLevel));
        }
    }

    private SpineIndexedBatch<TKey, TValue, TWeight> MaybeSpill(
        ResidentSpineIndexedBatch<TKey, TValue, TWeight> batch, int destLevel)
    {
        if (_spillConfig is null || destLevel < _spillConfig.MinSpillLevel)
        {
            return batch;
        }

        var path = _spillConfig.Prefix + "/batch_" +
            Interlocked.Increment(ref _spillCounter).ToString(System.Globalization.CultureInfo.InvariantCulture) +
            ".arrows";

        var iBuilder = new IndexedZSetBuilder<TKey, TValue, TWeight>();
        foreach (var (k, group) in batch.Entries())
        {
            foreach (var (v, w) in group)
            {
                iBuilder.Add(k, v, w);
            }
        }

        var iz = iBuilder.Build();
        var ctx = new SpillContext(_spillConfig.FileSystem);
        var saveTask = _spillConfig.Codec.SaveAsync(ctx, path, iz, default);
        if (!saveTask.IsCompletedSuccessfully)
        {
            saveTask.AsTask().GetAwaiter().GetResult();
        }

        return new SpilledSpineIndexedBatch<TKey, TValue, TWeight>(
            _spillConfig.FileSystem, path, _spillConfig.Codec,
            _keyComparer, _valueComparer, batch.Bloom, batch.GroupCount,
            batch.MinMonotoneKey, batch.MaxMonotoneKey);
    }

    private static void SyncDelete(SpilledSpineIndexedBatch<TKey, TValue, TWeight> spilled)
    {
        var t = spilled.DeleteAsync();
        if (!t.IsCompletedSuccessfully)
        {
            t.AsTask().GetAwaiter().GetResult();
        }
    }

    private List<KeyValuePair<TKey, ZSet<TValue, TWeight>>> MaterialiseGroups()
    {
        var perKey = new Dictionary<TKey, Dictionary<TValue, TWeight>>();

        void Fold(TKey k, ZSet<TValue, TWeight> group)
        {
            if (!perKey.TryGetValue(k, out var inner))
            {
                inner = new Dictionary<TValue, TWeight>();
                perKey[k] = inner;
            }

            foreach (var (v, w) in group)
            {
                if (inner.TryGetValue(v, out var existing))
                {
                    var sum = TWeight.Add(existing, w);
                    if (TWeight.IsZero(sum))
                    {
                        inner.Remove(v);
                    }
                    else
                    {
                        inner[v] = sum;
                    }
                }
                else
                {
                    inner[v] = w;
                }
            }
        }

        foreach (var level in _levels)
        {
            foreach (var batch in level)
            {
                foreach (var (k, group) in batch.Entries())
                {
                    Fold(k, group);
                }
            }
        }

        // The memtable's un-flushed deltas sum in alongside the batches.
        if (!_memtable.IsEmpty)
        {
            foreach (var (k, group) in _memtable)
            {
                Fold(k, group);
            }
        }

        var result = new List<KeyValuePair<TKey, ZSet<TValue, TWeight>>>(perKey.Count);
        foreach (var (k, inner) in perKey)
        {
            if (inner.Count == 0)
            {
                continue;
            }

            result.Add(new KeyValuePair<TKey, ZSet<TValue, TWeight>>(
                k, new ZSet<TValue, TWeight>(inner)));
        }

        return result;
    }
}
