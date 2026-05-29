// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// LSM-style trace over a Z-set: holds the running integral as a tiered
/// sequence of immutable sorted-columnar batches rather than a single
/// consolidated dictionary. <see cref="Integrate"/> appends a new batch
/// at level 0 (O(|delta| · log|delta|) for the sort) and triggers
/// compaction up the tier hierarchy per the supplied
/// <see cref="ICompactionStrategy"/>.
/// </summary>
/// <remarks>
/// <para><b>Compared to the flat <c>ZSetTrace</c>:</b> the spine trades
/// per-key read cost (one bloom-gated binary search per batch) for
/// cheap <see cref="Integrate"/> (no merge-on-write) and per-batch
/// serialisability (each batch is independently dumpable as a snapshot
/// chunk).</para>
/// <para><b>Snapshot bridge:</b> <see cref="Materialize"/> flattens the
/// spine into a single <see cref="ZSet{TKey,TWeight}"/> — exactly the
/// shape the current <c>IZSetTraceCodec</c> serialises. Per-batch
/// snapshotting is a later phase.</para>
/// </remarks>
public sealed class SpineZSetTrace<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly List<List<SpineBatch<TKey, TWeight>>> _levels = new();
    private readonly ICompactionStrategy _strategy;
    private readonly IComparer<TKey> _comparer;
    private readonly SpineSpillConfig<TKey, TWeight>? _spillConfig;
    private readonly Func<TKey, long>? _monotoneKey;
    private long _spillCounter;

    /// <summary>
    /// Creates an empty spine that uses
    /// <see cref="TieredCompactionStrategy.Default"/> (4 batches per
    /// level) and <see cref="Comparer{TKey}.Default"/>.
    /// </summary>
    public SpineZSetTrace() : this(TieredCompactionStrategy.Default, comparer: null)
    {
    }

    public SpineZSetTrace(
        ICompactionStrategy strategy,
        IComparer<TKey>? comparer = null,
        SpineSpillConfig<TKey, TWeight>? spillConfig = null,
        Func<TKey, long>? monotoneKey = null)
    {
        ArgumentNullException.ThrowIfNull(strategy);
        _strategy = strategy;
        _comparer = comparer ?? Comparer<TKey>.Default;
        _spillConfig = spillConfig;
        _monotoneKey = monotoneKey;
    }

    /// <summary>
    /// Folds <paramref name="delta"/> into the trace by appending it as a
    /// new sorted batch at level 0, then running compaction to
    /// settlement. Empty deltas are a no-op.
    /// </summary>
    public void Integrate(ZSet<TKey, TWeight> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        if (delta.IsEmpty)
        {
            return;
        }

        EnsureLevel(0);
        _levels[0].Add(ResidentSpineBatch<TKey, TWeight>.FromZSet(delta, _comparer, _monotoneKey));
        RunCompaction();
    }

    /// <summary>
    /// Drops every key whose <paramref name="monotoneKey"/> projection is
    /// strictly below <paramref name="threshold"/> (frontier-driven GC),
    /// returning the number of keys removed. A key exactly at the threshold is
    /// retained. The non-indexed analogue of
    /// <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}.DropKeysBelow"/>,
    /// used by DISTINCT (where the key is the whole row).
    /// </summary>
    /// <remarks>
    /// Per-batch dispatch on each batch's <see cref="SpineBatch{TKey,TWeight}.MinMonotoneKey"/>
    /// / <see cref="SpineBatch{TKey,TWeight}.MaxMonotoneKey"/>:
    /// <list type="bullet">
    ///   <item><c>MaxMonotoneKey &lt; threshold</c> — whole batch is sub-frontier:
    ///   evict in place, delete the spill file if any. O(1) (plus spill delete).</item>
    ///   <item><c>MinMonotoneKey ≥ threshold</c> — every key survives: keep
    ///   the batch at its original level with no work.</item>
    ///   <item>Otherwise — mixed: load resident, mask-filter keys above the
    ///   threshold, replace the batch at its original level.</item>
    /// </list>
    /// Batch ordering and level layout are preserved, so the tiered compaction
    /// strategy's insertion-order assumptions stay intact (cf.
    /// <see cref="ICompactionStrategy"/> + <c>Apply()</c>). Batches whose min/max
    /// projections are unknown (the trace was constructed without a
    /// <c>monotoneKey</c>) fall back to a per-key scan over a loaded resident
    /// copy — still local to the batch, no global rebuild.
    /// </remarks>
    public int DropKeysBelow(long threshold, Func<TKey, long> monotoneKey)
    {
        ArgumentNullException.ThrowIfNull(monotoneKey);

        var removed = 0;
        for (var li = 0; li < _levels.Count; li++)
        {
            var level = _levels[li];
            for (var bi = level.Count - 1; bi >= 0; bi--)
            {
                var batch = level[bi];
                var (batchMin, batchMax) = GetOrComputeMonotoneRange(batch, monotoneKey);

                if (batch.IsEmpty || batchMax < threshold)
                {
                    if (batch is SpilledSpineBatch<TKey, TWeight> spilled)
                    {
                        SyncDelete(spilled);
                    }

                    removed += batch.Count;
                    level.RemoveAt(bi);
                    continue;
                }

                if (batchMin >= threshold)
                {
                    continue;
                }

                var resident = batch.Materialise(_comparer);
                var (filtered, dropped) = FilterAbove(resident, threshold, monotoneKey);
                removed += dropped;

                if (batch is SpilledSpineBatch<TKey, TWeight> spilledMixed)
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

        return removed;
    }

    private (long Min, long Max) GetOrComputeMonotoneRange(
        SpineBatch<TKey, TWeight> batch, Func<TKey, long> monotoneKey)
    {
        if (batch.MinMonotoneKey is long min && batch.MaxMonotoneKey is long max)
        {
            return (min, max);
        }

        // Cold path: the trace was constructed without a monotoneKey
        // projection but DropKeysBelow is being called with one anyway.
        // Materialise once and compute the range; correct but pays an
        // extra load for spilled batches.
        if (batch.IsEmpty)
        {
            return (long.MaxValue, long.MinValue);
        }

        var resident = batch.Materialise(_comparer);
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

    private (ResidentSpineBatch<TKey, TWeight> Filtered, int Dropped) FilterAbove(
        ResidentSpineBatch<TKey, TWeight> resident, long threshold, Func<TKey, long> monotoneKey)
    {
        var keys = resident.Keys;
        var weights = resident.Weights;
        var keep = new bool[keys.Length];
        var keptCount = 0;
        for (var i = 0; i < keys.Length; i++)
        {
            if (monotoneKey(keys[i]) >= threshold)
            {
                keep[i] = true;
                keptCount++;
            }
        }

        var dropped = keys.Length - keptCount;
        if (dropped == 0)
        {
            return (resident, 0);
        }

        if (keptCount == 0)
        {
            return (ResidentSpineBatch<TKey, TWeight>.Empty(_comparer), dropped);
        }

        var newKeys = new TKey[keptCount];
        var newWeights = new TWeight[keptCount];
        var oi = 0;
        for (var i = 0; i < keys.Length; i++)
        {
            if (keep[i])
            {
                newKeys[oi] = keys[i];
                newWeights[oi] = weights[i];
                oi++;
            }
        }

        return (ResidentSpineBatch<TKey, TWeight>.FromSortedArrays(
            newKeys, newWeights, _comparer, _monotoneKey ?? monotoneKey), dropped);
    }

    /// <summary>
    /// Returns the integrated weight of <paramref name="key"/> by
    /// summing across every batch. Per batch: a cache-line bloom probe;
    /// only batches that pay the bloom go to binary search.
    /// </summary>
    public TWeight WeightOf(TKey key)
    {
        var total = TWeight.Zero;
        foreach (var level in _levels)
        {
            foreach (var batch in level)
            {
                total = TWeight.Add(total, batch.WeightOf(key));
            }
        }

        return total;
    }

    /// <summary>True iff every batch is empty (or the spine holds no batches).</summary>
    public bool IsEmpty
    {
        get
        {
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
    /// Enumerates <c>(key, summed weight)</c> for every key with a
    /// non-zero integrated weight. Materialises a hash table internally
    /// — cost is O(total batch entries).
    /// </summary>
    public IEnumerable<KeyValuePair<TKey, TWeight>> Entries() => MergeEntries();

    /// <summary>
    /// Returns a single consolidated <see cref="ZSet{TKey,TWeight}"/>
    /// equivalent to the spine's integrated state. Intended for snapshot
    /// serialisation paths that haven't been migrated to per-batch
    /// writes yet, and for tests that need an equality oracle.
    /// </summary>
    public ZSet<TKey, TWeight> Materialize()
    {
        var b = new ZSetBuilder<TKey, TWeight>();
        foreach (var (k, w) in MergeEntries())
        {
            b.Add(k, w);
        }

        return b.Build();
    }

    /// <summary>
    /// Total number of immutable batches across all levels. Useful for
    /// tests and benchmarks that want to verify the compaction policy.
    /// </summary>
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

    /// <summary>Number of levels currently allocated (some may be empty).</summary>
    public int LevelCount => _levels.Count;

    /// <summary>
    /// Number of distinct keys with a non-zero integrated weight. Materialises
    /// the merged view — O(total batch entries). Exposed for GC-bound tests.
    /// </summary>
    public int KeyCount => MergeEntries().Count;

    /// <summary>
    /// Snapshots each non-empty batch's entries as a fresh
    /// <see cref="ZSet{TKey,TWeight}"/>, level structure flattened.
    /// Used by the per-batch snapshot path: each returned Z-set is
    /// serialised as its own snapshot file, so on load the trace can
    /// reconstitute by integrating one batch at a time rather than
    /// flattening through <see cref="Materialize"/>.
    /// </summary>
    public IReadOnlyList<ZSet<TKey, TWeight>> GetBatches()
    {
        var result = new List<ZSet<TKey, TWeight>>(BatchCount);
        foreach (var level in _levels)
        {
            foreach (var batch in level)
            {
                if (batch.IsEmpty)
                {
                    continue;
                }

                var b = new ZSetBuilder<TKey, TWeight>();
                foreach (var (k, w) in batch.Entries())
                {
                    b.Add(k, w);
                }

                result.Add(b.Build());
            }
        }

        return result;
    }

    /// <summary>
    /// Read-only view of the current per-level batch sizes. Exposed for
    /// tests / benchmarks; not part of the trace's runtime contract.
    /// </summary>
    internal SpineState State => SnapshotState();

    private SpineState SnapshotState()
    {
        var outer = new List<IReadOnlyList<int>>(_levels.Count);
        foreach (var level in _levels)
        {
            var inner = new int[level.Count];
            for (var i = 0; i < level.Count; i++)
            {
                inner[i] = level[i].Count;
            }

            outer.Add(inner);
        }

        return new SpineState(outer);
    }

    private void EnsureLevel(int level)
    {
        while (_levels.Count <= level)
        {
            _levels.Add(new List<SpineBatch<TKey, TWeight>>());
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

        // Merge the OLDEST `BatchCount` batches at this level — the
        // recent arrivals stay at the front so frequently-touched keys
        // don't get pushed deep into the tier hierarchy on every
        // compaction round.
        var toMerge = src.GetRange(0, action.BatchCount);
        var merged = SpineBatch<TKey, TWeight>.Merge(toMerge, _comparer, _monotoneKey);
        src.RemoveRange(0, action.BatchCount);

        // Delete on-disk files for any spilled inputs — they have been
        // consumed by the merge and would otherwise leak.
        foreach (var input in toMerge)
        {
            if (input is SpilledSpineBatch<TKey, TWeight> spilled)
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

    private SpineBatch<TKey, TWeight> MaybeSpill(ResidentSpineBatch<TKey, TWeight> batch, int destLevel)
    {
        if (_spillConfig is null || destLevel < _spillConfig.MinSpillLevel)
        {
            return batch;
        }

        var path = _spillConfig.Prefix + "/batch_" +
            Interlocked.Increment(ref _spillCounter).ToString(System.Globalization.CultureInfo.InvariantCulture) +
            ".arrows";

        // Materialise into a ZSet for the codec. Sync block on the
        // SaveAsync: write paths are infrequent (one per compaction)
        // and the underlying ITableFileSystem either completes inline
        // (in-memory) or performs a bounded disk write.
        var zsetBuilder = new ZSetBuilder<TKey, TWeight>();
        foreach (var (k, w) in batch.Entries())
        {
            zsetBuilder.Add(k, w);
        }

        var zset = zsetBuilder.Build();
        var ctx = new SpillContext(_spillConfig.FileSystem);
        var saveTask = _spillConfig.Codec.SaveAsync(ctx, path, zset, default);
        if (!saveTask.IsCompletedSuccessfully)
        {
            saveTask.AsTask().GetAwaiter().GetResult();
        }

        return new SpilledSpineBatch<TKey, TWeight>(
            _spillConfig.FileSystem, path, _spillConfig.Codec, _comparer, batch.Bloom, batch.Count,
            batch.MinMonotoneKey, batch.MaxMonotoneKey);
    }

    private static void SyncDelete(SpilledSpineBatch<TKey, TWeight> spilled)
    {
        var t = spilled.DeleteAsync();
        if (!t.IsCompletedSuccessfully)
        {
            t.AsTask().GetAwaiter().GetResult();
        }
    }

    private Dictionary<TKey, TWeight> MergeEntries()
    {
        var merged = new Dictionary<TKey, TWeight>();
        foreach (var level in _levels)
        {
            foreach (var batch in level)
            {
                foreach (var (k, w) in batch.Entries())
                {
                    if (merged.TryGetValue(k, out var existing))
                    {
                        var sum = TWeight.Add(existing, w);
                        if (TWeight.IsZero(sum))
                        {
                            merged.Remove(k);
                        }
                        else
                        {
                            merged[k] = sum;
                        }
                    }
                    else
                    {
                        merged[k] = w;
                    }
                }
            }
        }

        return merged;
    }
}
