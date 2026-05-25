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
    private long _spillCounter;

    public SpineIndexedZSetTrace() : this(TieredCompactionStrategy.Default, keyComparer: null, valueComparer: null)
    {
    }

    public SpineIndexedZSetTrace(
        ICompactionStrategy strategy,
        IComparer<TKey>? keyComparer = null,
        IComparer<TValue>? valueComparer = null,
        SpineIndexedSpillConfig<TKey, TValue, TWeight>? spillConfig = null)
    {
        ArgumentNullException.ThrowIfNull(strategy);
        _strategy = strategy;
        _keyComparer = keyComparer ?? Comparer<TKey>.Default;
        _valueComparer = valueComparer ?? Comparer<TValue>.Default;
        _spillConfig = spillConfig;
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

        EnsureLevel(0);
        _levels[0].Add(ResidentSpineIndexedBatch<TKey, TValue, TWeight>.FromIndexed(
            delta, _keyComparer, _valueComparer));
        RunCompaction();
    }

    /// <summary>
    /// Frontier-driven GC: drops every key whose <paramref name="monotoneKey"/>
    /// projection is strictly below <paramref name="threshold"/>, returning the
    /// removed keys so the owning operator can drop parallel per-key caches.
    /// </summary>
    /// <remarks>
    /// Rebuilds the spine from the surviving (≥ threshold) state — O(retained
    /// state), which stays window-bounded as the frontier advances. Spill files
    /// for discarded batches are deleted. Folding the drop into compaction (so
    /// it costs O(dropped) and preserves batch structure) is a future
    /// optimisation; this correct first cut bounds memory, which is the point.
    /// </remarks>
    public IReadOnlyList<TKey> DropKeysBelow(long threshold, Func<TKey, long> monotoneKey)
    {
        ArgumentNullException.ThrowIfNull(monotoneKey);

        List<TKey>? removed = null;
        var survivors = new IndexedZSetBuilder<TKey, TValue, TWeight>();
        foreach (var (key, group) in MaterialiseGroups())
        {
            if (monotoneKey(key) < threshold)
            {
                (removed ??= new List<TKey>()).Add(key);
            }
            else
            {
                foreach (var (v, w) in group)
                {
                    survivors.Add(key, v, w);
                }
            }
        }

        if (removed is null)
        {
            return Array.Empty<TKey>();
        }

        foreach (var level in _levels)
        {
            foreach (var batch in level)
            {
                if (batch is SpilledSpineIndexedBatch<TKey, TValue, TWeight> spilled)
                {
                    SyncDelete(spilled);
                }
            }
        }

        _levels.Clear();
        var surviving = survivors.Build();
        if (!surviving.IsEmpty)
        {
            Integrate(surviving);
        }

        return removed;
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

        return any ? b.Build() : ZSet<TValue, TWeight>.Empty;
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
            toMerge, _keyComparer, _valueComparer);
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
            _keyComparer, _valueComparer, batch.Bloom, batch.GroupCount);
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
        foreach (var level in _levels)
        {
            foreach (var batch in level)
            {
                foreach (var (k, group) in batch.Entries())
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
