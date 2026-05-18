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
/// <see cref="GroupFor"/> walks all batches and unions each batch's
/// per-key group into a fresh <see cref="ZSet{TValue,TWeight}"/>;
/// <see cref="Entries"/> does the same key-major merge over the whole
/// trace. See <see cref="SpineZSetTrace{TKey,TWeight}"/> for the
/// compaction model — identical here.
/// </remarks>
public sealed class SpineIndexedZSetTrace<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly List<List<IndexedZSet<TKey, TValue, TWeight>>> _levels = new();
    private readonly ICompactionStrategy _strategy;

    public SpineIndexedZSetTrace() : this(TieredCompactionStrategy.Default)
    {
    }

    public SpineIndexedZSetTrace(ICompactionStrategy strategy)
    {
        ArgumentNullException.ThrowIfNull(strategy);
        _strategy = strategy;
    }

    /// <summary>
    /// Folds <paramref name="delta"/> in by appending it as a new batch
    /// at level 0, then running compaction to settlement. Empty deltas
    /// are a no-op.
    /// </summary>
    public void Integrate(IndexedZSet<TKey, TValue, TWeight> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        if (delta.IsEmpty)
        {
            return;
        }

        EnsureLevel(0);
        _levels[0].Add(delta);
        RunCompaction();
    }

    /// <summary>
    /// Returns the integrated group for <paramref name="key"/> — the
    /// union of every batch's group for that key, with weights summed
    /// per value. Zero-weight values are filtered.
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
            _levels.Add(new List<IndexedZSet<TKey, TValue, TWeight>>());
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

        var merged = MergeBatches(src, 0, action.BatchCount);
        src.RemoveRange(0, action.BatchCount);

        EnsureLevel(action.SourceLevel + 1);
        if (!merged.IsEmpty)
        {
            _levels[action.SourceLevel + 1].Add(merged);
        }
    }

    private static IndexedZSet<TKey, TValue, TWeight> MergeBatches(
        List<IndexedZSet<TKey, TValue, TWeight>> batches, int start, int count)
    {
        var b = new IndexedZSetBuilder<TKey, TValue, TWeight>();
        for (var i = 0; i < count; i++)
        {
            foreach (var (k, group) in batches[start + i])
            {
                foreach (var (v, w) in group)
                {
                    b.Add(k, v, w);
                }
            }
        }

        return b.Build();
    }

    private List<KeyValuePair<TKey, ZSet<TValue, TWeight>>> MaterialiseGroups()
    {
        var perKey = new Dictionary<TKey, Dictionary<TValue, TWeight>>();
        foreach (var level in _levels)
        {
            foreach (var batch in level)
            {
                foreach (var (k, group) in batch)
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
