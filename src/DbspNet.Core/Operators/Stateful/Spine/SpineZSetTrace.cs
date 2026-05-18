using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// LSM-style trace over a Z-set: holds the running integral as a tiered
/// sequence of immutable batches rather than a single consolidated
/// dictionary. <see cref="Integrate"/> appends a new batch at level 0
/// (O(1) regardless of trace size) and triggers compaction up the tier
/// hierarchy per the supplied <see cref="ICompactionStrategy"/>.
/// </summary>
/// <remarks>
/// <para><b>Compared to the flat <c>ZSetTrace</c>:</b> the spine trades
/// per-key read cost (probe N batches, sum weights) for cheap
/// <see cref="Integrate"/> (no merge-on-write) and per-batch
/// serialisability (each batch is independently dumpable as a snapshot
/// chunk). Phase 1 of the spine migration: the data structure stands
/// alone, no operators are wired through it yet.</para>
/// <para><b>Snapshot bridge:</b> <see cref="Materialize"/> flattens the
/// spine into a single <see cref="ZSet{TKey,TWeight}"/> — exactly the
/// shape the current <c>IZSetTraceCodec</c> serialises. Per-batch
/// snapshotting is a later phase.</para>
/// </remarks>
public sealed class SpineZSetTrace<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly List<List<ZSet<TKey, TWeight>>> _levels = new();
    private readonly ICompactionStrategy _strategy;

    /// <summary>
    /// Creates an empty spine that uses
    /// <see cref="TieredCompactionStrategy.Default"/> (4 batches per
    /// level) for compaction.
    /// </summary>
    public SpineZSetTrace() : this(TieredCompactionStrategy.Default)
    {
    }

    public SpineZSetTrace(ICompactionStrategy strategy)
    {
        ArgumentNullException.ThrowIfNull(strategy);
        _strategy = strategy;
    }

    /// <summary>
    /// Folds <paramref name="delta"/> into the trace by appending it as a
    /// new batch at level 0, then running compaction to settlement.
    /// Empty deltas are a no-op.
    /// </summary>
    public void Integrate(ZSet<TKey, TWeight> delta)
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
    /// Returns the integrated weight of <paramref name="key"/> by
    /// summing across every batch. Cost is O(B) where B is the total
    /// batch count.
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
            _levels.Add(new List<ZSet<TKey, TWeight>>());
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
        var merged = MergeBatches(src, 0, action.BatchCount);
        src.RemoveRange(0, action.BatchCount);

        EnsureLevel(action.SourceLevel + 1);
        if (!merged.IsEmpty)
        {
            _levels[action.SourceLevel + 1].Add(merged);
        }
    }

    private static ZSet<TKey, TWeight> MergeBatches(
        List<ZSet<TKey, TWeight>> batches, int start, int count)
    {
        var b = new ZSetBuilder<TKey, TWeight>();
        for (var i = 0; i < count; i++)
        {
            foreach (var (k, w) in batches[start + i])
            {
                b.Add(k, w);
            }
        }

        return b.Build();
    }

    private Dictionary<TKey, TWeight> MergeEntries()
    {
        var merged = new Dictionary<TKey, TWeight>();
        foreach (var level in _levels)
        {
            foreach (var batch in level)
            {
                foreach (var (k, w) in batch)
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
