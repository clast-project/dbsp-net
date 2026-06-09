// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Benchmark seam for <see cref="SpineIncrementalAggregateOp{TKey,TValue,TOut}"/>.
/// When set, the aggregate takes the per-key point-probe path (today's
/// <c>GroupFor</c> + Z-set rebuild, also the production path for single-key
/// ticks) for every tick instead of the batched galloping merge, so
/// <c>AggregateProbeBenchmark</c> can A/B both strategies in one process. Never
/// set in production code — the operator picks the path by tick width.
/// </summary>
internal static class SpineAggregateProbeMode
{
    internal static bool ForcePointProbe;
}

/// <summary>
/// Spine-backed incremental aggregate. Observable behaviour matches
/// <see cref="IncrementalAggregateOp{TKey,TValue,TOut}"/>; the only
/// difference is that the per-key value multisets are stored in a
/// <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}"/> rather than
/// a flat <see cref="IndexedZSetTrace{TKey,TValue,TWeight}"/>.
/// </summary>
/// <remarks>
/// Per tick the hot path is one <c>GroupFor</c> per delta key against
/// the spine trace — bloom-gated binary search across each batch — plus
/// the same aggregator update logic as the flat operator.
/// </remarks>
internal sealed class SpineIncrementalAggregateOp<TKey, TValue, TOut> : IOperator, ISnapshotable, IIntrospectable
    where TKey : notnull
    where TValue : notnull
    where TOut : notnull
{
    private readonly Stream<IndexedZSet<TKey, TValue, Z64>> _input;
    private readonly Stream<ZSet<(TKey Key, TOut Value), Z64>> _output;
    private readonly IAggregator<TValue, TOut> _aggregator;
    private readonly IComparer<TKey> _keyComparer;
    private readonly IComparer<TValue> _valueComparer;
    private readonly SpineIndexedZSetTrace<TKey, TValue, Z64> _trace;
    private readonly Dictionary<TKey, Optional<TOut>> _aggCache = new();
    private readonly Dictionary<TKey, object?> _stateCache = new();
    private readonly IIndexedZSetTraceCodec<TKey, TValue, Z64>? _snapshotCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<TKey, long>? _monotoneKey;
    private long _lastGcFrontier = long.MinValue;
    private long _gcDropped;

    public SpineIncrementalAggregateOp(
        Stream<IndexedZSet<TKey, TValue, Z64>> input,
        Stream<ZSet<(TKey Key, TOut Value), Z64>> output,
        IAggregator<TValue, TOut> aggregator,
        IIndexedZSetTraceCodec<TKey, TValue, Z64>? snapshotCodec = null,
        ICompactionStrategy? compactionStrategy = null,
        IComparer<TKey>? keyComparer = null,
        IComparer<TValue>? valueComparer = null,
        SpineIndexedSpillConfig<TKey, TValue, Z64>? spillConfig = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null)
    {
        _input = input;
        _output = output;
        _aggregator = aggregator;
        _keyComparer = keyComparer ?? Comparer<TKey>.Default;
        _valueComparer = valueComparer ?? Comparer<TValue>.Default;
        _trace = new SpineIndexedZSetTrace<TKey, TValue, Z64>(
            compactionStrategy ?? TieredCompactionStrategy.Default,
            keyComparer,
            valueComparer,
            spillConfig,
            monotoneKey);
        _snapshotCodec = snapshotCodec;
        _frontier = frontier;
        _monotoneKey = monotoneKey;
    }

    /// <summary>
    /// Distinct group keys currently retained in the trace. Exposed for tests
    /// that assert frontier-driven GC keeps state bounded.
    /// </summary>
    internal int RetainedGroupCount => _trace.GroupCount;

    /// <summary>Underlying trace — exposed for GC-shape tests.</summary>
    internal SpineIndexedZSetTrace<TKey, TValue, Z64> Trace => _trace;

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "SpineIncrementalAggregateOp was constructed without a snapshot codec.");
        }

        return SpineSnapshot.SaveAsync(
            writer,
            prefix: "trace",
            batches: _trace.GetBatches(),
            saveOne: (name, batch) => _snapshotCodec.SaveAsync(writer, name, batch, cancellationToken),
            cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "SpineIncrementalAggregateOp was constructed without a snapshot codec.");
        }

        await SpineSnapshot.LoadAsync(
            reader,
            prefix: "trace",
            loadOne: async name =>
            {
                var batch = await _snapshotCodec.LoadAsync(reader, name, cancellationToken).ConfigureAwait(false);
                _trace.Integrate(batch);
            },
            cancellationToken).ConfigureAwait(false);

        // Rebuild the per-group caches from the restored trace. Same
        // logic as IncrementalAggregateOp.LoadAsync — see the flat op
        // for the rationale.
        foreach (var (key, group) in _trace.Entries())
        {
            object? state = null;
            var agg = _aggregator.Update(ref state, Optional<TOut>.None, group, group);
            _aggCache[key] = agg;
            _stateCache[key] = state;
        }
    }

    public string SchemaFingerprint => _snapshotCodec?.SchemaFingerprint ?? string.Empty;

    public void Step()
    {
        var delta = _input.Current;
        if (delta.IsEmpty)
        {
            // No input deltas to process, but the frontier may still
            // have advanced — drop sub-frontier state before returning.
            // Mirrors DistinctOp's empty-path CollectGarbage.
            _output.SetCurrent(ZSet<(TKey, TOut), Z64>.Empty);
            CollectGarbage();
            return;
        }

        var builder = new ZSetBuilder<(TKey, TOut), Z64>();

        if (SpineAggregateProbeMode.ForcePointProbe || delta.GroupCount == 1)
        {
            // Point-probe path: one GroupFor per key, rebuilding the after-group
            // as a Z-set (which hashes every value row). Kept for single-key
            // ticks — the trace-level D==1 soft spot from merge-probe-bench.md.
            foreach (var key in delta.Keys)
            {
                var groupDelta = delta.GroupFor(key);
                var afterGroup = _trace.GroupFor(key) + groupDelta;
                EmitForKey(key, groupDelta, afterGroup, builder);
            }
        }
        else
        {
            // Merge path: sort the delta keys once, batch-probe the before-groups
            // as sorted runs (GroupForManySorted — no per-probe rehash), merge
            // each with its delta into a sorted after-run, and hand the
            // aggregator a SortedRunMultiset. Value rows are compared, never
            // hashed (docs/design-row-representation.md §8).
            var sortedKeys = SortedKeysOf(delta);
            var beforeRuns = _trace.GroupForManySorted(sortedKeys);
            var bi = 0;
            foreach (var key in sortedKeys)
            {
                // beforeRuns is the present-key subsequence of sortedKeys, in the
                // same order, so a single advancing cursor pairs them up.
                var beforeRun = Array.Empty<(TValue Value, Z64 Weight)>();
                if (bi < beforeRuns.Count && _keyComparer.Compare(beforeRuns[bi].Key, key) == 0)
                {
                    beforeRun = beforeRuns[bi].Group;
                    bi++;
                }

                var groupDelta = delta.GroupFor(key);
                // Lazy after-group view over (beforeRun, groupDelta): the
                // incremental MIN/MAX aggregators only probe WeightOf for the few
                // delta rows, so this never pays the O(N) after-run merge + array
                // the eager build cost (docs §12). SUM/AVG enumerate it lazily.
                EmitForKey(key, groupDelta, new MergeViewMultiset<TValue>(beforeRun, groupDelta, _valueComparer), builder);
            }
        }

        _output.SetCurrent(builder.Build());
        _trace.Integrate(delta);
        CollectGarbage();
    }

    /// <summary>
    /// Applies one group's update: runs the aggregator over the post-delta
    /// multiset, prunes or refreshes the per-key caches, and emits the
    /// retract/insert pair for any changed aggregate value. Shared by the
    /// point-probe and merge paths — they differ only in how
    /// <paramref name="afterGroup"/> is represented (a rebuilt <see cref="ZSet{TKey,TWeight}"/>
    /// vs a lazy <see cref="MergeViewMultiset{T}"/> over the before-run and the
    /// delta), which is exactly what the <see cref="IMultiset{TKey,TWeight}"/>
    /// abstraction hides.
    /// </summary>
    private void EmitForKey(
        TKey key,
        ZSet<TValue, Z64> groupDelta,
        IMultiset<TValue, Z64> afterGroup,
        ZSetBuilder<(TKey, TOut), Z64> builder)
    {
        var oldAgg = _aggCache.TryGetValue(key, out var cached)
            ? cached
            : Optional<TOut>.None;
        _stateCache.TryGetValue(key, out var state);

        var newAgg = _aggregator.Update(ref state, oldAgg, groupDelta, afterGroup);

        // Cache-pruning keyed on dict-shape IsEmpty (no trace entries) — see the
        // matching comment in IncrementalAggregateOp.Step for why the linear
        // gate can't be used here. A fully cancelled group's MergeViewMultiset
        // reports IsEmpty just like the rebuilt Z-set's.
        if (afterGroup.IsEmpty)
        {
            _aggCache.Remove(key);
            _stateCache.Remove(key);
        }
        else
        {
            _aggCache[key] = newAgg;
            _stateCache[key] = state;
        }

        if (oldAgg == newAgg)
        {
            return;
        }

        if (oldAgg.HasValue)
        {
            builder.Add((key, oldAgg.Value), new Z64(-1));
        }

        if (newAgg.HasValue)
        {
            builder.Add((key, newAgg.Value), new Z64(1));
        }
    }

    /// <summary>
    /// The delta's distinct outer keys, sorted by this op's key comparer (the
    /// order the spine batches are sorted by) — the contract
    /// <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}.GroupForManySorted"/>
    /// requires.
    /// </summary>
    private TKey[] SortedKeysOf(IndexedZSet<TKey, TValue, Z64> delta)
    {
        var keys = new TKey[delta.GroupCount];
        var i = 0;
        foreach (var key in delta.Keys)
        {
            keys[i++] = key;
        }

        Array.Sort(keys, _keyComparer);
        return keys;
    }

    /// <summary>
    /// Frontier-driven GC: once the advertised frontier advances, drop every
    /// group whose key is strictly below it (those groups are unreachable by
    /// future input). Reclaims trace and cache state without emitting — the
    /// group's already-emitted aggregate stays in the downstream view. No-op
    /// unless both a frontier and a monotone-key extractor were supplied.
    /// </summary>
    private void CollectGarbage()
    {
        if (_frontier is null || _monotoneKey is null)
        {
            return;
        }

        var frontier = _frontier.Value;
        if (frontier == long.MinValue || frontier <= _lastGcFrontier)
        {
            return;
        }

        _lastGcFrontier = frontier;
        foreach (var key in _trace.DropKeysBelow(frontier, _monotoneKey))
        {
            _aggCache.Remove(key);
            _stateCache.Remove(key);
            _gcDropped++;
        }
    }

    public string MetricName => "SpineIncrementalAggregate";

    public long RetainedRows => _trace.GroupCount;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => Metric.Frontier(_frontier);

    public long GcDroppedTotal => _gcDropped;
}
