// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Per-group incremental aggregate. State: an integrated indexed Z-set of
/// value multisets (one per group key), plus a cache of the last-emitted
/// aggregate value per key and an opaque per-key scratch state that the
/// aggregator uses to incrementalize its computation. Per tick:
/// <list type="number">
///   <item>For each group key touched by the delta, hand the aggregator its
///         prior cached value, the per-key delta, and the post-delta
///         multiset. The aggregator either folds the delta into its state
///         (SUM, COUNT, AVG) or rescans the post-delta multiset (MIN, MAX).</item>
///   <item>If the aggregate result changed, emit a retraction of the old
///         (key, value) pair and an insertion of the new one.</item>
///   <item>Drop cache entries for groups that became empty.</item>
/// </list>
/// </summary>
internal sealed class IncrementalAggregateOp<TKey, TValue, TOut> : IOperator, ISnapshotable
    where TKey : notnull
    where TValue : notnull
    where TOut : notnull
{
    private readonly Stream<IndexedZSet<TKey, TValue, Z64>> _input;
    private readonly Stream<ZSet<(TKey Key, TOut Value), Z64>> _output;
    private readonly IAggregator<TValue, TOut> _aggregator;
    private readonly IndexedZSetTrace<TKey, TValue, Z64> _trace = new();
    private readonly Dictionary<TKey, Optional<TOut>> _aggCache = new();
    private readonly Dictionary<TKey, object?> _stateCache = new();
    private readonly IIndexedZSetTraceCodec<TKey, TValue, Z64>? _snapshotCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<TKey, long>? _monotoneKey;
    private long _lastGcFrontier = long.MinValue;

    public IncrementalAggregateOp(
        Stream<IndexedZSet<TKey, TValue, Z64>> input,
        Stream<ZSet<(TKey Key, TOut Value), Z64>> output,
        IAggregator<TValue, TOut> aggregator,
        IIndexedZSetTraceCodec<TKey, TValue, Z64>? snapshotCodec = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null)
    {
        _input = input;
        _output = output;
        _aggregator = aggregator;
        _snapshotCodec = snapshotCodec;
        _frontier = frontier;
        _monotoneKey = monotoneKey;
    }

    /// <summary>
    /// Number of group keys currently retained in the trace. Exposed for tests
    /// that assert frontier-driven GC keeps state bounded.
    /// </summary>
    internal int RetainedGroupCount => _trace.Current.GroupCount;

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "IncrementalAggregateOp was constructed without a snapshot codec; " +
                "pass one to CircuitBuilder.IncrementalAggregate to enable Snapshot.WriteAsync/ReadAsync.");
        }

        return _snapshotCodec.SaveAsync(writer, "trace.arrows", _trace.Current, cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "IncrementalAggregateOp was constructed without a snapshot codec.");
        }

        var loaded = await _snapshotCodec.LoadAsync(reader, "trace.arrows", cancellationToken).ConfigureAwait(false);
        _trace.Integrate(loaded);

        // Rebuild the per-group caches from the restored trace. Each
        // aggregator's Update is already the function that maps a
        // multiset → (state, value); calling it with delta = afterMultiset
        // = group and a fresh state recovers both, matching the
        // steady-state invariants of every shipped aggregator (SUM/COUNT/
        // AVG fold weights into running totals; MIN/MAX walk transitions
        // out of beforeW=0 into the active set). One pass of size
        // |trace|; amortised across subsequent ticks.
        foreach (var (key, group) in _trace.Current)
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
            _output.SetCurrent(ZSet<(TKey, TOut), Z64>.Empty);
            return;
        }

        var before = _trace.Current;

        var builder = new ZSetBuilder<(TKey, TOut), Z64>();
        foreach (var key in delta.Keys)
        {
            var groupDelta = delta.GroupFor(key);
            var beforeGroup = before.GroupFor(key);
            var afterGroup = beforeGroup + groupDelta;

            var oldAgg = _aggCache.TryGetValue(key, out var cached)
                ? cached
                : Optional<TOut>.None;
            _stateCache.TryGetValue(key, out var state);

            var newAgg = _aggregator.Update(ref state, oldAgg, groupDelta, afterGroup);

            // Cache-pruning is keyed on "is there literally no trace
            // entry?" (dict-shape IsEmpty), not on the aggregator's
            // linear "group is present" gate. The aggregator's
            // per-key state (e.g. SqlSumAggregator.DistinctNonNullRows)
            // tracks weight transitions across ticks and must persist
            // for cancelling-weight groups where the trace still has
            // entries but the sum is zero. The emission decision
            // (retract vs emit) is handled separately by the
            // oldAgg/newAgg comparison below.
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
                continue;
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

        _output.SetCurrent(builder.Build());
        _trace.Integrate(delta);
        CollectGarbage();
    }

    /// <summary>
    /// Frontier-driven GC: once the advertised frontier advances, drop every
    /// group whose key is strictly below it — those groups can never be touched
    /// by future input, so their already-emitted aggregate stays in the
    /// downstream view while their trace and cache state is reclaimed. Emits
    /// nothing (GC reduces state, never output). No-op unless both a frontier
    /// and a monotone-key extractor were supplied.
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
        }
    }
}
