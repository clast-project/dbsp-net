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
internal sealed class IncrementalAggregateOp<TKey, TValue, TOut> : IOperator
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

    public IncrementalAggregateOp(
        Stream<IndexedZSet<TKey, TValue, Z64>> input,
        Stream<ZSet<(TKey Key, TOut Value), Z64>> output,
        IAggregator<TValue, TOut> aggregator)
    {
        _input = input;
        _output = output;
        _aggregator = aggregator;
    }

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
    }
}
