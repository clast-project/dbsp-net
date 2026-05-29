// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Core.Operators.Stateful.Spine;

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
internal sealed class SpineIncrementalAggregateOp<TKey, TValue, TOut> : IOperator, ISnapshotable
    where TKey : notnull
    where TValue : notnull
    where TOut : notnull
{
    private readonly Stream<IndexedZSet<TKey, TValue, Z64>> _input;
    private readonly Stream<ZSet<(TKey Key, TOut Value), Z64>> _output;
    private readonly IAggregator<TValue, TOut> _aggregator;
    private readonly SpineIndexedZSetTrace<TKey, TValue, Z64> _trace;
    private readonly Dictionary<TKey, Optional<TOut>> _aggCache = new();
    private readonly Dictionary<TKey, object?> _stateCache = new();
    private readonly IIndexedZSetTraceCodec<TKey, TValue, Z64>? _snapshotCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<TKey, long>? _monotoneKey;
    private long _lastGcFrontier = long.MinValue;

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
            _output.SetCurrent(ZSet<(TKey, TOut), Z64>.Empty);
            return;
        }

        var builder = new ZSetBuilder<(TKey, TOut), Z64>();
        foreach (var key in delta.Keys)
        {
            var groupDelta = delta.GroupFor(key);
            var beforeGroup = _trace.GroupFor(key);
            var afterGroup = beforeGroup + groupDelta;

            var oldAgg = _aggCache.TryGetValue(key, out var cached)
                ? cached
                : Optional<TOut>.None;
            _stateCache.TryGetValue(key, out var state);

            var newAgg = _aggregator.Update(ref state, oldAgg, groupDelta, afterGroup);

            // Cache-pruning keyed on dict-shape IsEmpty (no trace
            // entries) — see the matching comment in
            // IncrementalAggregateOp.Step for why the linear gate
            // can't be used here.
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
        }
    }
}
