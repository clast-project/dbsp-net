// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Benchmark seam for <see cref="SpineIncrementalJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/>.
/// When set, the join takes the per-key point-probe path (today's
/// <c>GroupFor</c> loop, also the production path for single-key ticks)
/// for every tick instead of the batched galloping merge, so
/// <c>JoinProbeBenchmark</c> can A/B both strategies in one process. Never
/// set in production code — the operator picks the path by tick width.
/// </summary>
internal static class SpineJoinProbeMode
{
    internal static bool ForcePointProbe;
}

/// <summary>
/// Spine-backed incremental inner equi-join. Observable behaviour
/// matches <see cref="IncrementalJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/>;
/// the only difference is that the two integrated traces live in
/// <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}"/> rather than
/// the flat <see cref="IndexedZSetTrace{TKey,TValue,TWeight}"/>.
/// </summary>
/// <remarks>
/// <para>The asymmetric two-pass form — <c>dl ⋈ R_t + L_{t-1} ⋈ dr</c>
/// — is unchanged. The integrate ordering (right before, left after)
/// matters identically; the bilinearity argument still holds.</para>
/// <para><b>Probe direction.</b> Each <see cref="JoinInto"/> call
/// iterates the delta side and probes the spine trace via
/// <c>GroupFor</c>. This is the steady-state shape (delta &lt;&lt;
/// trace); workloads where the delta is larger than the trace will
/// pay the wrong-side iteration cost. The flat operator would pick the
/// smaller side dynamically.</para>
/// </remarks>
internal sealed class SpineIncrementalJoinOp<TKey, TLeft, TRight, TOut, TWeight> : IOperator, ISnapshotable, IIntrospectable
    where TKey : notnull
    where TLeft : notnull
    where TRight : notnull
    where TOut : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<IndexedZSet<TKey, TLeft, TWeight>> _leftIn;
    private readonly Stream<IndexedZSet<TKey, TRight, TWeight>> _rightIn;
    private readonly Stream<ZSet<TOut, TWeight>> _output;
    private readonly Func<TKey, TLeft, TRight, TOut> _combine;
    // _combine with the delta/probe arguments swapped, for the second join
    // pass (dr probes the left trace). Cached so Step allocates no delegate.
    private readonly Func<TKey, TRight, TLeft, TOut> _combineSwapped;
    private readonly IComparer<TKey> _keyComparer;
    private readonly SpineIndexedZSetTrace<TKey, TLeft, TWeight> _leftTrace;
    private readonly SpineIndexedZSetTrace<TKey, TRight, TWeight> _rightTrace;
    private readonly IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? _leftSnapshotCodec;
    private readonly IIndexedZSetTraceCodec<TKey, TRight, TWeight>? _rightSnapshotCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<TKey, long>? _monotoneKey;
    private long _lastGcFrontier = long.MinValue;
    private long _gcDropped;

    public SpineIncrementalJoinOp(
        Stream<IndexedZSet<TKey, TLeft, TWeight>> leftIn,
        Stream<IndexedZSet<TKey, TRight, TWeight>> rightIn,
        Stream<ZSet<TOut, TWeight>> output,
        Func<TKey, TLeft, TRight, TOut> combine,
        IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? leftSnapshotCodec = null,
        IIndexedZSetTraceCodec<TKey, TRight, TWeight>? rightSnapshotCodec = null,
        ICompactionStrategy? compactionStrategy = null,
        IComparer<TKey>? keyComparer = null,
        IComparer<TLeft>? leftValueComparer = null,
        IComparer<TRight>? rightValueComparer = null,
        SpineIndexedSpillConfig<TKey, TLeft, TWeight>? leftSpillConfig = null,
        SpineIndexedSpillConfig<TKey, TRight, TWeight>? rightSpillConfig = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null)
    {
        _leftIn = leftIn;
        _rightIn = rightIn;
        _output = output;
        _combine = combine;
        _combineSwapped = (key, rv, lv) => combine(key, lv, rv);
        _keyComparer = keyComparer ?? Comparer<TKey>.Default;
        var strategy = compactionStrategy ?? TieredCompactionStrategy.Default;
        _leftTrace = new SpineIndexedZSetTrace<TKey, TLeft, TWeight>(strategy, keyComparer, leftValueComparer, leftSpillConfig, monotoneKey);
        _rightTrace = new SpineIndexedZSetTrace<TKey, TRight, TWeight>(strategy, keyComparer, rightValueComparer, rightSpillConfig, monotoneKey);
        _leftSnapshotCodec = leftSnapshotCodec;
        _rightSnapshotCodec = rightSnapshotCodec;
        _frontier = frontier;
        _monotoneKey = monotoneKey;
    }

    /// <summary>Total keys retained across both traces. Exposed for GC-bound tests.</summary>
    internal int RetainedKeyCount => _leftTrace.GroupCount + _rightTrace.GroupCount;

    public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_leftSnapshotCodec is null || _rightSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "SpineIncrementalJoinOp was constructed without snapshot codecs.");
        }

        await SpineSnapshot.SaveAsync(
            writer, prefix: "left",
            batches: _leftTrace.GetBatches(),
            saveOne: (name, batch) => _leftSnapshotCodec.SaveAsync(writer, name, batch, cancellationToken),
            cancellationToken).ConfigureAwait(false);

        await SpineSnapshot.SaveAsync(
            writer, prefix: "right",
            batches: _rightTrace.GetBatches(),
            saveOne: (name, batch) => _rightSnapshotCodec.SaveAsync(writer, name, batch, cancellationToken),
            cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_leftSnapshotCodec is null || _rightSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "SpineIncrementalJoinOp was constructed without snapshot codecs.");
        }

        await SpineSnapshot.LoadAsync(
            reader, prefix: "left",
            loadOne: async name =>
            {
                var batch = await _leftSnapshotCodec.LoadAsync(reader, name, cancellationToken).ConfigureAwait(false);
                _leftTrace.Integrate(batch);
            },
            cancellationToken).ConfigureAwait(false);

        await SpineSnapshot.LoadAsync(
            reader, prefix: "right",
            loadOne: async name =>
            {
                var batch = await _rightSnapshotCodec.LoadAsync(reader, name, cancellationToken).ConfigureAwait(false);
                _rightTrace.Integrate(batch);
            },
            cancellationToken).ConfigureAwait(false);
    }

    public string SchemaFingerprint =>
        $"left={_leftSnapshotCodec?.SchemaFingerprint ?? ""};right={_rightSnapshotCodec?.SchemaFingerprint ?? ""}";

    public void Step()
    {
        var dl = _leftIn.Current;
        var dr = _rightIn.Current;

        // Right delta integrated BEFORE the joins so the first pass sees
        // R_t = R_{t-1} + dr — absorbs the dl ⋈ dr cross term. The left
        // trace stays at L_{t-1} for the second pass.
        _rightTrace.Integrate(dr);

        var builder = new ZSetBuilder<TOut, TWeight>();

        // dl ⋈ R_t — iterate dl, probe right spine. delta value is the left
        // side, so _combine takes (key, deltaValue, probeValue) directly.
        ProbeAndJoin(dl, _rightTrace, _combine, builder);

        // L_{t-1} ⋈ dr — iterate dr, probe left spine. delta value is the
        // right side now, so the probe value fills _combine's left slot.
        ProbeAndJoin(dr, _leftTrace, _combineSwapped, builder);

        _output.SetCurrent(builder.Build());

        _leftTrace.Integrate(dl);
        CollectGarbage();
    }

    /// <summary>
    /// Joins one delta side against the integrated <paramref name="trace"/> on
    /// the other side. Replaces the per-key <c>GroupFor</c> point-probe with a
    /// single batched galloping merge (<see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}.GroupForManySorted"/>):
    /// the delta keys are sorted once and each sorted batch's outer-key column
    /// is walked once, so probe-side misses skip in ~one comparison instead of
    /// a full bloom + binary search per key. Matched groups arrive as sorted
    /// <c>(value, weight)</c> runs sliced straight from the batch columns — the
    /// cross-product consumes them by iteration exactly like the dictionary
    /// group it replaces, so the output is identical (see
    /// docs/design-row-representation.md §8 and docs/merge-probe-bench.md).
    /// </summary>
    /// <remarks>
    /// The merge only pays off once the tick spans more than one key — at
    /// <c>D == 1</c> a single point probe beats the per-batch cursor setup
    /// (merge-probe-bench.md), so single-key ticks keep today's path.
    /// </remarks>
    private void ProbeAndJoin<TDelta, TProbe>(
        IndexedZSet<TKey, TDelta, TWeight> delta,
        SpineIndexedZSetTrace<TKey, TProbe, TWeight> trace,
        Func<TKey, TDelta, TProbe, TOut> combine,
        ZSetBuilder<TOut, TWeight> builder)
        where TDelta : notnull
        where TProbe : notnull
    {
        if (delta.IsEmpty)
        {
            return;
        }

        if (SpineJoinProbeMode.ForcePointProbe || delta.GroupCount == 1)
        {
            foreach (var (key, dGroup) in delta)
            {
                var pGroup = trace.GroupFor(key);
                if (pGroup.IsEmpty)
                {
                    continue;
                }

                foreach (var (dv, dw) in dGroup)
                {
                    foreach (var (pv, pw) in pGroup)
                    {
                        builder.Add(combine(key, dv, pv), TWeight.Multiply(dw, pw));
                    }
                }
            }

            return;
        }

        var sortedKeys = SortedKeysOf(delta);
        foreach (var (key, pRun) in trace.GroupForManySorted(sortedKeys))
        {
            var dGroup = delta.GroupFor(key);
            foreach (var (dv, dw) in dGroup)
            {
                foreach (var (pv, pw) in pRun)
                {
                    builder.Add(combine(key, dv, pv), TWeight.Multiply(dw, pw));
                }
            }
        }
    }

    /// <summary>
    /// The delta's distinct outer keys, sorted by this op's key comparer (the
    /// same order the spine batches are sorted by) — the contract
    /// <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}.GroupForManySorted"/>
    /// requires.
    /// </summary>
    private TKey[] SortedKeysOf<TDelta>(IndexedZSet<TKey, TDelta, TWeight> delta)
        where TDelta : notnull
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

    // Frontier-driven GC: when the join key is monotone on BOTH sides, drop
    // sub-frontier keys from both traces (filter-rebuild). Emits nothing.
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
        _gcDropped += _leftTrace.DropKeysBelow(frontier, _monotoneKey).Count;
        _gcDropped += _rightTrace.DropKeysBelow(frontier, _monotoneKey).Count;
    }

    public string MetricName => "SpineIncrementalInnerJoin";

    public long RetainedRows => _leftTrace.GroupCount + _rightTrace.GroupCount;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => Metric.Frontier(_frontier);

    public long GcDroppedTotal => _gcDropped;
}
