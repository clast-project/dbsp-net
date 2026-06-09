// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Spine-backed incremental inner equi-join whose RIGHT side is supplied by a
/// <b>shared</b> <see cref="ISpineArrangement{TKey,TValue,TWeight}"/> instead of
/// a private right trace. Many of these joins probe one shared spine trace, so
/// the right relation's expensive sorted-batch maintenance is paid once (by the
/// <see cref="SpineArrangeOp{TKey,TValue,TWeight}"/>) rather than rebuilt per
/// consumer every tick — the §8.3 q4 substrate cost, deduplicated
/// (docs/design-row-representation.md §6.2 / §9).
/// </summary>
/// <remarks>
/// The incremental rule is unchanged from
/// <see cref="SpineIncrementalJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/>:
/// <c>out_t = dl ⋈ R_t + L_{t-1} ⋈ dr</c>. The shared relation is always the
/// <c>R_t</c> ("after") side: the arrangement folds <c>dr</c> into the shared
/// trace before this operator runs (registration / topological order), so the
/// first pass probes exactly the <c>R_t</c> a private right trace would hold.
/// Only the left (delayed <c>L_{t-1}</c>) trace is owned here. Both passes go
/// through <see cref="SpineJoinProbe"/>, identical to the private-trace join, so
/// output is byte-identical.
/// </remarks>
internal sealed class SpineIncrementalJoinSharedRightOp<TKey, TLeft, TRight, TOut, TWeight> : IOperator, IIntrospectable
    where TKey : notnull
    where TLeft : notnull
    where TRight : notnull
    where TOut : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<IndexedZSet<TKey, TLeft, TWeight>> _leftIn;
    private readonly Stream<IndexedZSet<TKey, TRight, TWeight>> _rightDelta;
    private readonly ISpineArrangement<TKey, TRight, TWeight> _rightArrangement;
    private readonly Stream<ZSet<TOut, TWeight>> _output;
    private readonly Func<TKey, TLeft, TRight, TOut> _combine;
    private readonly Func<TKey, TRight, TLeft, TOut> _combineSwapped;
    private readonly IComparer<TKey> _keyComparer;
    private readonly SpineIndexedZSetTrace<TKey, TLeft, TWeight> _leftTrace;

    public SpineIncrementalJoinSharedRightOp(
        Stream<IndexedZSet<TKey, TLeft, TWeight>> leftIn,
        Stream<IndexedZSet<TKey, TRight, TWeight>> rightDelta,
        ISpineArrangement<TKey, TRight, TWeight> rightArrangement,
        Stream<ZSet<TOut, TWeight>> output,
        Func<TKey, TLeft, TRight, TOut> combine,
        ICompactionStrategy? compactionStrategy = null,
        IComparer<TKey>? keyComparer = null,
        IComparer<TLeft>? leftValueComparer = null,
        SpineIndexedSpillConfig<TKey, TLeft, TWeight>? leftSpillConfig = null)
    {
        ArgumentNullException.ThrowIfNull(leftIn);
        ArgumentNullException.ThrowIfNull(rightDelta);
        ArgumentNullException.ThrowIfNull(rightArrangement);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(combine);

        _leftIn = leftIn;
        _rightDelta = rightDelta;
        _rightArrangement = rightArrangement;
        _output = output;
        _combine = combine;
        _combineSwapped = (key, rv, lv) => combine(key, lv, rv);
        _keyComparer = keyComparer ?? Comparer<TKey>.Default;
        _leftTrace = new SpineIndexedZSetTrace<TKey, TLeft, TWeight>(
            compactionStrategy ?? TieredCompactionStrategy.Default, keyComparer, leftValueComparer, leftSpillConfig);
    }

    public void Step()
    {
        var dl = _leftIn.Current;
        var dr = _rightDelta.Current;

        var builder = new ZSetBuilder<TOut, TWeight>();

        // dl ⋈ R_t — probe the SHARED right trace, which the arrangement already
        // folded dr into this tick. Sort dl's keys by the arrangement's order.
        SpineJoinProbe.ProbeAndJoin(dl, _rightArrangement.Trace, _combine, _rightArrangement.KeyComparer, builder);

        // L_{t-1} ⋈ dr — probe the owned, delayed left trace.
        SpineJoinProbe.ProbeAndJoin(dr, _leftTrace, _combineSwapped, _keyComparer, builder);

        _output.SetCurrent(builder.Build());

        _leftTrace.Integrate(dl);
    }

    public string MetricName => "SpineIncrementalInnerJoinSharedRight";

    // The shared right trace is owned and reported by the SpineArrangeOp; this
    // operator only retains the left side.
    public long RetainedRows => _leftTrace.GroupCount;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;
}
