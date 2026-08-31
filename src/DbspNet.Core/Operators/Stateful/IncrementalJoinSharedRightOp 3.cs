// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Incremental inner equi-join whose RIGHT side is supplied by a <b>shared</b>
/// <see cref="IArrangement{TKey,TValue,TWeight}"/> instead of a private right
/// trace. Many of these joins read one arrangement, so the right relation's
/// integral is built once (by the <see cref="ArrangeOp{TKey,TValue,TWeight}"/>)
/// rather than re-integrated per consumer (docs/design-row-representation.md
/// §6.2 — cross-operator shared arrangements).
/// </summary>
/// <remarks>
/// The incremental rule is unchanged from
/// <see cref="IncrementalJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/>:
/// <code>
/// out_t = dl ⋈ R_t + L_{t-1} ⋈ dr
/// </code>
/// The shared relation is always the <c>R_t</c> ("after") side: the
/// <see cref="ArrangeOp{TKey,TValue,TWeight}"/> folds <c>dr</c> into the shared
/// trace before this operator runs (registration / topological order), so
/// <see cref="IArrangement{TKey,TValue,TWeight}.Current"/> is exactly the
/// <c>R_t</c> a private right trace would hold. Only the left trace
/// (<c>L_{t-1}</c>, the delayed, non-shared side) is owned here. Output is
/// therefore byte-identical to a plain join over the same inputs.
/// <para>The shared trace carries no frontier GC (see
/// <see cref="ArrangeOp{TKey,TValue,TWeight}"/>); the left trace is also left
/// un-GC'd in this first increment to keep the contract symmetric.</para>
/// </remarks>
internal sealed class IncrementalJoinSharedRightOp<TKey, TLeft, TRight, TOut, TWeight> : IOperator, IIntrospectable
    where TKey : notnull
    where TLeft : notnull
    where TRight : notnull
    where TOut : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<IndexedZSet<TKey, TLeft, TWeight>> _leftIn;
    private readonly Stream<IndexedZSet<TKey, TRight, TWeight>> _rightDelta;
    private readonly IArrangement<TKey, TRight, TWeight> _rightArrangement;
    private readonly Stream<ZSet<TOut, TWeight>> _output;
    private readonly Func<TKey, TLeft, TRight, TOut> _combine;
    private readonly Func<TOut, bool>? _residual;
    private readonly IndexedZSetTrace<TKey, TLeft, TWeight> _leftTrace = new();

    public IncrementalJoinSharedRightOp(
        Stream<IndexedZSet<TKey, TLeft, TWeight>> leftIn,
        Stream<IndexedZSet<TKey, TRight, TWeight>> rightDelta,
        IArrangement<TKey, TRight, TWeight> rightArrangement,
        Stream<ZSet<TOut, TWeight>> output,
        Func<TKey, TLeft, TRight, TOut> combine,
        Func<TOut, bool>? residual = null)
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
        _residual = residual;
    }

    public void Step()
    {
        var dl = _leftIn.Current;
        var dr = _rightDelta.Current;

        var builder = new ZSetBuilder<TOut, TWeight>();
        // dl ⋈ R_t — the shared arrangement already folded dr into R_t this tick.
        IncrementalJoinCore.JoinInto(dl, _rightArrangement.Current, _combine, _residual, builder);
        // L_{t-1} ⋈ dr — the delayed left trace against this tick's right delta.
        IncrementalJoinCore.JoinInto(_leftTrace.Current, dr, _combine, _residual, builder);
        _output.SetCurrent(builder.Build());

        _leftTrace.Integrate(dl);
    }

    public string MetricName => "IncrementalInnerJoinSharedRight";

    // The shared right trace is owned and reported by the ArrangeOp; this
    // operator only retains the left side.
    public long RetainedRows => _leftTrace.Current.GroupCount;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;
}
