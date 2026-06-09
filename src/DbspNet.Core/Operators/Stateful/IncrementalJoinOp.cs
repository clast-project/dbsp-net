// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Incremental inner equi-join of two indexed Z-set streams. Emits a flat
/// Z-set of combined rows. State: the integrated traces of both inputs.
/// Per tick, output is the asymmetric two-pass factoring of the join delta
/// rule
/// <code>
/// out_t = dl ⋈ R_t + L_{t-1} ⋈ dr
/// </code>
/// where <c>dl, dr</c> are this tick's deltas, <c>L_{t-1}</c> is the
/// delayed left trace, and <c>R_t</c> is the right trace with <c>dr</c>
/// already folded in. This subsumes the literal three-term expansion
/// <code>
/// dl ⋈ R_{t-1} + L_{t-1} ⋈ dr + dl ⋈ dr
/// </code>
/// by absorbing the cross term <c>dl ⋈ dr</c> into <c>dl ⋈ R_t</c>.
/// Equivalent to the <c>D(↑Q(I, I))</c> sandwich on the bilinear operator
/// <c>Q(a,b) = a ⋈ b</c> by the DBSP bilinearity theorem.
/// </summary>
internal sealed class IncrementalJoinOp<TKey, TLeft, TRight, TOut, TWeight> : IOperator, ISnapshotable, IIntrospectable
    where TKey : notnull
    where TLeft : notnull
    where TRight : notnull
    where TOut : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private const string LeftTraceFile = "left.arrows";
    private const string RightTraceFile = "right.arrows";

    private readonly Stream<IndexedZSet<TKey, TLeft, TWeight>> _leftIn;
    private readonly Stream<IndexedZSet<TKey, TRight, TWeight>> _rightIn;
    private readonly Stream<ZSet<TOut, TWeight>> _output;
    private readonly Func<TKey, TLeft, TRight, TOut> _combine;
    private readonly Func<TOut, bool>? _residual;
    private readonly IndexedZSetTrace<TKey, TLeft, TWeight> _leftTrace = new();
    private readonly IndexedZSetTrace<TKey, TRight, TWeight> _rightTrace = new();
    private readonly IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? _leftSnapshotCodec;
    private readonly IIndexedZSetTraceCodec<TKey, TRight, TWeight>? _rightSnapshotCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<TKey, long>? _monotoneKey;
    private long _lastGcFrontier = long.MinValue;
    private long _gcDropped;

    public IncrementalJoinOp(
        Stream<IndexedZSet<TKey, TLeft, TWeight>> leftIn,
        Stream<IndexedZSet<TKey, TRight, TWeight>> rightIn,
        Stream<ZSet<TOut, TWeight>> output,
        Func<TKey, TLeft, TRight, TOut> combine,
        IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? leftSnapshotCodec = null,
        IIndexedZSetTraceCodec<TKey, TRight, TWeight>? rightSnapshotCodec = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null,
        Func<TOut, bool>? residual = null)
    {
        _leftIn = leftIn;
        _rightIn = rightIn;
        _output = output;
        _combine = combine;
        _residual = residual;
        _leftSnapshotCodec = leftSnapshotCodec;
        _rightSnapshotCodec = rightSnapshotCodec;
        _frontier = frontier;
        _monotoneKey = monotoneKey;
    }

    /// <summary>Total keys retained across both traces. Exposed for GC-bound tests.</summary>
    internal int RetainedKeyCount => _leftTrace.Current.GroupCount + _rightTrace.Current.GroupCount;

    public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_leftSnapshotCodec is null || _rightSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "IncrementalJoinOp was constructed without snapshot codecs; pass them " +
                "to CircuitBuilder.IncrementalInnerJoin to enable Snapshot.WriteAsync/ReadAsync.");
        }

        await _leftSnapshotCodec.SaveAsync(writer, LeftTraceFile, _leftTrace.Current, cancellationToken).ConfigureAwait(false);
        await _rightSnapshotCodec.SaveAsync(writer, RightTraceFile, _rightTrace.Current, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_leftSnapshotCodec is null || _rightSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "IncrementalJoinOp was constructed without snapshot codecs.");
        }

        _leftTrace.Integrate(await _leftSnapshotCodec.LoadAsync(reader, LeftTraceFile, cancellationToken).ConfigureAwait(false));
        _rightTrace.Integrate(await _rightSnapshotCodec.LoadAsync(reader, RightTraceFile, cancellationToken).ConfigureAwait(false));
    }

    public string SchemaFingerprint =>
        $"left={_leftSnapshotCodec?.SchemaFingerprint ?? ""};right={_rightSnapshotCodec?.SchemaFingerprint ?? ""}";

    public void Step()
    {
        var dl = _leftIn.Current;
        var dr = _rightIn.Current;

        // Integrate dr into the right trace BEFORE the joins so the first
        // pass sees R_t = R_{t-1} + dr — that absorbs the dl ⋈ dr cross
        // term. The left trace stays at L_{t-1} for the second pass and is
        // integrated AFTER. Order matters: do not move the two Integrate
        // calls together.
        _rightTrace.Integrate(dr);

        var builder = new ZSetBuilder<TOut, TWeight>();
        // dl ⋈ R_t
        IncrementalJoinCore.JoinInto(dl, _rightTrace.Current, _combine, _residual, builder);
        // L_{t-1} ⋈ dr
        IncrementalJoinCore.JoinInto(_leftTrace.Current, dr, _combine, _residual, builder);
        _output.SetCurrent(builder.Build());

        _leftTrace.Integrate(dl);
        CollectGarbage();
    }

    // Frontier-driven GC: when the join key is monotone on BOTH sides (so no
    // future delta can arrive at a sub-frontier key on either input), drop those
    // keys from both traces. Emits nothing; the already-emitted joined rows stay.
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

    public string MetricName => "IncrementalInnerJoin";

    public long RetainedRows => _leftTrace.Current.GroupCount + _rightTrace.Current.GroupCount;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => Metric.Frontier(_frontier);

    public long GcDroppedTotal => _gcDropped;
}
