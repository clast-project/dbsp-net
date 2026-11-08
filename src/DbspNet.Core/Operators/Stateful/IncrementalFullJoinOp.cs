// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Incremental FULL OUTER equi-join of two indexed Z-set streams. Emits the
/// inner join on keys matched on both sides, a NULL-padded-right row for every
/// left row whose key has no right-side match, and a NULL-padded-left row for
/// every right row whose key has no left-side match.
/// </summary>
/// <remarks>
/// <para>
/// The full-outer content of a key is
/// <c>F(L,R) = inner + leftPad + rightPad</c> where
/// <c>inner = (L,R both non-empty) ? L⋈R : ∅</c>,
/// <c>leftPad = (R empty) ? nullPadRight(L) : ∅</c>, and
/// <c>rightPad = (L empty) ? nullPadLeft(R) : ∅</c>. The
/// <c>inner + leftPad</c> part is exactly
/// <see cref="IncrementalLeftJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/>'s
/// per-key case analysis keyed on right-presence; FULL OUTER adds the symmetric
/// <c>rightPad</c> delta keyed on left-presence:
/// </para>
/// <list type="bullet">
/// <item><b>left stayed present</b> (old ∃ left, new ∃ left): rightPad is ∅
/// both ticks ⇒ nothing.</item>
/// <item><b>left stayed absent</b> (old ∄ left, new ∄ left):
/// <c>delta = nullPadLeft(drK)</c>.</item>
/// <item><b>left gained presence</b> (old ∄ left, new ∃ left): retract
/// <c>nullPadLeft(oldR)</c> (now covered by the inner part).</item>
/// <item><b>left lost presence</b> (old ∃ left, new ∄ left): emit
/// <c>nullPadLeft(newR)</c>.</item>
/// </list>
/// <para>
/// The two decompositions are independent, so simultaneous both-side
/// match-flips compose correctly: total per-key delta is exactly
/// <c>F(new) − F(old)</c>.
/// </para>
/// <para>
/// NULL-keyed rows on either side are handled by the caller (the
/// <see cref="DbspNet.Sql.Compiler"/> plan→circuit layer routes NULL-keyed
/// left rows to the NULL-padded-right branch and NULL-keyed right rows to the
/// NULL-padded-left branch): this operator works on non-null-keyed rows only.
/// </para>
/// </remarks>
internal sealed class IncrementalFullJoinOp<TKey, TLeft, TRight, TOut, TWeight> : IOperator, ISnapshotable, IIntrospectable
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
    private readonly Func<TKey, TLeft, TRight, TOut> _joinCombine;
    private readonly Func<TKey, TLeft, TOut> _nullPadRightCombine;
    private readonly Func<TKey, TRight, TOut> _nullPadLeftCombine;
    private readonly IGroupTrace<TKey, TLeft, TWeight> _leftTrace;
    private readonly IGroupTrace<TKey, TRight, TWeight> _rightTrace;
    private readonly IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? _leftSnapshotCodec;
    private readonly IIndexedZSetTraceCodec<TKey, TRight, TWeight>? _rightSnapshotCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<TKey, long>? _monotoneKey;

    // A non-equi ON conjunct (docs/design-flat-trace-state.md §3.30): match-presence per
    // (key, preserved row), on each side.
    private readonly Func<TKey, TLeft, TRight, bool>? _residual;
    private readonly Func<TKey, TRight, TLeft, bool>? _residualFromRight;
    private long _lastGcFrontier = long.MinValue;
    private long _gcDropped;

    // Last tick's output count, used to pre-size this tick's delta builder
    // (docs/design-row-representation.md §16.8).
    private int _lastOutputSize;

    public IncrementalFullJoinOp(
        Stream<IndexedZSet<TKey, TLeft, TWeight>> leftIn,
        Stream<IndexedZSet<TKey, TRight, TWeight>> rightIn,
        Stream<ZSet<TOut, TWeight>> output,
        Func<TKey, TLeft, TRight, TOut> joinCombine,
        Func<TKey, TLeft, TOut> nullPadRightCombine,
        Func<TKey, TRight, TOut> nullPadLeftCombine,
        IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? leftSnapshotCodec = null,
        IIndexedZSetTraceCodec<TKey, TRight, TWeight>? rightSnapshotCodec = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null,
        IGroupTrace<TKey, TLeft, TWeight>? leftTrace = null,
        IGroupTrace<TKey, TRight, TWeight>? rightTrace = null,
        Func<TKey, TLeft, TRight, bool>? residual = null)
    {
        _residual = residual;
        _residualFromRight = residual is null ? null : (k, r, l) => residual(k, l, r);
        _leftTrace = leftTrace ?? new ManagedGroupTrace<TKey, TLeft, TWeight>();
        _rightTrace = rightTrace ?? new ManagedGroupTrace<TKey, TRight, TWeight>();
        _leftIn = leftIn;
        _rightIn = rightIn;
        _output = output;
        _joinCombine = joinCombine;
        _nullPadRightCombine = nullPadRightCombine;
        _nullPadLeftCombine = nullPadLeftCombine;
        _leftSnapshotCodec = leftSnapshotCodec;
        _rightSnapshotCodec = rightSnapshotCodec;
        _frontier = frontier;
        _monotoneKey = monotoneKey;
    }

    /// <summary>Whether both traces are held as blocks (DBSPNET_TRACE_BLOCKS=join).</summary>
    internal bool IsBlocked => _leftTrace is Blocks.BlockedGroupTrace && _rightTrace is Blocks.BlockedGroupTrace;

    /// <summary>Total keys retained across both traces. Exposed for GC-bound tests.</summary>
    internal int RetainedKeyCount => _leftTrace.GroupCount + _rightTrace.GroupCount;

    public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_leftSnapshotCodec is null || _rightSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "IncrementalFullJoinOp was constructed without snapshot codecs; pass " +
                "them to CircuitBuilder.IncrementalFullJoin to enable Snapshot.WriteAsync/ReadAsync.");
        }

        await _leftTrace.SaveAsync(_leftSnapshotCodec, writer, LeftTraceFile, cancellationToken).ConfigureAwait(false);
        await _rightTrace.SaveAsync(_rightSnapshotCodec, writer, RightTraceFile, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_leftSnapshotCodec is null || _rightSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "IncrementalFullJoinOp was constructed without snapshot codecs.");
        }

        await _leftTrace.LoadAsync(_leftSnapshotCodec, reader, LeftTraceFile, cancellationToken).ConfigureAwait(false);
        await _rightTrace.LoadAsync(_rightSnapshotCodec, reader, RightTraceFile, cancellationToken).ConfigureAwait(false);
    }

    public string SchemaFingerprint =>
        $"left={_leftSnapshotCodec?.SchemaFingerprint ?? ""};right={_rightSnapshotCodec?.SchemaFingerprint ?? ""}";

    public void Step()
    {
        var dl = _leftIn.Current;
        var dr = _rightIn.Current;
        if (dl.IsEmpty && dr.IsEmpty)
        {
            _leftTrace.Idle();
            _rightTrace.Idle();
        }
        var leftOld = _leftTrace;
        var rightOld = _rightTrace;

        var builder = Blocks.DeltaSpill.For<TOut, TWeight>(_output.DeltaFormat, _lastOutputSize);

        // Every key that could have a non-zero delta this tick shows up in dl or dr, each once (§5.34).
        foreach (var (key, dlK) in TouchedKeys.Of(dl, dr))
        {
            var oldL = leftOld.GroupFor(key);
            var oldR = rightOld.GroupFor(key);
            var drK = dr.GroupFor(key);
            var newL = oldL + dlK;
            var newR = oldR + drK;

            if (_residual is { } residual)
            {
                ResidualOuterJoin.JoinInto(builder, key, oldL, dlK, drK, newR, residual, _joinCombine);
                ResidualOuterJoin.PadInto(builder, key, oldL, dlK, oldR, drK, newR, residual, _nullPadRightCombine);
                ResidualOuterJoin.PadInto(builder, key, oldR, drK, oldL, dlK, newL, _residualFromRight!, _nullPadLeftCombine);
                continue;
            }

            var oldRMatched = !oldR.IsEmpty;
            var newRMatched = !newR.IsEmpty;

            // Part A — inner + left-pad, keyed on right-presence (identical to
            // IncrementalLeftJoinOp).
            if (oldRMatched && newRMatched)
            {
                JoinInto(builder, key, dlK, newR);
                JoinInto(builder, key, oldL, drK);
            }
            else if (!oldRMatched && !newRMatched)
            {
                NullPadRightInto(builder, key, dlK);
            }
            else if (!oldRMatched && newRMatched)
            {
                NullPadRightInto(builder, key, oldL.Negate());
                JoinInto(builder, key, newL, newR);
            }
            else
            {
                JoinInto(builder, key, oldL.Negate(), oldR);
                NullPadRightInto(builder, key, newL);
            }

            // Part B — right-pad, keyed on left-presence (the FULL-OUTER
            // addition). Independent of part A.
            var oldLMatched = !oldL.IsEmpty;
            var newLMatched = !newL.IsEmpty;
            if (oldLMatched && newLMatched)
            {
                // rightPad ∅ both ticks — nothing.
            }
            else if (!oldLMatched && !newLMatched)
            {
                NullPadLeftInto(builder, key, drK);
            }
            else if (!oldLMatched && newLMatched)
            {
                NullPadLeftInto(builder, key, oldR.Negate());
            }
            else
            {
                NullPadLeftInto(builder, key, newR);
            }
        }

        var result = builder.Build();
        _lastOutputSize = result.Count;
        _output.SetCurrent(result);

        _leftTrace.Integrate(dl);
        _rightTrace.Integrate(dr);
        CollectGarbage();
    }

    // Frontier-driven GC: when the join key is monotone on BOTH sides, no future
    // delta can arrive at a sub-frontier key on either input, so that key's
    // match-presence state on both sides is frozen and its already-emitted
    // output (joined or either-side NULL-padded) is final. Drop those keys from
    // both traces. See IncrementalLeftJoinOp.CollectGarbage for the full
    // correctness argument and the both-sides-monotone license the caller must
    // verify.
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
        _gcDropped += _leftTrace.DropKeysBelow(frontier, _monotoneKey)?.Count ?? 0;
        _gcDropped += _rightTrace.DropKeysBelow(frontier, _monotoneKey)?.Count ?? 0;
    }

    public string MetricName => "IncrementalFullJoin";

    public long RetainedRows => _leftTrace.GroupCount + _rightTrace.GroupCount;

    public long LastOutputRows => _output.LastCount;

    public long? GcFrontier => Metric.Frontier(_frontier);

    public long GcDroppedTotal => _gcDropped;

    private void JoinInto(
        ZSetBuilder<TOut, TWeight> output,
        TKey key,
        ZSet<TLeft, TWeight> left,
        ZSet<TRight, TWeight> right)
    {
        if (left.IsEmpty || right.IsEmpty)
        {
            return;
        }

        foreach (var (lv, lw) in left)
        {
            foreach (var (rv, rw) in right)
            {
                output.Add(_joinCombine(key, lv, rv), TWeight.Multiply(lw, rw));
            }
        }
    }

    private void NullPadRightInto(
        ZSetBuilder<TOut, TWeight> output,
        TKey key,
        ZSet<TLeft, TWeight> left)
    {
        if (left.IsEmpty)
        {
            return;
        }

        foreach (var (lv, lw) in left)
        {
            output.Add(_nullPadRightCombine(key, lv), lw);
        }
    }

    private void NullPadLeftInto(
        ZSetBuilder<TOut, TWeight> output,
        TKey key,
        ZSet<TRight, TWeight> right)
    {
        if (right.IsEmpty)
        {
            return;
        }

        foreach (var (rv, rw) in right)
        {
            output.Add(_nullPadLeftCombine(key, rv), rw);
        }
    }
}
