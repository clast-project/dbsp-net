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
internal sealed class IncrementalFullJoinOp<TKey, TLeft, TRight, TOut, TWeight> : IOperator, ISnapshotable
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
    private readonly IndexedZSetTrace<TKey, TLeft, TWeight> _leftTrace = new();
    private readonly IndexedZSetTrace<TKey, TRight, TWeight> _rightTrace = new();
    private readonly IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? _leftSnapshotCodec;
    private readonly IIndexedZSetTraceCodec<TKey, TRight, TWeight>? _rightSnapshotCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<TKey, long>? _monotoneKey;
    private long _lastGcFrontier = long.MinValue;

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
        Func<TKey, long>? monotoneKey = null)
    {
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

    /// <summary>Total keys retained across both traces. Exposed for GC-bound tests.</summary>
    internal int RetainedKeyCount => _leftTrace.Current.GroupCount + _rightTrace.Current.GroupCount;

    public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_leftSnapshotCodec is null || _rightSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "IncrementalFullJoinOp was constructed without snapshot codecs; pass " +
                "them to CircuitBuilder.IncrementalFullJoin to enable Snapshot.WriteAsync/ReadAsync.");
        }

        await _leftSnapshotCodec.SaveAsync(writer, LeftTraceFile, _leftTrace.Current, cancellationToken).ConfigureAwait(false);
        await _rightSnapshotCodec.SaveAsync(writer, RightTraceFile, _rightTrace.Current, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_leftSnapshotCodec is null || _rightSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "IncrementalFullJoinOp was constructed without snapshot codecs.");
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
        var leftOld = _leftTrace.Current;
        var rightOld = _rightTrace.Current;

        var builder = new ZSetBuilder<TOut, TWeight>();

        var touched = new HashSet<TKey>();
        foreach (var k in dl.Keys)
        {
            touched.Add(k);
        }

        foreach (var k in dr.Keys)
        {
            touched.Add(k);
        }

        foreach (var key in touched)
        {
            var oldL = leftOld.GroupFor(key);
            var oldR = rightOld.GroupFor(key);
            var dlK = dl.GroupFor(key);
            var drK = dr.GroupFor(key);
            var newL = oldL + dlK;
            var newR = oldR + drK;

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

        _output.SetCurrent(builder.Build());

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
        _leftTrace.DropKeysBelow(frontier, _monotoneKey);
        _rightTrace.DropKeysBelow(frontier, _monotoneKey);
    }

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
