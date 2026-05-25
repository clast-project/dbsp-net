// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Incremental LEFT OUTER equi-join of two indexed Z-set streams. Behaves
/// like <see cref="IncrementalJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/> on
/// keys that have a right-side match, and additionally emits a NULL-padded
/// row for every left row whose key currently has no right-side entries.
/// </summary>
/// <remarks>
/// <para>
/// LEFT JOIN is not bilinear, so the <c>dl⋈R + L⋈dr + dl⋈dr</c> factoring
/// that works for inner joins doesn't directly apply. This operator works
/// per key via a small case analysis on the match-presence transition:
/// </para>
/// <list type="bullet">
/// <item><b>stayed-matched</b> (old ∃ right, new ∃ right): the standard
/// inner-join bilinear delta, in its two-pass asymmetric form
/// <c>dlK ⋈ newR + oldL ⋈ drK</c> (where <c>newR = oldR + drK</c> is
/// already computed for the case-selection check below).</item>
/// <item><b>stayed-unmatched</b> (old ∄ right, new ∄ right):
/// <c>delta = dl × {NULL}</c>.</item>
/// <item><b>gained-match</b> (old ∄ right, new ∃ right):
/// retract <c>oldL × {NULL}</c>, emit <c>newL ⋈ newR</c>.</item>
/// <item><b>lost-match</b> (old ∃ right, new ∄ right):
/// retract <c>oldL ⋈ oldR</c>, emit <c>newL × {NULL}</c>.</item>
/// </list>
/// <para>
/// Correctness hinges on the Z-set invariant that an indexed Z-set's group
/// is non-empty iff at least one entry has non-zero weight — i.e.
/// "match-presence" is cheap to check (<c>!group.IsEmpty</c>).
/// </para>
/// <para>
/// NULL-keyed rows on the left must be handled by the caller (the
/// <see cref="DbspNet.Sql.Compiler"/> plan→circuit layer routes them
/// directly to a NULL-padded output branch): this operator works on
/// non-null-keyed rows only, since the key must be hashable and equality-comparable.
/// </para>
/// </remarks>
internal sealed class IncrementalLeftJoinOp<TKey, TLeft, TRight, TOut, TWeight> : IOperator, ISnapshotable
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
    private readonly Func<TKey, TLeft, TOut> _nullPadCombine;
    private readonly IndexedZSetTrace<TKey, TLeft, TWeight> _leftTrace = new();
    private readonly IndexedZSetTrace<TKey, TRight, TWeight> _rightTrace = new();
    private readonly IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? _leftSnapshotCodec;
    private readonly IIndexedZSetTraceCodec<TKey, TRight, TWeight>? _rightSnapshotCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<TKey, long>? _monotoneKey;
    private long _lastGcFrontier = long.MinValue;

    public IncrementalLeftJoinOp(
        Stream<IndexedZSet<TKey, TLeft, TWeight>> leftIn,
        Stream<IndexedZSet<TKey, TRight, TWeight>> rightIn,
        Stream<ZSet<TOut, TWeight>> output,
        Func<TKey, TLeft, TRight, TOut> joinCombine,
        Func<TKey, TLeft, TOut> nullPadCombine,
        IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? leftSnapshotCodec = null,
        IIndexedZSetTraceCodec<TKey, TRight, TWeight>? rightSnapshotCodec = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null)
    {
        _leftIn = leftIn;
        _rightIn = rightIn;
        _output = output;
        _joinCombine = joinCombine;
        _nullPadCombine = nullPadCombine;
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
                "IncrementalLeftJoinOp was constructed without snapshot codecs; pass " +
                "them to CircuitBuilder.IncrementalLeftJoin to enable Snapshot.WriteAsync/ReadAsync.");
        }

        await _leftSnapshotCodec.SaveAsync(writer, LeftTraceFile, _leftTrace.Current, cancellationToken).ConfigureAwait(false);
        await _rightSnapshotCodec.SaveAsync(writer, RightTraceFile, _rightTrace.Current, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_leftSnapshotCodec is null || _rightSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "IncrementalLeftJoinOp was constructed without snapshot codecs.");
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

        // Every key that could have a non-zero delta this tick shows up in
        // dl or dr; keys not touched either way contribute nothing.
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

            var oldMatched = !oldR.IsEmpty;
            var newMatched = !newR.IsEmpty;

            if (oldMatched && newMatched)
            {
                // Bilinear inner-join delta, just for this key. Two-pass
                // asymmetric form: dlK ⋈ newR absorbs both dlK ⋈ oldR and
                // the cross term dlK ⋈ drK.
                JoinInto(builder, key, dlK, newR);
                JoinInto(builder, key, oldL, drK);
            }
            else if (!oldMatched && !newMatched)
            {
                // Still no right match for this key: the left delta becomes
                // NULL-padded output deltas 1:1.
                NullPadInto(builder, key, dlK);
            }
            else if (!oldMatched && newMatched)
            {
                // Key gained a match this tick. Retract every NULL-padded
                // row that was present (oldL), emit the full join on newL × newR.
                NullPadInto(builder, key, oldL.Negate());
                JoinInto(builder, key, newL, newR);
            }
            else
            {
                // Key lost all matches this tick. Retract the prior joined
                // rows, emit newL × NULL.
                JoinInto(builder, key, oldL.Negate(), oldR);
                NullPadInto(builder, key, newL);
            }
        }

        _output.SetCurrent(builder.Build());

        _leftTrace.Integrate(dl);
        _rightTrace.Integrate(dr);
        CollectGarbage();
    }

    // Frontier-driven GC: when the join key is monotone on BOTH sides, no
    // future delta can arrive at a sub-frontier key on either input — so that
    // key's match-presence state is frozen and its already-emitted output
    // (joined or NULL-padded) is final. Drop those keys from both traces.
    // The both-sides-monotone license is stricter than the analyzer's output
    // marking (which flags only the preserved side for an outer join): a future
    // row on the non-monotone side could still flip a match below the frontier,
    // so the caller must verify both sides before supplying the frontier here.
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

    private void NullPadInto(
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
            output.Add(_nullPadCombine(key, lv), lw);
        }
    }
}
