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
/// inner-join bilinear delta.</item>
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
internal sealed class IncrementalLeftJoinOp<TKey, TLeft, TRight, TOut, TWeight> : IOperator
    where TKey : notnull
    where TLeft : notnull
    where TRight : notnull
    where TOut : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<IndexedZSet<TKey, TLeft, TWeight>> _leftIn;
    private readonly Stream<IndexedZSet<TKey, TRight, TWeight>> _rightIn;
    private readonly Stream<ZSet<TOut, TWeight>> _output;
    private readonly Func<TKey, TLeft, TRight, TOut> _joinCombine;
    private readonly Func<TKey, TLeft, TOut> _nullPadCombine;
    private readonly IndexedZSetTrace<TKey, TLeft, TWeight> _leftTrace = new();
    private readonly IndexedZSetTrace<TKey, TRight, TWeight> _rightTrace = new();

    public IncrementalLeftJoinOp(
        Stream<IndexedZSet<TKey, TLeft, TWeight>> leftIn,
        Stream<IndexedZSet<TKey, TRight, TWeight>> rightIn,
        Stream<ZSet<TOut, TWeight>> output,
        Func<TKey, TLeft, TRight, TOut> joinCombine,
        Func<TKey, TLeft, TOut> nullPadCombine)
    {
        _leftIn = leftIn;
        _rightIn = rightIn;
        _output = output;
        _joinCombine = joinCombine;
        _nullPadCombine = nullPadCombine;
    }

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
                // Bilinear inner-join delta, just for this key.
                JoinInto(builder, key, dlK, oldR);
                JoinInto(builder, key, oldL, drK);
                JoinInto(builder, key, dlK, drK);
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
