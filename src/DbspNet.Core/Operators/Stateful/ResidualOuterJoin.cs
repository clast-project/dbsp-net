// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// The per-key deltas of an outer join whose ON clause carries a residual (a non-equi conjunct)
/// (docs/design-flat-trace-state.md §3.30). A preserved row is matched iff some row of its key's
/// other-side group (non-zero weight) satisfies the residual with it; it is NULL-padded otherwise.
/// </summary>
internal static class ResidualOuterJoin
{
    /// <summary>
    /// The joined rows' delta for one key: <c>dL ⋈ R' + L ⋈ dR</c> (<c>R' = R + dR</c>), keeping
    /// only pairs that satisfy <paramref name="residual"/>.
    /// </summary>
    public static void JoinInto<TKey, TLeft, TRight, TOut, TWeight>(
        ZSetBuilder<TOut, TWeight> output,
        TKey key,
        ZSet<TLeft, TWeight> oldL,
        ZSet<TLeft, TWeight> dL,
        ZSet<TRight, TWeight> dR,
        ZSet<TRight, TWeight> newR,
        Func<TKey, TLeft, TRight, bool> residual,
        Func<TKey, TLeft, TRight, TOut> combine)
        where TKey : notnull
        where TLeft : notnull
        where TRight : notnull
        where TOut : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        Pairs(output, key, dL, newR, residual, combine);
        Pairs(output, key, oldL, dR, residual, combine);
    }

    private static void Pairs<TKey, TLeft, TRight, TOut, TWeight>(
        ZSetBuilder<TOut, TWeight> output,
        TKey key,
        ZSet<TLeft, TWeight> left,
        ZSet<TRight, TWeight> right,
        Func<TKey, TLeft, TRight, bool> residual,
        Func<TKey, TLeft, TRight, TOut> combine)
        where TKey : notnull
        where TLeft : notnull
        where TRight : notnull
        where TOut : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        if (left.IsEmpty || right.IsEmpty)
        {
            return;
        }

        foreach (var (lv, lw) in left)
        {
            foreach (var (rv, rw) in right)
            {
                if (residual(key, lv, rv))
                {
                    output.Add(combine(key, lv, rv), TWeight.Multiply(lw, rw));
                }
            }
        }
    }

    /// <summary>
    /// The padding delta of one preserved side for one key. Only rows of <paramref name="dP"/> and
    /// rows of <paramref name="oldP"/> that some row of <paramref name="dO"/> satisfies can change
    /// their padding: for any other preserved row, the other-side rows satisfying the residual with
    /// it are unchanged. For each such row, old and new match-presence are scans of
    /// <paramref name="oldO"/> and <paramref name="newO"/>; it emits
    /// <c>pad(p)·(w'(p)·[¬m'(p)] − w(p)·[¬m(p)])</c>.
    /// </summary>
    /// <param name="residual">The residual oriented (key, preserved row, other row).</param>
    public static void PadInto<TKey, TP, TO, TOut, TWeight>(
        ZSetBuilder<TOut, TWeight> output,
        TKey key,
        ZSet<TP, TWeight> oldP,
        ZSet<TP, TWeight> dP,
        ZSet<TO, TWeight> oldO,
        ZSet<TO, TWeight> dO,
        ZSet<TO, TWeight> newO,
        Func<TKey, TP, TO, bool> residual,
        Func<TKey, TP, TOut> pad)
        where TKey : notnull
        where TP : notnull
        where TO : notnull
        where TOut : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        // Preserved rows whose padding may change.
        HashSet<TP>? candidates = null;
        foreach (var (p, _) in dP)
        {
            (candidates ??= new()).Add(p);
        }

        if (!dO.IsEmpty)
        {
            foreach (var (p, _) in oldP)
            {
                if (candidates is not null && candidates.Contains(p))
                {
                    continue;
                }

                foreach (var (o, _) in dO)
                {
                    if (residual(key, p, o))
                    {
                        (candidates ??= new()).Add(p);
                        break;
                    }
                }
            }
        }

        if (candidates is null)
        {
            return;
        }

        foreach (var p in candidates)
        {
            var was = oldP.WeightOf(p);
            var now = TWeight.Add(was, dP.WeightOf(p));
            var before = TWeight.IsZero(was) || Matched(key, p, oldO, residual) ? TWeight.Zero : was;
            var after = TWeight.IsZero(now) || Matched(key, p, newO, residual) ? TWeight.Zero : now;
            var change = TWeight.Add(after, TWeight.Negate(before));
            if (!TWeight.IsZero(change))
            {
                output.Add(pad(key, p), change);
            }
        }
    }

    private static bool Matched<TKey, TP, TO, TWeight>(TKey key, TP p, ZSet<TO, TWeight> other, Func<TKey, TP, TO, bool> residual)
        where TKey : notnull
        where TP : notnull
        where TO : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        foreach (var (o, _) in other)
        {
            if (residual(key, p, o))
            {
                return true;
            }
        }

        return false;
    }
}
