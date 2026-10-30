// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Shared inner-join cross-product kernel used by both the private-trace
/// <see cref="IncrementalJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/> and the
/// shared-arrangement <see cref="IncrementalJoinSharedRightOp{TKey,TLeft,TRight,TOut,TWeight}"/>.
/// Factored out so the two operators run byte-identical join logic — only their
/// trace ownership differs.
/// </summary>
internal static class IncrementalJoinCore
{
    /// <summary>
    /// Emits <c>a ⋈ b</c> (equi-join on the shared key) into
    /// <paramref name="output"/>, iterating the smaller side by group count to
    /// amortize probing. An optional <paramref name="residual"/> (e.g. a
    /// non-equi WHERE conjunct spanning both sides, folded in by the optimizer)
    /// is applied during the cross-product enumeration so rejected rows never
    /// enter the output Z-set.
    /// </summary>
    public static void JoinInto<TKey, TLeft, TRight, TOut, TWeight>(
        IndexedZSet<TKey, TLeft, TWeight> a,
        IndexedZSet<TKey, TRight, TWeight> b,
        Func<TKey, TLeft, TRight, TOut> combine,
        Func<TOut, bool>? residual,
        ZSetBuilder<TOut, TWeight> output)
        where TKey : notnull
        where TLeft : notnull
        where TRight : notnull
        where TOut : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        if (a.IsEmpty || b.IsEmpty)
        {
            return;
        }

        if (a.GroupCount <= b.GroupCount)
        {
            foreach (var (key, aGroup) in a)
            {
                var bGroup = b.GroupFor(key);
                if (bGroup.IsEmpty)
                {
                    continue;
                }

                foreach (var (av, aw) in aGroup)
                {
                    foreach (var (bv, bw) in bGroup)
                    {
                        var outRow = combine(key, av, bv);
                        if (residual is null || residual(outRow))
                        {
                            output.Add(outRow, TWeight.Multiply(aw, bw));
                        }
                    }
                }
            }
        }
        else
        {
            foreach (var (key, bGroup) in b)
            {
                var aGroup = a.GroupFor(key);
                if (aGroup.IsEmpty)
                {
                    continue;
                }

                foreach (var (av, aw) in aGroup)
                {
                    foreach (var (bv, bw) in bGroup)
                    {
                        var outRow = combine(key, av, bv);
                        if (residual is null || residual(outRow))
                        {
                            output.Add(outRow, TWeight.Multiply(aw, bw));
                        }
                    }
                }
            }
        }
    }
}
