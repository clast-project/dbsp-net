// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Shared spine join probe-and-combine kernel used by both the private-trace
/// <see cref="SpineIncrementalJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/> and the
/// shared-arrangement <see cref="SpineIncrementalJoinSharedRightOp{TKey,TLeft,TRight,TOut,TWeight}"/>.
/// Factored out so the two operators run byte-identical probe logic — only their
/// trace ownership differs.
/// </summary>
internal static class SpineJoinProbe
{
    /// <summary>
    /// Joins one delta side against the integrated <paramref name="trace"/> on
    /// the other side, emitting into <paramref name="builder"/>. Replaces the
    /// per-key <c>GroupFor</c> point-probe with a single batched galloping merge
    /// (<see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}.GroupForManySorted"/>):
    /// the delta keys are sorted once (by <paramref name="keyComparer"/>, which
    /// must match the trace's key order) and each sorted batch's outer-key column
    /// is walked once. Matched groups arrive as sorted <c>(value, weight)</c>
    /// runs sliced straight from the batch columns; the cross-product consumes
    /// them by iteration exactly like the dictionary group it replaces, so the
    /// output is identical (docs/design-row-representation.md §8).
    /// </summary>
    /// <remarks>
    /// The merge only pays off once the tick spans more than one key — at
    /// <c>D == 1</c> a single point probe beats the per-batch cursor setup, so
    /// single-key ticks keep the <c>GroupFor</c> path. The benchmark seam
    /// <see cref="SpineJoinProbeMode.ForcePointProbe"/> forces that path for the
    /// whole tick so the two strategies can be A/B'd in one process.
    /// </remarks>
    public static void ProbeAndJoin<TKey, TDelta, TProbe, TOut, TWeight>(
        IndexedZSet<TKey, TDelta, TWeight> delta,
        SpineIndexedZSetTrace<TKey, TProbe, TWeight> trace,
        Func<TKey, TDelta, TProbe, TOut> combine,
        IComparer<TKey> keyComparer,
        ZSetBuilder<TOut, TWeight> builder)
        where TKey : notnull
        where TDelta : notnull
        where TProbe : notnull
        where TOut : notnull
        where TWeight : struct, IZRing<TWeight>
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

        var sortedKeys = SortedKeysOf(delta, keyComparer);
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
    /// The delta's distinct outer keys, sorted by <paramref name="keyComparer"/>
    /// (the same order the spine batches use) — the contract
    /// <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}.GroupForManySorted"/>
    /// requires.
    /// </summary>
    private static TKey[] SortedKeysOf<TKey, TDelta, TWeight>(
        IndexedZSet<TKey, TDelta, TWeight> delta, IComparer<TKey> keyComparer)
        where TKey : notnull
        where TDelta : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        var keys = new TKey[delta.GroupCount];
        var i = 0;
        foreach (var key in delta.Keys)
        {
            keys[i++] = key;
        }

        Array.Sort(keys, keyComparer);
        return keys;
    }
}
