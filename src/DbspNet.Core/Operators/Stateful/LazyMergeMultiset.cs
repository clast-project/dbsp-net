// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using System.Collections.Generic;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// The post-delta group of the flat aggregate, presented as a <b>lazy view</b>
/// over the stored before-group Z-set and this tick's <c>groupDelta</c> — without
/// rebuilding the merged after-group. Replaces
/// <c>afterGroup = beforeGroup + groupDelta</c> (<see cref="ZSet{TKey,TWeight}.Plus"/>
/// → <c>ZSetBuilder.From</c>, a full per-entry <b>re-hash</b> of the whole group
/// every tick — O(K) per tick, O(K²) over a growing group;
/// docs/design-row-representation.md §14). The incremental aggregators
/// (SUM/COUNT/AVG and incremental MIN/MAX) probe only the few delta rows via
/// <see cref="WeightOf"/>, so a tick that touches a large group while adding one
/// row pays O(|delta|) dictionary probes instead of an O(K) rebuild + allocation.
/// </summary>
/// <remarks>
/// <para>The flat analogue of the spine's <c>MergeViewMultiset</c> (§12), and
/// simpler: the flat before-group is a hashed <see cref="ZSet{TKey,TWeight}"/>
/// with O(1) <see cref="ZSet{TKey,TWeight}.WeightOf"/>, so no binary search is
/// needed — both inputs answer <c>WeightOf</c>/<c>Contains</c> in O(1).</para>
/// <para>Both <paramref name="before"/> and <c>delta</c> are distinct, zero-free
/// Z-sets; the view's logical content is their per-value weight sum with zeros
/// dropped — identical to <c>before + delta</c>. In particular
/// <see cref="IsEmpty"/> matches the eager <c>(before + delta).IsEmpty</c>:
/// <c>ZSetBuilder</c> drops zero-net entries, so the eager after-group's
/// dict-shape emptiness is exactly "every value cancels", which this view
/// reproduces.</para>
/// <para>Enumeration order is unspecified (before values folded with the delta,
/// then delta-only values); every <see cref="IMultiset{TKey,TWeight}"/> consumer
/// folds commutatively, so this is sound. The view is <b>transient</b>: it holds
/// a reference to the trace's current inner Z-set and must not be retained past
/// the trace <c>Integrate</c> that mutates that Z-set in place.</para>
/// </remarks>
internal sealed class LazyMergeMultiset<T> : IMultiset<T, Z64>
    where T : notnull
{
    private readonly ZSet<T, Z64> _before;
    private readonly ZSet<T, Z64> _delta;

    public LazyMergeMultiset(ZSet<T, Z64> before, ZSet<T, Z64> delta)
    {
        _before = before;
        _delta = delta;
    }

    public bool IsEmpty
    {
        get
        {
            // A zero-free before value survives unless the delta exactly cancels
            // it; for a growing group the first before value the delta does not
            // touch has net = its before weight ≠ 0, so this returns in O(1).
            // Only a fully-cancelled group scans to the end.
            foreach (var (v, bw) in _before)
            {
                if (!Z64.IsZero(Z64.Add(bw, _delta.WeightOf(v))))
                {
                    return false;
                }
            }

            // Every before value cancelled; the group is non-empty only if the
            // delta adds a (zero-free) value the before-group did not hold.
            foreach (var (v, dw) in _delta)
            {
                if (!Z64.IsZero(dw) && !_before.Contains(v))
                {
                    return false;
                }
            }

            return true;
        }
    }

    public Z64 WeightOf(T key) => Z64.Add(_before.WeightOf(key), _delta.WeightOf(key));

    public Z64 SumWeights() => Z64.Add(_before.SumWeights(), _delta.SumWeights());

    public IEnumerator<KeyValuePair<T, Z64>> GetEnumerator()
    {
        // Before-run values with the delta folded in (drop zero nets), then the
        // delta-only values. Each distinct value is yielded exactly once.
        foreach (var (v, bw) in _before)
        {
            var net = Z64.Add(bw, _delta.WeightOf(v));
            if (!Z64.IsZero(net))
            {
                yield return new KeyValuePair<T, Z64>(v, net);
            }
        }

        foreach (var (v, dw) in _delta)
        {
            if (!Z64.IsZero(dw) && !_before.Contains(v))
            {
                yield return new KeyValuePair<T, Z64>(v, dw);
            }
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>
/// Test/benchmark seam: force the flat aggregate to rebuild the after-group
/// eagerly (<c>beforeGroup + groupDelta</c>) instead of using the lazy
/// <see cref="LazyMergeMultiset{T}"/>, so a benchmark can A/B both strategies in
/// one process. Never set in production code — the lazy view is the default and
/// is always correctness-equivalent.
/// </summary>
internal static class FlatAggregateMode
{
    internal static bool ForceEagerRebuild;
}
