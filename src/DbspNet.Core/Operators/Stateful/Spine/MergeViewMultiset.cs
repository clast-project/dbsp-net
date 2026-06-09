// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using System.Collections.Generic;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// The post-delta group of the spine aggregate, presented as a <b>lazy view</b>
/// over a sorted before-run (from <c>GroupForManySorted</c>) and this tick's flat
/// <c>groupDelta</c> — without first materialising the merged after-run. Replaces
/// <see cref="SortedRunMultiset{T}"/> built from an eager merge: it serves the
/// incremental aggregators' per-delta-row <see cref="WeightOf"/> probes (a binary
/// search of the before-run plus an O(1) delta lookup) directly, so a tick that
/// only probes a few values never pays the O(N) merge + array allocation the
/// after-run build cost (docs/design-row-representation.md §12). Aggregators that
/// scan the whole group (SUM/COUNT/AVG, and the non-incremental MIN/MAX
/// <c>Compute</c>) merge lazily through <see cref="GetEnumerator"/>, at the same
/// cost as the eager merge minus the throwaway array.
/// </summary>
/// <remarks>
/// <para>Contract on the inputs (exactly what the aggregate op produces):
/// <paramref name="beforeRun"/> is sorted by <paramref name="comparer"/>,
/// distinct, and zero-free; <c>delta</c> is a flat Z-set (distinct, zero-free).
/// The view's logical content is their per-value weight sum with zeros dropped —
/// identical to <c>SortedRunMultiset(MergeDeltaIntoRun(beforeRun, delta))</c>.</para>
/// <para>Enumeration order is NOT sorted (it walks the before-run then the
/// delta-only values); every consumer of <see cref="IMultiset{TKey,TWeight}"/>
/// is order-independent (MIN/MAX/SUM/COUNT/AVG and the sketches all fold
/// commutatively), so this is sound and saves the merge.</para>
/// </remarks>
internal sealed class MergeViewMultiset<T> : IMultiset<T, Z64>
    where T : notnull
{
    private readonly (T Value, Z64 Weight)[] _beforeRun;
    private readonly ZSet<T, Z64> _delta;
    private readonly IComparer<T> _comparer;

    public MergeViewMultiset((T Value, Z64 Weight)[] beforeRun, ZSet<T, Z64> delta, IComparer<T> comparer)
    {
        _beforeRun = beforeRun;
        _delta = delta;
        _comparer = comparer;
    }

    public bool IsEmpty
    {
        get
        {
            // A zero-free before-run value survives unless the delta exactly
            // cancels it; the delta is small, so the first before-run value the
            // delta does not touch has net = its before weight ≠ 0 → not empty in
            // O(1) for the common (growing) group. Only a fully-cancelled group
            // scans to the end.
            foreach (var (v, bw) in _beforeRun)
            {
                if (!Z64.IsZero(Z64.Add(bw, _delta.WeightOf(v))))
                {
                    return false;
                }
            }

            // Every before-run value cancelled; the group is non-empty only if the
            // delta adds a (zero-free) value the before-run did not hold.
            foreach (var (v, dw) in _delta)
            {
                if (!Z64.IsZero(dw) && !Present(v))
                {
                    return false;
                }
            }

            return true;
        }
    }

    public Z64 SumWeights()
    {
        // sum_v (before_v + delta_v) = sum(before-run) + sum(delta); zero-net
        // values contribute 0 either way, so no merge is needed.
        var sum = Z64.Zero;
        foreach (var (_, w) in _beforeRun)
        {
            sum = Z64.Add(sum, w);
        }

        return Z64.Add(sum, _delta.SumWeights());
    }

    public Z64 WeightOf(T key)
    {
        var before = BinarySearch(key);
        return Z64.Add(before, _delta.WeightOf(key));
    }

    public IEnumerator<KeyValuePair<T, Z64>> GetEnumerator()
    {
        // Before-run values with the delta folded in (drop zero nets), then the
        // delta-only values. Each distinct value is yielded exactly once.
        foreach (var (v, bw) in _beforeRun)
        {
            var net = Z64.Add(bw, _delta.WeightOf(v));
            if (!Z64.IsZero(net))
            {
                yield return new KeyValuePair<T, Z64>(v, net);
            }
        }

        foreach (var (v, dw) in _delta)
        {
            if (Z64.IsZero(dw))
            {
                continue;
            }

            // A delta value already present in the before-run was emitted above.
            if (!Present(v))
            {
                yield return new KeyValuePair<T, Z64>(v, dw);
            }
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private Z64 BinarySearch(T key)
    {
        int lo = 0, hi = _beforeRun.Length - 1;
        while (lo <= hi)
        {
            var mid = (int)(((uint)lo + (uint)hi) >> 1);
            var cmp = _comparer.Compare(_beforeRun[mid].Value, key);
            if (cmp < 0)
            {
                lo = mid + 1;
            }
            else if (cmp > 0)
            {
                hi = mid - 1;
            }
            else
            {
                return _beforeRun[mid].Weight;
            }
        }

        return Z64.Zero;
    }

    private bool Present(T key)
    {
        int lo = 0, hi = _beforeRun.Length - 1;
        while (lo <= hi)
        {
            var mid = (int)(((uint)lo + (uint)hi) >> 1);
            var cmp = _comparer.Compare(_beforeRun[mid].Value, key);
            if (cmp < 0)
            {
                lo = mid + 1;
            }
            else if (cmp > 0)
            {
                hi = mid - 1;
            }
            else
            {
                return true;
            }
        }

        return false;
    }
}
