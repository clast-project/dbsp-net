// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using System.Collections.Generic;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// An <see cref="IMultiset{TKey,TWeight}"/> backed by a sorted, distinct,
/// zero-free <c>(value, weight)</c> run — the post-delta group the spine
/// aggregate builds by merging a <c>GroupForManySorted</c> before-run with the
/// per-tick delta. Unlike a rebuilt <see cref="ZSet{TKey,TWeight}"/> it never
/// hashes a value: <see cref="WeightOf"/> binary-searches the run (comparing the
/// key) and <see cref="SumWeights"/> / enumeration are a linear pass. This is
/// the consumption side of the merge-execution win (docs/design-row-representation.md §8).
/// </summary>
/// <remarks>
/// The run must be sorted by <paramref name="comparer"/>, hold no duplicate
/// values, and carry no zero weights — exactly what
/// <c>SpineIncrementalAggregateOp</c>'s merge produces. <see cref="IsEmpty"/> is
/// therefore equivalent to a Z-set's dict-shape emptiness (a fully cancelled
/// group merges to a length-0 run).
/// </remarks>
internal sealed class SortedRunMultiset<T> : IMultiset<T, Z64>
    where T : notnull
{
    private readonly (T Value, Z64 Weight)[] _run;
    private readonly IComparer<T> _comparer;

    public SortedRunMultiset((T Value, Z64 Weight)[] run, IComparer<T> comparer)
    {
        _run = run;
        _comparer = comparer;
    }

    public bool IsEmpty => _run.Length == 0;

    public Z64 SumWeights()
    {
        var sum = Z64.Zero;
        foreach (var (_, w) in _run)
        {
            sum = Z64.Add(sum, w);
        }

        return sum;
    }

    public Z64 WeightOf(T key)
    {
        int lo = 0, hi = _run.Length - 1;
        while (lo <= hi)
        {
            var mid = (int)(((uint)lo + (uint)hi) >> 1);
            var cmp = _comparer.Compare(_run[mid].Value, key);
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
                return _run[mid].Weight;
            }
        }

        return Z64.Zero;
    }

    public IEnumerator<KeyValuePair<T, Z64>> GetEnumerator()
    {
        foreach (var (v, w) in _run)
        {
            yield return new KeyValuePair<T, Z64>(v, w);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
